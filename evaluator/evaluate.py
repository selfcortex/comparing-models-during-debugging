#!/usr/bin/env python3
"""Hidden evaluator for the `taskflow` debugging benchmark.

Usage:
    python evaluate.py path/to/taskflow.py [--verbose] [--json result.json]

Keep this file OUTSIDE the workspace the agent can see.  Every check runs in
isolation (own fixtures, wall-clock timeout) and gives pass/fail.  The final
score is the weighted share of passed checks, 0..100.
"""
import argparse
import copy
import importlib.util
import json
import random
import signal
import sys
import time
import traceback

CHECKS = []   # (id, area, weight, title, func)


def check(cid, area, weight, title):
    def deco(fn):
        CHECKS.append((cid, area, weight, title, fn))
        return fn
    return deco


def ensure(cond, msg="assertion failed"):
    if not cond:
        raise AssertionError(msg)


def raises(exc_type, fn, *a, **kw):
    try:
        fn(*a, **kw)
    except exc_type as e:
        return e
    except BaseException as e:  # noqa: BLE001
        raise AssertionError(f"expected {exc_type.__name__}, got {type(e).__name__}: {e}")
    raise AssertionError(f"expected {exc_type.__name__}, nothing was raised")


class Abort(BaseException):
    pass


# ==========================================================================
# TokenBucket
# ==========================================================================
@check("bucket.fractions", "bucket", 2, "fractional tokens are accumulated, failed acquire takes nothing")
def _(tf):
    c = tf.Clock()
    b = tf.TokenBucket(2, 0.5, c)
    ensure(b.try_acquire(2) is True)
    c.advance(1)
    ensure(b.try_acquire(1) is False, "0.5 token must not be enough")
    c.advance(1)
    ensure(b.try_acquire(1) is True, "0.5 + 0.5 tokens must be enough (fractions lost?)")
    ensure(b.try_acquire(1) is False)


@check("bucket.idle", "bucket", 2, "idle time while full gives no burst credit")
def _(tf):
    c = tf.Clock()
    b = tf.TokenBucket(2, 1, c)
    c.advance(100)
    ensure(b.try_acquire(2) is True)
    ensure(b.try_acquire(1) is False)
    c.advance(1)
    ensure(b.try_acquire(1) is True, "one second must yield exactly one token")
    ensure(b.try_acquire(1) is False, "stale timestamp gave free tokens")


@check("bucket.time_until", "bucket", 2, "time_until accounts for the current balance; argument validation")
def _(tf):
    c = tf.Clock()
    b = tf.TokenBucket(4, 2, c)
    ensure(b.time_until(1) == 0.0)
    ensure(b.try_acquire(4))
    ensure(b.time_until(1) == 0.5)
    c.advance(0.25)
    ensure(b.time_until(1) == 0.25, f"got {b.time_until(1)}")
    ensure(b.time_until(3) == 1.25, f"got {b.time_until(3)}")
    raises(ValueError, b.try_acquire, 5)
    raises(ValueError, b.try_acquire, 0)


# ==========================================================================
# TTLCache
# ==========================================================================
@check("cache.ttl_boundary", "cache", 2, "expiry boundary is inclusive; get does not extend TTL")
def _(tf):
    c = tf.Clock()
    k = tf.TTLCache(4, ttl=10, clock=c)
    k.put("a", 1)
    c.advance(9.5)
    ensure(k.get("a") == 1)
    c.advance(0.5)
    ensure(k.get("a", "gone") == "gone", "entry must be expired exactly at t+ttl")
    k.put("b", 2)
    c.advance(6)
    ensure(k.get("b") == 2)
    c.advance(4)
    ensure(k.get("b", "gone") == "gone", "get() must not refresh the TTL")


@check("cache.ttl_zero", "cache", 2, "per-entry ttl=0 expires immediately, ttl=None uses default")
def _(tf):
    c = tf.Clock()
    k = tf.TTLCache(4, ttl=5, clock=c)
    k.put("z", 1, ttl=0)
    ensure(k.get("z", "gone") == "gone", "ttl=0 must not mean 'default'")
    k.put("d", 2)
    c.advance(5)
    ensure("d" not in k)
    k2 = tf.TTLCache(4, ttl=None, clock=c)
    k2.put("x", 1)
    c.advance(10 ** 6)
    ensure(k2.get("x") == 1)
    k2.put("y", 2, ttl=3)
    c.advance(3)
    ensure(k2.get("y", "gone") == "gone")


@check("cache.lru_get", "cache", 2, "successful get refreshes recency")
def _(tf):
    k = tf.TTLCache(2, clock=tf.Clock())
    k.put("a", 1)
    k.put("b", 2)
    ensure(k.get("a") == 1)
    k.put("c", 3)
    ensure(k.keys() == ["a", "c"], f"keys={k.keys()}")


@check("cache.replace", "cache", 3, "replacing a key refreshes recency and never evicts")
def _(tf):
    k = tf.TTLCache(2, clock=tf.Clock())
    k.put("a", 1)
    k.put("b", 2)
    k.put("a", 10)
    ensure(len(k) == 2 and "a" in k and "b" in k, "replacing an existing key must not evict anything")
    ensure(k.keys() == ["b", "a"], f"keys={k.keys()} (replace must make 'a' most recent)")
    k.put("c", 3)
    ensure(k.keys() == ["a", "c"], f"keys={k.keys()} ('b' was the least recently used)")
    ensure("a" in k and k.keys() == ["a", "c"], "`in` must not touch recency")
    ensure(k.get("a") == 10, "replaced value must be the new one")


@check("cache.purge_first", "cache", 3, "expired entries are dropped before live ones are evicted")
def _(tf):
    c = tf.Clock()
    k = tf.TTLCache(2, clock=c)
    k.put("live", 1, ttl=100)
    k.put("short", 2, ttl=1)
    c.advance(2)
    k.put("new", 3)
    ensure("live" in k, "live entry was evicted although an expired one occupied a slot")
    ensure(k.keys() == ["live", "new"], f"keys={k.keys()}")


@check("cache.live_view", "cache", 2, "len/in/keys ignore expired entries")
def _(tf):
    c = tf.Clock()
    k = tf.TTLCache(5, clock=c)
    k.put("a", 1, ttl=1)
    k.put("b", 2, ttl=10)
    c.advance(1)
    ensure(len(k) == 1, f"len={len(k)}")
    ensure("a" not in k and "b" in k)
    ensure(k.keys() == ["b"])


# ==========================================================================
# EventBus
# ==========================================================================
@check("bus.order", "bus", 2, "priority order; equal priority = subscription order; patterns merged")
def _(tf):
    bus = tf.EventBus()
    seen = []
    mk = lambda tag: (lambda t, p: seen.append(tag))
    bus.subscribe("x.y", mk("exact0"))
    bus.subscribe("*", mk("star0"))
    bus.subscribe("x.*", mk("prefix0"))
    bus.subscribe("x.y", mk("hi5a"), priority=5)
    bus.subscribe("*", mk("hi5b"), priority=5)
    bus.subscribe("x.y", mk("top"), priority=10)
    bus.subscribe("x.y", mk("low"), priority=-1)
    bus.emit("x.y")
    ensure(seen == ["top", "hi5a", "hi5b", "exact0", "star0", "prefix0", "low"], str(seen))


@check("bus.patterns", "bus", 2, "wildcard semantics")
def _(tf):
    bus = tf.EventBus()
    got = []
    bus.subscribe("job.*", lambda t, p: got.append(t))
    for t in ("job.started", "job.a.b", "job", "jobs.started", "xjob.started", "other"):
        bus.emit(t)
    ensure(got == ["job.started", "job.a.b"], str(got))
    allg = []
    bus.subscribe("*", lambda t, p: allg.append(t))
    bus.emit("anything")
    bus.emit("")
    ensure(allg == ["anything", ""])
    ex = []
    bus.subscribe("job", lambda t, p: ex.append(t))
    bus.emit("job.x")
    bus.emit("job")
    ensure(ex == ["job"])


@check("bus.mutation", "bus", 3, "unsubscribe / subscribe during dispatch")
def _(tf):
    bus = tf.EventBus()
    calls, unsub_results = [], []
    tok = {}
    flag = {"first_time": True}

    def first(t, p):
        calls.append("first")
        unsub_results.append(bus.unsubscribe(tok["second"]))
        if flag["first_time"]:
            flag["first_time"] = False
            bus.subscribe("t", lambda t_, p_: calls.append("late"), priority=100)

    bus.subscribe("t", first, priority=10)
    tok["second"] = bus.subscribe("t", lambda t, p: calls.append("second"), priority=5)
    bus.subscribe("t", lambda t, p: calls.append("third"), priority=1)
    bus.emit("t")
    ensure(calls == ["first", "third"], f"first emit: {calls}")
    calls.clear()
    bus.emit("t")
    ensure(calls == ["late", "first", "third"], f"second emit: {calls}")
    ensure(unsub_results == [True, False], str(unsub_results))


@check("bus.once_errors", "bus", 3, "once + re-entrant emit; handler errors are isolated and returned")
def _(tf):
    bus = tf.EventBus()
    hits = []
    state = {"re": True}

    def once(t, p):
        hits.append("once")
        if state["re"]:
            state["re"] = False
            bus.emit("t")

    bus.subscribe("t", once, once=True, priority=5)
    bus.subscribe("t", lambda t, p: hits.append("plain"), priority=1)
    bus.emit("t")
    ensure(hits.count("once") == 1, f"once handler ran {hits.count('once')} times: {hits}")
    ensure(hits.count("plain") == 2, str(hits))
    bus2 = tf.EventBus()
    order = []

    def boom(t, p):
        order.append("boom")
        raise RuntimeError("x")

    bus2.subscribe("e", boom, priority=9)
    bus2.subscribe("e", lambda t, p: order.append("after"))
    errs = bus2.emit("e")
    ensure(order == ["boom", "after"], str(order))
    ensure(len(errs) == 1 and isinstance(errs[0], RuntimeError), str(errs))
    ensure(bus2.emit("none") == [])


# ==========================================================================
# DAG
# ==========================================================================
@check("dag.dup_deps", "dag", 3, "duplicate dependency names are collapsed (nothing silently dropped)")
def _(tf):
    g = tf.DAG()
    g.add("b", ("a", "a", "a"))
    g.add("a")
    ensure(g.deps("b") == ("a",), str(g.deps("b")))
    ensure(g.order() == ["a", "b"], f"order={g.order()}")
    ensure(g.levels() == [["a"], ["b"]], str(g.levels()))
    raises(tf.DuplicateNodeError, g.add, "a")


@check("dag.self_loop", "dag", 2, "self-dependency is a cycle")
def _(tf):
    g = tf.DAG()
    g.add("x", ("x",))
    e = raises(tf.CycleError, g.validate)
    ensure(e.cycle == ["x", "x"], str(e.cycle))


@check("dag.tiebreak", "dag", 3, "ready nodes are taken in insertion order")
def _(tf):
    g = tf.DAG()
    g.add("d", ("b", "c"))
    g.add("c", ("a",))
    g.add("b", ("a",))
    g.add("a")
    g.add("z")
    g.add("y")
    ensure(g.order() == ["a", "c", "b", "d", "z", "y"], str(g.order()))


@check("dag.cycle_path", "dag", 3, "cycle path holds only cycle members; order() refuses cycles")
def _(tf):
    g = tf.DAG()
    g.add("x", ("a",))
    g.add("a", ("b",))
    g.add("b", ("c",))
    g.add("c", ("a",))
    e = raises(tf.CycleError, g.validate)
    cyc = e.cycle
    ensure(cyc[0] == cyc[-1] and len(cyc) == 4 and set(cyc) == {"a", "b", "c"}, f"cycle={cyc}")
    for u, v in zip(cyc, cyc[1:]):
        ensure(v in g.deps(u), f"{u} does not depend on {v}")
    raises(tf.CycleError, g.order)
    raises(tf.CycleError, g.levels)
    g2 = tf.DAG()
    g2.add("p", ("q", "ghost"))
    g2.add("q", ("p",))
    raises(tf.MissingDependencyError, g2.validate)  # missing is reported before cycles


@check("dag.deep_chain", "dag", 4, "20000-node chain: no recursion errors, linear time")
def _(tf):
    n = 20000
    g = tf.DAG()
    for i in range(n):
        g.add(f"n{i}", (f"n{i - 1}",) if i else ())
    t0 = time.time()
    g.validate()
    o = g.order()
    ensure(len(o) == n and o[0] == "n0" and o[-1] == f"n{n - 1}")
    lv = g.levels()
    ensure(len(lv) == n)
    ensure(len(g.descendants("n0")) == n - 1)
    ensure(time.time() - t0 < 5, "too slow")
    g.add("loop_a", ("loop_b",))
    g.add("loop_b", ("n5", "loop_a"))
    e = raises(tf.CycleError, g.validate)
    ensure(set(e.cycle) == {"loop_a", "loop_b"}, str(e.cycle))


@check("dag.levels", "dag", 2, "level = 1 + MAX level of dependencies")
def _(tf):
    g = tf.DAG()
    g.add("d", ("a", "c"))
    g.add("c", ("b",))
    g.add("b", ("a",))
    g.add("a")
    g.add("e")
    ensure(g.levels() == [["a", "e"], ["b"], ["c"], ["d"]], str(g.levels()))
    ensure(tf.DAG().levels() == [])


@check("dag.targets", "dag", 3, "order(targets) = transitive closure; descendants")
def _(tf):
    g = tf.DAG()
    for name, deps in [("a", ()), ("b", ("a",)), ("c", ("b",)), ("d", ("c",)), ("e", ()), ("f", ("a",))]:
        g.add(name, deps)
    ensure(g.order(["d"]) == ["a", "b", "c", "d"], str(g.order(["d"])))
    ensure(g.order(["c", "f"]) == ["a", "b", "c", "f"], str(g.order(["c", "f"])))
    ensure(g.order([]) == [])
    raises(KeyError, g.order, ["nope"])
    ensure(g.descendants("a") == {"b", "c", "d", "f"}, str(g.descendants("a")))
    ensure(g.descendants("d") == set() and g.descendants("e") == set())


# ==========================================================================
# RetryPolicy
# ==========================================================================
@check("retry.delay", "retry", 2, "exponential delay formula with cap")
def _(tf):
    p = tf.RetryPolicy(max_attempts=9, base_delay=1.0, factor=2.0, max_delay=30.0)
    got = [p.delay(i) for i in range(1, 8)]
    ensure(got == [1, 2, 4, 8, 16, 30, 30], str(got))
    q = tf.RetryPolicy(max_attempts=9, base_delay=0.5, factor=3, max_delay=100)
    ensure([q.delay(i) for i in (1, 2, 3)] == [0.5, 1.5, 4.5])


@check("retry.overflow", "retry", 2, "huge attempt numbers do not overflow")
def _(tf):
    for factor in (2.0, 2, 10, 1.5):
        p = tf.RetryPolicy(max_attempts=10 ** 6, base_delay=1.0, factor=factor, max_delay=60.0)
        ensure(p.delay(5000) == 60.0, f"factor={factor}")
        ensure(p.delay(10 ** 5) == 60.0)


@check("retry.should_retry", "retry", 3, "attempt budget, retry_on and no_retry_on precedence")
def _(tf):
    p = tf.RetryPolicy(max_attempts=3)
    ensure([p.should_retry(ValueError(), a) for a in (1, 2, 3, 4)] == [True, True, False, False])
    p = tf.RetryPolicy(max_attempts=5, retry_on=(OSError,))
    ensure(p.should_retry(OSError(), 1) and not p.should_retry(ValueError(), 1))
    p = tf.RetryPolicy(max_attempts=5, retry_on=(Exception,), no_retry_on=(LookupError,))
    ensure(not p.should_retry(KeyError("k"), 1), "no_retry_on must win (and match subclasses)")
    ensure(p.should_retry(ValueError(), 1))
    ensure(tf.RetryPolicy(max_attempts=1).should_retry(ValueError(), 1) is False)


# ==========================================================================
# Store
# ==========================================================================
@check("store.nesting", "store", 4, "nested commit merges into parent only; rollback semantics")
def _(tf):
    s = tf.Store()
    s.set("a", 1)
    s.begin()
    s.set("a", 2)
    s.begin()
    s.set("a", 3)
    s.commit()
    ensure(s.get("a") == 3 and s.depth == 1)
    s.rollback()
    ensure(s.get("a") == 1 and s.depth == 0, "inner commit leaked past the outer rollback")
    s.begin()
    s.set("b", 1)
    s.begin()
    s.set("b", 2)
    s.set("c", 9)
    s.rollback()
    ensure(s.get("b") == 1 and not s.has("c"))
    s.commit()
    ensure(s.get("b") == 1 and s.keys() == ["a", "b"])
    raises(tf.TransactionError, s.commit)
    raises(tf.TransactionError, s.rollback)


@check("store.delete", "store", 4, "deletes inside transactions: hidden, restorable, no leaked sentinels")
def _(tf):
    s = tf.Store()
    s.set("x", 1)
    s.set("y", 2)
    s.begin()
    ensure(s.delete("x") is True and s.delete("x") is False and s.delete("nope") is False)
    ensure(s.get("x", "none") == "none" and s.keys() == ["y"] and not s.has("x"))
    s.commit()
    ensure(s.keys() == ["y"], f"keys={s.keys()}")
    ensure(s.snapshot() == {"y": 2}, f"snapshot={s.snapshot()}")
    ensure(list(s.snapshot().values()) == [2])
    s.set("x", 5)
    s.begin()
    s.delete("x")
    s.rollback()
    ensure(s.get("x") == 5)
    s.begin()
    s.begin()
    s.delete("y")
    s.commit()
    ensure(s.get("y") is None)
    s.rollback()
    ensure(s.get("y") == 2, "deletion committed into parent must still be discarded by parent rollback")
    s.begin()
    s.delete("y")
    s.set("y", 7)
    s.commit()
    ensure(s.get("y") == 7 and s.snapshot() == {"x": 5, "y": 7})


@check("store.isolation", "store", 3, "values are copied in and out")
def _(tf):
    s = tf.Store()
    v = {"l": [1, 2]}
    s.set("k", v)
    v["l"].append(3)
    ensure(s.get("k") == {"l": [1, 2]}, "set() stored a reference")
    got = s.get("k")
    got["l"].append(99)
    ensure(s.get("k") == {"l": [1, 2]}, "get() returned a reference")
    s.begin()
    got = s.get("k")
    got["l"].clear()
    s.set("k2", got)
    got["l"].append(1)
    ensure(s.get("k2") == {"l": []})
    s.rollback()
    ensure(s.get("k") == {"l": [1, 2]})


@check("store.transaction_cm", "store", 3, "context manager: commit / rollback on Exception and BaseException")
def _(tf):
    s = tf.Store()
    with s.transaction():
        s.set("a", 1)
    ensure(s.get("a") == 1 and s.depth == 0)
    try:
        with s.transaction():
            s.set("a", 2)
            raise ValueError("boom")
    except ValueError:
        pass
    ensure(s.get("a") == 1 and s.depth == 0)
    try:
        with s.transaction():
            s.set("a", 3)
            s.set("b", 3)
            raise Abort()
    except Abort:
        pass
    ensure(s.depth == 0, "transaction left open after BaseException")
    ensure(s.get("a") == 1 and not s.has("b"), "BaseException must roll back")


# ==========================================================================
# serialization
# ==========================================================================
@check("ser.roundtrip", "serial", 3, "tuples/sets survive at any depth, incl. mixed-type sets")
def _(tf):
    data = {
        "default/a": [1, (2, [3, (4,)])],
        "default/s": {1, "a", (2, 3)},
        "default/d": {"k": ({"z": {5, 6}},), "n": None, "f": 1.5, "t": True},
        "default/t": (),
    }
    text = tf.serialize(data, {"who": ("me", 1)})
    out, meta = tf.deserialize(text)
    ensure(out == data, f"{out}")
    ensure(type(out["default/a"][1]) is tuple and type(out["default/a"][1][1][1]) is tuple, "nested tuple lost")
    ensure(type(out["default/d"]["k"]) is tuple and type(out["default/d"]["k"][0]["z"]) is set)
    ensure(meta == {"who": ("me", 1)}, str(meta))


@check("ser.canonical", "serial", 3, "canonical output; non-str keys and foreign types raise TypeError")
def _(tf):
    a = {"b": 1, "a": {3, "x", 1, (1, 2)}}
    b = {"a": {(1, 2), 1, "x", 3}, "b": 1}
    ensure(tf.serialize(a) == tf.serialize(b), "output is not canonical")
    ensure(" " not in tf.serialize(a))
    raises(TypeError, tf.serialize, {1: "x"})
    raises(TypeError, tf.serialize, {"a": [{2: "x"}]})
    raises(TypeError, tf.serialize, {"a": (object(),)})
    raises(TypeError, tf.serialize, {"a": [(1, {2: 3})]})


@check("ser.migrate_v1", "serial", 4, "v1 (no version field) migrates through v2 to v3")
def _(tf):
    text = json.dumps({"items": [["a", 1], ["b", (2, 3)], ["a", 3], ["ns/c", 4]]})
    data, meta = tf.deserialize(text)
    ensure(data == {"default/a": 3, "default/b": [2, 3], "ns/c": 4}, f"data={data}")
    ensure(meta == {"migrated_from": 1}, f"meta={meta}")
    data2, meta2 = tf.deserialize(json.dumps({"version": 1, "items": []}))
    ensure(data2 == {} and meta2 == {"migrated_from": 1})


@check("ser.migrate_v2", "serial", 3, "v2 -> v3: explicit namespaced key wins, meta preserved")
def _(tf):
    for order in ([("default/a", 1), ("a", 2)], [("a", 2), ("default/a", 1)]):
        doc = {"version": 2, "data": dict(order, **{"ns/b": 3}), "meta": {"x": 1}}
        data, meta = tf.deserialize(json.dumps(doc))
        ensure(data == {"default/a": 1, "ns/b": 3}, f"data={data} for {order}")
        ensure(meta == {"x": 1, "migrated_from": 2}, f"meta={meta}")
    d3, m3 = tf.deserialize(tf.serialize({"a/b": 1}, {"k": 2}))
    ensure(d3 == {"a/b": 1} and m3 == {"k": 2}, "v3 must pass through untouched")


@check("ser.versions", "serial", 2, "unsupported versions are rejected with UnsupportedVersion")
def _(tf):
    for v in (4, 0, -1, "2", None, True, 2.0, 99):
        raises(tf.UnsupportedVersion, tf.deserialize, json.dumps({"version": v, "data": {}}))
    raises(ValueError, tf.deserialize, "[1, 2]")


# ==========================================================================
# Engine (integration)
# ==========================================================================
def _record(engine):
    log = []
    engine.bus.subscribe("*", lambda t, p: log.append((t, p)))
    return log


@check("engine.dataflow", "engine", 3, "diamond dataflow, targets, params, report fields")
def _(tf):
    e = tf.Engine()
    e.add_task("sum", lambda c: c.inputs["l"] + c.inputs["r"], deps=("l", "r"))
    e.add_task("l", lambda c: c.inputs["root"] * 2, deps=("root",))
    e.add_task("r", lambda c: c.inputs["root"] + c.params["k"], deps=("root",))
    e.add_task("root", lambda c: 10)
    e.add_task("unrelated", lambda c: 1 / 0)
    rep = e.run(targets=["sum"], params={"k": 5})
    ensure(rep.results == {"root": 10, "l": 20, "r": 15, "sum": 35}, str(rep.results))
    ensure(rep.order == ["root", "l", "r", "sum"], str(rep.order))
    ensure(rep.status == {"root": "ok", "l": "ok", "r": "ok", "sum": "ok"} and rep.ok)
    ensure(rep.attempts == {"root": 1, "l": 1, "r": 1, "sum": 1} and rep.errors == {})
    rep2 = e.run()
    ensure(rep2.status["unrelated"] == "failed" and isinstance(rep2.errors["unrelated"], ZeroDivisionError))
    ensure(not rep2.ok)


@check("engine.skip_propagation", "engine", 4, "failures skip dependants transitively, independents still run")
def _(tf):
    called = []

    def mk(name, fail=False):
        def f(c):
            called.append(name)
            if fail:
                raise RuntimeError(name)
            return name
        return f

    e = tf.Engine()
    log = _record(e)
    e.add_task("A", mk("A", True))
    e.add_task("B", mk("B"), deps=("A",))
    e.add_task("C", mk("C"), deps=("B",))
    e.add_task("D", mk("D"))
    e.add_task("E", mk("E"), deps=("A", "D"))
    e.add_task("F", mk("F"), deps=("D",))
    rep = e.run()
    ensure(rep.status == {"A": "failed", "B": "skipped", "C": "skipped", "D": "ok", "E": "skipped", "F": "ok"},
           str(rep.status))
    ensure(called == ["A", "D", "F"] or sorted(called) == ["A", "D", "F"], f"called={called}")
    ensure(rep.attempts["B"] == 0 and rep.attempts["A"] == 1)
    skipped = {p["name"]: p["blocked_by"] for t, p in log if t == "task.skipped"}
    ensure(skipped == {"B": ["A"], "C": ["B"], "E": ["A"]}, str(skipped))
    ensure(set(rep.results) == {"D", "F"} and set(rep.errors) == {"A"})


@check("engine.retry_timing", "engine", 4, "retry events, backoff timing, no sleep after the final failure")
def _(tf):
    clock = tf.Clock()
    e = tf.Engine(clock=clock)
    log = _record(e)
    state = {"n": 0}

    def flaky(c):
        state["n"] += 1
        if state["n"] < 3:
            raise OSError("flaky")
        return "fine"

    e.add_task("t", flaky, retry=tf.RetryPolicy(max_attempts=4, base_delay=1, factor=2))
    rep = e.run()
    ensure(rep.status["t"] == "ok" and rep.attempts["t"] == 3 and rep.elapsed == 3.0, f"{rep.attempts} {rep.elapsed}")
    topics = [(t, (p or {}).get("attempt")) for t, p in log if t.startswith("task.")]
    ensure(topics == [("task.started", 1), ("task.retry", 1), ("task.started", 2), ("task.retry", 2),
                      ("task.started", 3), ("task.succeeded", 3)], str(topics))
    ensure([p["delay"] for t, p in log if t == "task.retry"] == [1, 2])

    clock2 = tf.Clock()
    e2 = tf.Engine(clock=clock2)

    def bad(c):
        raise ValueError(f"attempt {c.attempt}")

    e2.add_task("bad", bad, retry=tf.RetryPolicy(max_attempts=3, base_delay=1, factor=2))
    e2.add_task("fatal", bad, retry=tf.RetryPolicy(max_attempts=5, no_retry_on=(ValueError,)))
    r2 = e2.run()
    ensure(r2.attempts["bad"] == 3 and str(r2.errors["bad"]) == "attempt 3", f"{r2.attempts} {r2.errors}")
    ensure(r2.attempts["fatal"] == 1)
    ensure(clock2.now() == 3.0 and r2.elapsed == 3.0, f"clock={clock2.now()} (slept after the last attempt?)")


@check("engine.store_atomicity", "engine", 4, "every attempt is a transaction: failures leave no trace")
def _(tf):
    e = tf.Engine()
    seen = []

    def t(c):
        seen.append(c.store.get("partial"))
        c.store.set("partial", c.attempt)
        c.store.delete("keep")
        if c.attempt == 1:
            raise OSError("first")
        c.store.set("done", True)
        return "r"

    e.store.set("keep", "yes")
    e.add_task("t", t, retry=tf.RetryPolicy(max_attempts=2, base_delay=0))
    e.add_task("w", lambda c: c.store.set("w", 1) or (_ for _ in ()).throw(KeyError("w")))
    rep = e.run()
    ensure(seen == [None, None], f"second attempt saw leftovers of the first: {seen}")
    ensure(e.store.snapshot() == {"keep": None, "partial": 2, "done": True} or
           e.store.snapshot() == {"partial": 2, "done": True}, str(e.store.snapshot()))
    ensure(not e.store.has("w"), "failed task leaked store writes")
    ensure(e.store.depth == 0 and rep.status["w"] == "failed")


@check("engine.timeout", "engine", 3, "timeout: strictly greater, rolled back, retriable")
def _(tf):
    clock = tf.Clock()
    e = tf.Engine(clock=clock)

    def slow(dt, key):
        def f(c):
            c.store.set(key, 1)
            c.clock.advance(dt)
            return key
        return f

    e.add_task("exact", slow(5, "exact"), timeout=5)
    e.add_task("over", slow(5.5, "over"), timeout=5)
    rep = e.run()
    ensure(rep.status["exact"] == "ok" and e.store.has("exact"), "elapsed == timeout is allowed")
    ensure(rep.status["over"] == "failed" and isinstance(rep.errors["over"], tf.TaskTimeout), str(rep.status))
    ensure(not e.store.has("over"), "timed-out attempt must be rolled back")
    ensure("over" not in rep.results)

    e2 = tf.Engine(clock=tf.Clock())

    def second_time_fast(c):
        c.clock.advance(10 if c.attempt == 1 else 1)
        return c.attempt

    e2.add_task("r", second_time_fast, timeout=3, retry=tf.RetryPolicy(max_attempts=2, base_delay=2))
    rep2 = e2.run()
    ensure(rep2.status["r"] == "ok" and rep2.results["r"] == 2 and rep2.elapsed == 13.0, f"{rep2}")


@check("engine.cache_hits", "engine", 4, "cache: hit/miss, key order independence, only successes cached")
def _(tf):
    calls = {"n": 0, "f": 0}
    e = tf.Engine()
    log = _record(e)

    def work(c):
        calls["n"] += 1
        return {"v": c.params["x"]}

    def flaky(c):
        calls["f"] += 1
        raise RuntimeError("no")

    e.add_task("w", work, cacheable=True)
    e.add_task("fl", flaky, cacheable=True)
    r1 = e.run(targets=["w"], params={"x": 1, "y": 2})
    r2 = e.run(targets=["w"], params={"y": 2, "x": 1})
    ensure(calls["n"] == 1, f"key must not depend on dict order (calls={calls['n']})")
    ensure(r2.status["w"] == "cached" and r2.attempts["w"] == 0 and r2.results["w"] == {"v": 1})
    ensure(any(t == "task.cached" for t, _ in log))
    e.run(targets=["w"], params={"x": 2, "y": 2})
    ensure(calls["n"] == 2)
    e.run(targets=["fl"])
    e.run(targets=["fl"])
    ensure(calls["f"] == 2, "failed result must not be cached")
    ensure(r1.status["w"] == "ok")


@check("engine.cache_keys", "engine", 4, "cache key includes task name and version")
def _(tf):
    calls = []
    e = tf.Engine()
    e.add_task("a", lambda c: calls.append("a") or "A", cacheable=True)
    e.add_task("b", lambda c: calls.append("b") or "B", cacheable=True)
    rep = e.run(params={"p": 1})
    ensure(rep.results == {"a": "A", "b": "B"}, f"name collision in cache: {rep.results}")
    e.run(params={"p": 1})
    ensure(calls == ["a", "b"])
    e2 = tf.Engine()
    n = {"v": 0}
    e2.add_task("t", lambda c: n.__setitem__("v", n["v"] + 1) or n["v"], cacheable=True, version="1")
    e2.run()
    e2.run()
    ensure(n["v"] == 1)
    e3 = tf.Engine(cache=e2.cache)
    e3.add_task("t", lambda c: 42, cacheable=True, version="2")
    ensure(e3.run().results["t"] == 42, "a new task version must not hit the old cache entry")
    # inputs participate in the key
    e4 = tf.Engine()
    src = {"v": 1}
    e4.add_task("src", lambda c: src["v"])
    e4.add_task("dep", lambda c: c.inputs["src"] * 10, deps=("src",), cacheable=True)
    ensure(e4.run().results["dep"] == 10)
    src["v"] = 2
    ensure(e4.run().results["dep"] == 20, "changed inputs must miss the cache")


@check("engine.injected_cache", "engine", 3, "injected EMPTY cache is used; results are defensively copied; TTL honoured")
def _(tf):
    clock = tf.Clock()
    cache = tf.TTLCache(8, ttl=10, clock=clock)
    e = tf.Engine(clock=clock, cache=cache)
    n = {"c": 0}
    e.add_task("t", lambda c: n.__setitem__("c", n["c"] + 1) or [1, 2], cacheable=True)
    rep = e.run()
    ensure(e.cache is cache and len(cache) == 1, "engine ignored the injected (empty) cache")
    rep.results["t"].append("poison")
    rep2 = e.run()
    ensure(rep2.status["t"] == "cached" and rep2.results["t"] == [1, 2], f"cache was poisoned: {rep2.results}")
    rep2.results["t"].append("again")
    ensure(e.run().results["t"] == [1, 2])
    clock.advance(10)
    rep3 = e.run()
    ensure(rep3.status["t"] == "ok" and n["c"] == 2, "expired cache entry must be recomputed")
    # empty but injected collaborators in general
    bus, store = tf.EventBus(), tf.Store()
    e5 = tf.Engine(bus=bus, store=store)
    ensure(e5.bus is bus and e5.store is store)


@check("engine.input_isolation", "engine", 3, "consumers cannot corrupt other consumers or the report")
def _(tf):
    e = tf.Engine()
    keep = {}

    def producer(c):
        out = {"v": [1]}
        keep["ref"] = out
        return out

    def vandal(c):
        c.inputs["P"]["v"].append("evil")
        return "x"

    seen = {}

    def observer(c):
        seen["v"] = copy.deepcopy(c.inputs["P"])
        return 1

    e.add_task("P", producer)
    e.add_task("V", vandal, deps=("P",))
    e.add_task("O", observer, deps=("P",))
    rep = e.run()
    ensure(seen["v"] == {"v": [1]}, f"observer saw {seen['v']}")
    ensure(rep.results["P"] == {"v": [1]}, f"report corrupted: {rep.results['P']}")
    keep["ref"]["v"].append("late")
    ensure(rep.results["P"] == {"v": [1]}, "report must not alias the object the task returned")


@check("engine.cancel", "engine", 3, "cancel affects the current run only")
def _(tf):
    e = tf.Engine()
    log = _record(e)
    ran = []

    def a(c):
        ran.append("a")
        return 1

    def b(c):
        ran.append("b")
        if len(ran) < 3:
            c.cancel()
        return 2

    e.add_task("a", a)
    e.add_task("b", b, deps=("a",))
    e.add_task("c", lambda c: ran.append("c") or 3, deps=("b",))
    e.add_task("d", lambda c: ran.append("d") or 4)
    rep = e.run()
    ensure(rep.status == {"a": "ok", "b": "ok", "c": "cancelled", "d": "cancelled"}, str(rep.status))
    ensure(rep.attempts["c"] == 0 and [p["name"] for t, p in log if t == "task.cancelled"] == ["c", "d"])
    ran_before = len(ran)
    rep2 = e.run()
    ensure(all(s == "ok" for s in rep2.status.values()), f"cancel flag leaked into next run: {rep2.status}")
    e.cancel()
    rep3 = e.run()
    ensure(all(s == "ok" for s in rep3.status.values()), "cancel() outside a run must have no effect")
    ensure(ran_before == 2, f"ran_before={ran_before}")


@check("engine.rate_limit", "engine", 4, "one token per real attempt; cache hits are free")
def _(tf):
    clock = tf.Clock()
    e = tf.Engine(clock=clock, rate_limiter=tf.TokenBucket(2, 1, clock))
    for i in range(4):
        e.add_task(f"t{i}", lambda c: 1)
    rep = e.run()
    ensure(rep.elapsed == 2.0 and clock.now() == 2.0, f"elapsed={rep.elapsed}")

    clock = tf.Clock()
    bucket = tf.TokenBucket(2, 1, clock)
    e = tf.Engine(clock=clock, rate_limiter=bucket)
    e.add_task("a", lambda c: 1 if c.attempt > 1 else (_ for _ in ()).throw(OSError()),
               retry=tf.RetryPolicy(max_attempts=2, base_delay=0))
    e.add_task("b", lambda c: 2, deps=("a",))
    rep = e.run()
    ensure(rep.status == {"a": "ok", "b": "ok"} and rep.attempts["a"] == 2)
    ensure(rep.elapsed == 1.0, f"each attempt needs a token; elapsed={rep.elapsed}")

    clock = tf.Clock()
    bucket = tf.TokenBucket(2, 1, clock)
    e = tf.Engine(clock=clock, rate_limiter=bucket)
    e.add_task("c", lambda c: 5, cacheable=True)
    e.run()
    ensure(bucket.tokens == 1.0)
    rep = e.run()
    ensure(rep.status["c"] == "cached" and bucket.tokens == 1.0, f"tokens={bucket.tokens}")
    e.add_task("boom", lambda c: 1 / 0)
    e.add_task("after", lambda c: 1, deps=("boom",))
    e.run(targets=["after"])
    ensure(clock.now() == 0.0 and bucket.tokens == 0.0,
           f"only the real attempt of 'boom' costs a token (clock={clock.now()}, tokens={bucket.tokens})")


@check("engine.fail_fast", "engine", 4, "invalid graphs fail before anything runs")
def _(tf):
    side = []
    e = tf.Engine()
    log = _record(e)
    e.add_task("a", lambda c: side.append("a"))
    e.add_task("b", lambda c: side.append("b"), deps=("c",))
    e.add_task("c", lambda c: side.append("c"), deps=("b",))
    raises(tf.CycleError, e.run)
    ensure(side == [] and log == [], f"side effects before validation: {side}, events={log}")
    raises(tf.CycleError, e.run, ["a"])  # whole graph is validated even for a subset
    e2 = tf.Engine()
    e2.add_task("x", lambda c: side.append("x"), deps=("ghost",))
    e2.add_task("y", lambda c: side.append("y"))
    raises(tf.MissingDependencyError, e2.run)
    ensure(side == [])
    e2.add_task("ghost", lambda c: 1)  # forward references are legal
    ensure(e2.run().ok)
    raises(tf.DuplicateNodeError, e2.add_task, "x", lambda c: 0)
    e3 = tf.Engine()
    e3.add_task("s", lambda c: side.append("s"), deps=("s",))
    raises(tf.CycleError, e3.run)


@check("engine.events", "engine", 2, "event sequence of a mixed run")
def _(tf):
    e = tf.Engine()
    log = _record(e)
    e.add_task("ok", lambda c: 1)
    e.add_task("bad", lambda c: 1 / 0)
    e.add_task("dep", lambda c: 2, deps=("bad",))
    e.run()
    seq = [(t, (p or {}).get("name")) for t, p in log]
    ensure(seq == [("run.started", None), ("task.started", "ok"), ("task.succeeded", "ok"),
                   ("task.started", "bad"), ("task.failed", "bad"), ("task.skipped", "dep"),
                   ("run.finished", None)], str(seq))
    ensure(log[0][1] == {"tasks": ["ok", "bad", "dep"]} and log[-1][1]["ok"] is False)
    # a crashing subscriber must never break a run
    e.bus.subscribe("task.started", lambda t, p: 1 / 0, priority=100)
    ensure(e.run().status["ok"] == "ok")


# ==========================================================================
# randomized differential tests (guard against special-casing)
# ==========================================================================
@check("fuzz.cache", "fuzz", 4, "TTLCache vs naive model, 60 random programs")
def _(tf):
    for seed in range(60):
        rnd = random.Random(seed)
        cap = rnd.randint(1, 5)
        dttl = rnd.choice([None, 5, 10])
        clock = tf.Clock()
        cache = tf.TTLCache(cap, ttl=dttl, clock=clock)
        model = []  # list of [key, value, expires] LRU -> MRU

        def alive(en):
            return en[2] is None or clock.now() < en[2]

        for step in range(120):
            op = rnd.choice(["put", "put", "get", "get", "adv", "len", "keys", "in"])
            k = rnd.choice("abcdefg")
            if op == "put":
                ttl = rnd.choice([None, None, 0, 1, 3, 8])
                eff = dttl if ttl is None else ttl
                exp = None if eff is None else clock.now() + eff
                v = rnd.randint(0, 99)
                existing = [en for en in model if en[0] == k]
                if existing:
                    model.remove(existing[0])
                    model.append([k, v, exp])
                else:
                    model[:] = [en for en in model if alive(en)]
                    while len(model) >= cap:
                        model.pop(0)
                    model.append([k, v, exp])
                cache.put(k, v, ttl=ttl)
            elif op == "get":
                ex = [en for en in model if en[0] == k]
                want = None
                if ex:
                    if alive(ex[0]):
                        model.remove(ex[0])
                        model.append(ex[0])
                        want = ex[0][1]
                    else:
                        model.remove(ex[0])
                got = cache.get(k)
                ensure(got == want, f"seed={seed} step={step} get({k}) -> {got}, want {want}")
            elif op == "adv":
                dt = rnd.choice([0, 1, 2, 5])
                clock.advance(dt)
            elif op == "len":
                ensure(len(cache) == sum(1 for en in model if alive(en)), f"seed={seed} step={step} len")
                model[:] = [en for en in model if alive(en)]
            elif op == "keys":
                model[:] = [en for en in model if alive(en)]
                ensure(cache.keys() == [en[0] for en in model], f"seed={seed} step={step} keys")
            else:
                ensure((k in cache) == any(en[0] == k and alive(en) for en in model), f"seed={seed} in")


@check("fuzz.store", "fuzz", 4, "Store vs naive snapshot-stack model, 80 random programs")
def _(tf):
    for seed in range(80):
        rnd = random.Random(1000 + seed)
        s = tf.Store()
        model = {}
        stack = []
        for step in range(150):
            op = rnd.choice(["set", "set", "del", "begin", "commit", "rollback", "get", "snap"])
            k = rnd.choice("pqrs")
            if op == "set":
                v = [rnd.randint(0, 9)]
                s.set(k, v)
                model[k] = copy.deepcopy(v)
                v.append(77)
            elif op == "del":
                ensure(s.delete(k) == (k in model), f"seed={seed} step={step} delete result")
                model.pop(k, None)
            elif op == "begin":
                s.begin()
                stack.append(copy.deepcopy(model))
            elif op == "commit":
                if stack:
                    s.commit()
                    stack.pop()
                else:
                    raises(tf.TransactionError, s.commit)
            elif op == "rollback":
                if stack:
                    s.rollback()
                    model = stack.pop()
                else:
                    raises(tf.TransactionError, s.rollback)
            elif op == "get":
                ensure(s.get(k) == model.get(k), f"seed={seed} step={step} get({k})")
            ensure(s.keys() == sorted(model), f"seed={seed} step={step} op={op} keys {s.keys()} vs {sorted(model)}")
            ensure(s.snapshot() == model, f"seed={seed} step={step} snapshot")
            ensure(s.depth == len(stack))
        while stack:
            s.rollback()
            model = stack.pop()
        ensure(s.snapshot() == model)


def _naive_order(nodes, deps, targets=None):
    idx = {n: i for i, n in enumerate(nodes)}
    want = set(nodes)
    if targets is not None:
        want, st = set(), list(targets)
        while st:
            n = st.pop()
            if n not in want:
                want.add(n)
                st.extend(deps[n])
    done, out = set(), []
    while len(out) < len(want):
        ready = [n for n in want if n not in done and all(d in done for d in deps[n])]
        n = min(ready, key=lambda x: idx[x])
        out.append(n)
        done.add(n)
    return out


def _random_dag(rnd, size):
    names = [f"n{i}" for i in range(size)]
    rank = {n: rnd.random() for n in names}
    insertion = names[:]
    rnd.shuffle(insertion)
    deps = {}
    for n in insertion:
        cands = [m for m in names if rank[m] < rank[n]]
        k = rnd.randint(0, min(3, len(cands)))
        d = rnd.sample(cands, k) if k else []
        if d and rnd.random() < 0.3:
            d = d + [rnd.choice(d)]
        deps[n] = d
    return insertion, deps


@check("fuzz.dag", "fuzz", 5, "DAG vs naive model: order, targets, levels, descendants, cycles")
def _(tf):
    for seed in range(60):
        rnd = random.Random(5000 + seed)
        nodes, deps = _random_dag(rnd, rnd.randint(1, 25))
        g = tf.DAG()
        for n in nodes:
            g.add(n, deps[n])
        uniq = {n: list(dict.fromkeys(d)) for n, d in deps.items()}
        ensure(g.order() == _naive_order(nodes, uniq), f"seed={seed} full order")
        tg = rnd.sample(nodes, rnd.randint(0, min(3, len(nodes))))
        ensure(g.order(tg) == _naive_order(nodes, uniq, tg), f"seed={seed} targets={tg}")
        lvl = {}
        for n in _naive_order(nodes, uniq):
            lvl[n] = 0 if not uniq[n] else 1 + max(lvl[d] for d in uniq[n])
        want_levels = [[n for n in nodes if lvl[n] == i] for i in range(max(lvl.values()) + 1)]
        ensure(g.levels() == want_levels, f"seed={seed} levels")
        probe = rnd.choice(nodes)
        desc, st = set(), [probe]
        while st:
            cur = st.pop()
            for n in nodes:
                if cur in uniq[n] and n not in desc:
                    desc.add(n)
                    st.append(n)
        ensure(g.descendants(probe) == desc, f"seed={seed} descendants")
        # inject a back edge -> must report a valid cycle
        if len(nodes) >= 2:
            g2 = tf.DAG()
            a, b = rnd.sample(nodes, 2)
            # add both edges a->b and b->a: a cycle for sure
            first = {n: list(uniq[n]) for n in nodes}
            first[a] = list(dict.fromkeys(first[a] + [b]))
            first[b] = list(dict.fromkeys(first[b] + [a]))
            for n in nodes:
                g2.add(n, first[n])
            e = raises(tf.CycleError, g2.validate)
            c = e.cycle
            ensure(len(c) >= 2 and c[0] == c[-1], f"seed={seed} cycle shape {c}")
            ensure(len(set(c[:-1])) == len(c) - 1, f"seed={seed} cycle repeats nodes {c}")
            for u, v in zip(c, c[1:]):
                ensure(v in g2.deps(u), f"seed={seed} bogus cycle edge {u}->{v}")


@check("fuzz.engine", "fuzz", 5, "Engine vs model: random graphs with random failures, retries and cache")
def _(tf):
    for seed in range(40):
        rnd = random.Random(9000 + seed)
        nodes, deps = _random_dag(rnd, rnd.randint(1, 14))
        uniq = {n: list(dict.fromkeys(d)) for n, d in deps.items()}
        fail_plan = {n: rnd.choice([0, 0, 0, 1, 2, 99]) for n in nodes}   # failures before success
        max_att = {n: rnd.choice([1, 2, 3]) for n in nodes}
        calls = {n: 0 for n in nodes}
        clock = tf.Clock()
        e = tf.Engine(clock=clock)

        def mk(n):
            def f(c):
                calls[n] += 1
                if calls[n] <= fail_plan[n]:
                    raise OSError(n)
                return {"n": n, "in": sorted(c.inputs)}
            return f

        for n in nodes:
            e.add_task(n, mk(n), deps=deps[n], retry=tf.RetryPolicy(max_attempts=max_att[n], base_delay=1, factor=2),
                       cacheable=rnd.random() < 0.3)
        order = _naive_order(nodes, uniq)
        rep = e.run()
        ensure(rep.order == order, f"seed={seed} order")
        status, attempts, t = {}, {}, 0.0
        for n in order:
            if any(status[d] not in ("ok", "cached") for d in uniq[n]):
                status[n], attempts[n] = "skipped", 0
                continue
            made = min(max_att[n], fail_plan[n] + 1)
            attempts[n] = made
            if fail_plan[n] < max_att[n]:
                status[n] = "ok"
                t += sum(2 ** i for i in range(made - 1))
            else:
                status[n] = "failed"
                t += sum(2 ** i for i in range(made - 1))
        ensure(rep.status == status, f"seed={seed} status {rep.status} vs {status}")
        ensure(rep.attempts == attempts, f"seed={seed} attempts")
        ensure(rep.elapsed == t == clock.now(), f"seed={seed} elapsed {rep.elapsed} vs {t}")
        for n in nodes:
            ensure(calls[n] == attempts[n], f"seed={seed} {n} called {calls[n]} times, expected {attempts[n]}")
            if status[n] == "ok":
                ensure(rep.results[n] == {"n": n, "in": sorted(uniq[n])})
        # the second run: ok+cacheable tasks are cached, the rest behave deterministically again
        rep2 = e.run()
        for n in nodes:
            if rep.status[n] == "ok" and e._tasks[n].cacheable and all(rep2.status[d] in ("ok", "cached") for d in uniq[n]):
                ensure(rep2.status[n] == "cached", f"seed={seed} {n} should be cached, is {rep2.status[n]}")


# ==========================================================================
# runner
# ==========================================================================
class _Timeout(Exception):
    pass


def _alarm(_sig, _frm):
    raise _Timeout("check timed out")


def load_module(path):
    spec = importlib.util.spec_from_file_location("taskflow_under_test", path)
    mod = importlib.util.module_from_spec(spec)
    sys.modules["taskflow_under_test"] = mod
    spec.loader.exec_module(mod)
    return mod


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("module")
    ap.add_argument("--verbose", "-v", action="store_true")
    ap.add_argument("--json", dest="json_out")
    ap.add_argument("--timeout", type=int, default=20)
    args = ap.parse_args()

    results = []
    try:
        tf = load_module(args.module)
    except BaseException:  # noqa: BLE001
        print("IMPORT FAILED - score 0/100")
        traceback.print_exc()
        if args.json_out:
            json.dump({"score": 0, "import_failed": True}, open(args.json_out, "w"))
        return 1

    has_alarm = hasattr(signal, "SIGALRM")
    if has_alarm:
        signal.signal(signal.SIGALRM, _alarm)
    sys.setrecursionlimit(1000)

    for cid, area, weight, title, fn in CHECKS:
        t0 = time.time()
        err = None
        try:
            if has_alarm:
                signal.alarm(args.timeout)
            fn(tf)
        except BaseException as e:  # noqa: BLE001
            err = f"{type(e).__name__}: {e}"
            if args.verbose:
                err += "\n" + "".join(traceback.format_exc().splitlines(True)[-4:])
        finally:
            if has_alarm:
                signal.alarm(0)
        results.append({"id": cid, "area": area, "weight": weight, "title": title,
                        "passed": err is None, "error": err, "seconds": round(time.time() - t0, 3)})

    total = sum(r["weight"] for r in results)
    got = sum(r["weight"] for r in results if r["passed"])
    score = round(100.0 * got / total, 1)

    for r in results:
        mark = "PASS" if r["passed"] else "FAIL"
        print(f"[{mark}] {r['id']:<26} w={r['weight']}  {r['title']}")
        if not r["passed"] and args.verbose:
            print("        " + r["error"].replace("\n", "\n        "))

    print("\nby area:")
    areas = {}
    for r in results:
        a = areas.setdefault(r["area"], [0, 0, 0, 0])
        a[0] += r["weight"]
        a[1] += r["weight"] if r["passed"] else 0
        a[2] += 1
        a[3] += 1 if r["passed"] else 0
    for name, (w, g, n, p) in areas.items():
        print(f"  {name:<8} {p}/{n} checks   {g}/{w} weight")
    npass = sum(1 for r in results if r["passed"])
    print(f"\nchecks passed: {npass}/{len(results)}")
    print(f"SCORE: {score}/100")
    if args.json_out:
        json.dump({"score": score, "passed": npass, "total": len(results), "areas": {k: v for k, v in areas.items()},
                   "results": results}, open(args.json_out, "w"), ensure_ascii=False, indent=2)
    return 0


if __name__ == "__main__":
    sys.exit(main())
