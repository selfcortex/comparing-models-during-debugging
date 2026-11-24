import sys
sys.path.insert(0, '/home/zetta/bench/workspace')
from taskflow import *
from taskflow import Clock, TokenBucket, TTLCache, EventBus, DAG, RetryPolicy, Store, Engine

def test_clock():
    c = Clock()
    assert c.now() == 0.0
    c.advance(5)
    assert c.now() == 5.0
    try:
        c.advance(-1)
        assert False
    except ValueError:
        pass

def test_token_bucket():
    c = Clock()
    tb = TokenBucket(10, 2, c)
    assert tb.tokens == 10
    assert tb.try_acquire(3)
    assert tb.tokens == 7
    c.advance(1)
    # 7 + 2*1 = 9
    assert tb.tokens == 9
    # fractions not lost
    c = Clock()
    tb = TokenBucket(10, 0.5, c)
    c.advance(1)
    # 10 already full, no credit
    assert tb.tokens == 10
    tb.try_acquire(10)
    c.advance(0.5)
    # 0 + 0.5*0.5 = 0.25
    assert abs(tb.tokens - 0.25) < 1e-9
    # time_until
    c = Clock()
    tb = TokenBucket(10, 2, c)
    tb.try_acquire(10)
    assert tb.time_until(4) == 2.0
    c.advance(1)
    # 2 tokens now, need 4, missing 2, 2/2 = 1.0
    assert tb.time_until(4) == 1.0
    # n > capacity
    try:
        tb.try_acquire(11)
        assert False
    except ValueError:
        pass

def test_ttl_cache():
    c = Clock()
    cache = TTLCache(3, ttl=10, clock=c)
    cache.put('a', 1)
    cache.put('b', 2)
    cache.put('c', 3)
    # get a -> LRU update
    assert cache.get('a') == 1
    cache.put('d', 4)  # evicts b (LRU after a moved)
    assert cache.get('b') is None
    assert cache.get('a') == 1
    # ttl=0 expires immediately
    c2 = Clock()
    cache2 = TTLCache(3, clock=c2)
    cache2.put('x', 1, ttl=0)
    assert cache2.get('x') is None
    assert 'x' not in cache2
    # boundary: expired at t+T
    c3 = Clock()
    cache3 = TTLCache(3, clock=c3)
    cache3.put('y', 1, ttl=5)
    c3.advance(5)
    assert cache3.get('y') is None
    # len sees only live
    c4 = Clock()
    cache4 = TTLCache(10, clock=c4)
    cache4.put('p', 1, ttl=2)
    cache4.put('q', 2, ttl=None)
    c4.advance(3)
    assert len(cache4) == 1
    assert 'p' not in cache4
    assert 'q' in cache4
    assert cache4.keys() == ['q']

def test_event_bus():
    bus = EventBus()
    calls = []
    bus.subscribe('a.b', lambda t, p: calls.append(('a.b', t, p)), priority=0)
    bus.subscribe('a.*', lambda t, p: calls.append(('a.*', t, p)), priority=1)
    bus.subscribe('*', lambda t, p: calls.append(('*', t, p)), priority=0)
    bus.emit('a.b', 'x')
    # priority 1 first (a.*), then priority 0 in subscription order (a.b, *)
    assert calls[0][0] == 'a.*'
    assert calls[1][0] == 'a.b'
    assert calls[2][0] == '*'
    # prefix.* does not match bare prefix
    calls2 = []
    bus2 = EventBus()
    bus2.subscribe('job.*', lambda t, p: calls2.append(t))
    bus2.emit('job')
    assert calls2 == []
    bus2.emit('jobs.started')
    assert calls2 == []
    bus2.emit('job.started')
    assert calls2 == ['job.started']
    # once removed before handler
    bus3 = EventBus()
    count = [0]
    def h(t, p):
        count[0] += 1
        if count[0] == 1:
            bus3.emit('x', 1)
    bus3.subscribe('x', h, once=True)
    bus3.emit('x', 1)
    assert count[0] == 1
    # unsubscribe during emit
    bus4 = EventBus()
    order = []
    t1 = bus4.subscribe('t', lambda t, p: order.append(1))
    def mid(t, p):
        order.append(2)
        bus4.unsubscribe(t2)
    t2 = bus4.subscribe('t', mid, priority=1)
    t3 = bus4.subscribe('t', lambda t, p: order.append(3))
    bus4.emit('t')
    # priority: t2(1) first (calls itself, unsubscribes itself - already in matching, still called)
    # then t1(0), t3(0) in sub order
    assert order == [2, 1, 3]
    # now: a handler unsubscribed by an EARLIER handler is not called
    bus5 = EventBus()
    o2 = []
    def first(t, p):
        o2.append('first')
        bus5.unsubscribe(late_token)
    def late(t, p):
        o2.append('late')
    first_tok = bus5.subscribe('u', first, priority=1)
    late_token = bus5.subscribe('u', late, priority=0)
    bus5.emit('u')
    assert o2 == ['first']
    # exceptions don't stop dispatch
    bus5 = EventBus()
    errs = []
    bus5.subscribe('e', lambda t, p: errs.append(1) or 1/0)
    bus5.subscribe('e', lambda t, p: errs.append(2))
    result = bus5.emit('e')
    assert len(result) == 1
    assert errs == [1, 2]

def test_dag():
    d = DAG()
    d.add('c', ['a', 'b'])
    d.add('a')
    d.add('b')
    assert d.order() == ['a', 'b', 'c']
    # self-dep is a cycle
    d2 = DAG()
    d2.add('x', ['x'])
    try:
        d2.validate()
        assert False
    except CycleError as e:
        assert e.cycle == ['x', 'x']
    # dup deps collapse
    d3 = DAG()
    d3.add('n', ['a', 'a', 'b'])
    assert d3.deps('n') == ('a', 'b')
    # order with targets
    d4 = DAG()
    d4.add('a')
    d4.add('b', ['a'])
    d4.add('c', ['b'])
    d4.add('d')
    assert d4.order(['c']) == ['a', 'b', 'c']
    # levels
    d5 = DAG()
    d5.add('a')
    d5.add('b')
    d5.add('c', ['a', 'b'])
    lv = d5.levels()
    assert lv[0] == ['a', 'b']
    assert lv[1] == ['c']
    # descendants
    d6 = DAG()
    d6.add('a')
    d6.add('b', ['a'])
    d6.add('c', ['b'])
    assert d6.descendants('a') == {'b', 'c'}
    # big chain no recursion
    d7 = DAG()
    for i in range(20000):
        d7.add(str(i), [str(i-1)] if i else [])
    o = d7.order()
    assert len(o) == 20000
    assert o[0] == '0'

def test_retry():
    r = RetryPolicy(max_attempts=3, base_delay=1.0, factor=2.0, max_delay=30.0)
    assert r.delay(1) == 1.0
    assert r.delay(2) == 2.0
    assert r.delay(3) == 4.0
    # no overflow for huge attempt
    r2 = RetryPolicy(max_attempts=100, base_delay=1.0, factor=2.0, max_delay=30.0)
    assert r2.delay(10000) == 30.0
    # should_retry
    assert r.should_retry(Exception(), 1) == True
    assert r.should_retry(Exception(), 2) == True
    assert r.should_retry(Exception(), 3) == False
    # no_retry_on wins
    r3 = RetryPolicy(retry_on=(Exception,), no_retry_on=(ValueError,))
    assert r3.should_retry(ValueError(), 1) == False
    assert r3.should_retry(KeyError(), 1) == True

def test_store():
    s = Store()
    s.set('a', [1, 2])
    v = s.get('a')
    v.append(3)
    assert s.get('a') == [1, 2]
    # nested tx
    s.begin()
    s.set('b', 1)
    s.commit()  # merges to parent (none -> base)
    assert s.get('b') == 1
    s.begin()
    s.set('c', 1)
    s.begin()
    s.set('d', 1)
    s.rollback()
    assert s.get('d') is None
    assert s.get('c') == 1
    s.rollback()
    assert s.get('c') is None
    # outer rollback discards inner commits
    s.begin()
    s.set('x', 1)
    s.begin()
    s.set('y', 2)
    s.commit()
    assert s.get('y') == 2
    s.rollback()
    assert s.get('y') is None
    assert s.get('x') is None
    # delete in tx
    s.set('k', 1)
    s.begin()
    assert s.delete('k') == True
    assert s.get('k') is None
    s.rollback()
    assert s.get('k') == 1
    # commit without tx
    try:
        s.commit()
        assert False
    except TransactionError:
        pass

def test_serialize():
    # tuple/set round trip
    data = {"a": (1, 2), "b": {3, 4}, "c": [(1, {2,})]}
    text = serialize(data)
    d2, m2 = deserialize(text)
    assert d2["a"] == (1, 2)
    assert d2["b"] == {3, 4}
    assert d2["c"] == [(1, {2,})]
    # dict keys must be str
    try:
        serialize({1: 2})
        assert False
    except TypeError:
        pass
    # v1 migration
    import json
    v1 = json.dumps({"items": [["a", 1], ["b", 2]]})
    d, m = deserialize(v1)
    assert d == {"default/a": 1, "default/b": 2}
    assert m.get("migrated_from") == 1
    # v2 migration
    v2 = json.dumps({"version": 2, "data": {"x": 1}, "meta": {}})
    d, m = deserialize(v2)
    assert d == {"default/x": 1}
    assert m.get("migrated_from") == 2
    # version bool not valid
    try:
        deserialize(json.dumps({"version": True, "data": {}}))
        assert False
    except UnsupportedVersion:
        pass
    # version 4 not valid
    try:
        deserialize(json.dumps({"version": 4, "data": {}}))
        assert False
    except UnsupportedVersion:
        pass

def test_engine_basic():
    c = Clock()
    e = Engine(clock=c)
    results = {}
    def a(ctx):
        return 1
    def b(ctx):
        return ctx.inputs['a'] + 1
    e.add_task('a', a)
    e.add_task('b', b, deps=['a'])
    r = e.run()
    assert r.ok
    assert r.results['a'] == 1
    assert r.results['b'] == 2
    assert r.order == ['a', 'b']
    assert r.attempts['a'] == 1
    assert r.attempts['b'] == 1

def test_engine_skip():
    e = Engine()
    def fail(ctx):
        raise ValueError('x')
    def b(ctx):
        return 1
    def c(ctx):
        return 2
    e.add_task('a', fail)
    e.add_task('b', b, deps=['a'])
    e.add_task('c', c, deps=['b'])
    r = e.run()
    assert r.status['a'] == 'failed'
    assert r.status['b'] == 'skipped'
    assert r.status['c'] == 'skipped'
    assert r.order == ['a', 'b', 'c']

def test_engine_retry():
    c = Clock()
    e = Engine(clock=c)
    attempts = [0]
    def flaky(ctx):
        attempts[0] += 1
        if attempts[0] < 3:
            raise ValueError('fail')
        return 'ok'
    e.add_task('t', flaky, retry=RetryPolicy(max_attempts=5, base_delay=1.0))
    r = e.run()
    assert r.status['t'] == 'ok'
    assert r.attempts['t'] == 3
    # clock advanced by delays: 1 + 2 = 3
    assert c.now() == 3.0

def test_engine_cancel():
    e = Engine()
    def cancel_task(ctx):
        ctx.cancel()
        return 1
    def b(ctx):
        return 2
    e.add_task('a', cancel_task)
    e.add_task('b', b)
    r = e.run()
    assert r.status['a'] == 'ok'
    assert r.status['b'] == 'cancelled'
    # next run: cancel forgotten, but a cancels again
    r2 = e.run()
    assert r2.status['a'] == 'ok'
    assert r2.status['b'] == 'cancelled'
    # cancel while nothing running has no effect
    e2 = Engine()
    def t1(ctx): return 1
    e2.add_task('x', t1)
    e2.cancel()
    r3 = e2.run()
    assert r3.status['x'] == 'ok'

def test_engine_cache():
    e = Engine()
    calls = [0]
    def t(ctx):
        calls[0] += 1
        return 'r'
    e.add_task('t', t, cacheable=True)
    r1 = e.run()
    assert r1.status['t'] == 'ok'
    assert calls[0] == 1
    r2 = e.run()
    assert r2.status['t'] == 'cached'
    assert calls[0] == 1
    assert r2.attempts['t'] == 0

def test_engine_timeout():
    c = Clock()
    e = Engine(clock=c)
    def slow(ctx):
        ctx.clock.advance(10)
        return 1
    e.add_task('t', slow, timeout=5)
    r = e.run()
    assert r.status['t'] == 'failed'
    assert isinstance(r.errors['t'], TaskTimeout)

def test_engine_rate_limiter():
    c = Clock()
    tb = TokenBucket(2, 1, c)
    e = Engine(clock=c, rate_limiter=tb)
    def t1(ctx): return 1
    def t2(ctx): return 2
    def t3(ctx): return 3
    e.add_task('a', t1)
    e.add_task('b', t2)
    e.add_task('c', t3)
    r = e.run()
    # 3 tasks, 2 tokens, rate 1/s -> need 1s wait
    assert c.now() == 1.0

if __name__ == '__main__':
    fns = [v for k, v in list(globals().items()) if k.startswith('test_')]
    for fn in fns:
        try:
            fn()
            print(f"PASS {fn.__name__}")
        except Exception as ex:
            print(f"FAIL {fn.__name__}: {ex}")
            import traceback; traceback.print_exc()
