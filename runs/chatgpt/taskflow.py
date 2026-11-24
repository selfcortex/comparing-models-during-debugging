"""taskflow - a tiny dependency-aware task execution engine.

The module is deterministic: there is no real time, no threads and no
randomness.  Time is provided by an injectable ``Clock``.

THE DOCSTRINGS ARE THE CONTRACT.  Whenever the code and a docstring disagree,
the docstring is right and the code is wrong.

Building blocks
---------------
Clock, TokenBucket, TTLCache, EventBus, DAG, RetryPolicy, Store,
serialize/deserialize and finally Engine, which glues everything together.
"""
from __future__ import annotations

import copy
import hashlib
import heapq
import json
from collections import OrderedDict
from contextlib import contextmanager
from dataclasses import dataclass, field

__all__ = [
    "TaskflowError", "CycleError", "MissingDependencyError", "DuplicateNodeError",
    "TransactionError", "UnsupportedVersion", "TaskTimeout",
    "Clock", "TokenBucket", "TTLCache", "EventBus", "DAG", "RetryPolicy",
    "Store", "serialize", "deserialize", "TaskContext", "RunReport", "Engine",
]


# --------------------------------------------------------------------------
# errors
# --------------------------------------------------------------------------
class TaskflowError(Exception):
    """Base class of all errors raised on purpose by this module."""


class CycleError(TaskflowError):
    """``.cycle`` is a list of nodes ``[n0, n1, ..., nk, n0]``: first == last,
    and every node depends on the node that follows it.  The list contains
    ONLY the nodes that take part in the cycle."""

    def __init__(self, cycle):
        super().__init__(" -> ".join(map(str, cycle)))
        self.cycle = list(cycle)


class MissingDependencyError(TaskflowError):
    def __init__(self, node, missing):
        super().__init__(f"{node!r} depends on unknown node {missing!r}")
        self.node = node
        self.missing = missing


class DuplicateNodeError(TaskflowError):
    pass


class TransactionError(TaskflowError):
    pass


class UnsupportedVersion(TaskflowError):
    pass


class TaskTimeout(TaskflowError):
    pass


# --------------------------------------------------------------------------
# clock
# --------------------------------------------------------------------------
class Clock:
    """Manual clock.  ``sleep`` simply advances time.  Time never goes back:
    negative durations raise ValueError."""

    def __init__(self, start=0.0):
        self._now = float(start)

    def now(self):
        return self._now

    def advance(self, dt):
        if dt < 0:
            raise ValueError("time cannot go backwards")
        self._now += dt

    sleep = advance


# --------------------------------------------------------------------------
# rate limiting
# --------------------------------------------------------------------------
class TokenBucket:
    """Classic token bucket.

    * starts full, holds at most ``capacity`` tokens (a float);
    * accumulates ``refill_rate`` tokens per second of *clock* time,
      fractions of a token are NOT lost;
    * time spent while the bucket is full gives no credit: after a long idle
      period the bucket holds exactly ``capacity`` tokens, never more;
    * ``try_acquire(n)`` takes n tokens and returns True, or returns False and
      takes nothing.  ``n <= 0`` or ``n > capacity`` raises ValueError;
    * ``time_until(n)`` is the number of seconds until n tokens are available
      (0.0 if they are available right now).
    """

    def __init__(self, capacity, refill_rate, clock):
        if capacity <= 0 or refill_rate <= 0:
            raise ValueError("capacity and refill_rate must be positive")
        self.capacity = float(capacity)
        self.rate = float(refill_rate)
        self.clock = clock
        self._tokens = float(capacity)
        self._last = clock.now()

    def _refill(self):
        now = self.clock.now()
        elapsed = now - self._last
        if elapsed <= 0:
            return

        if self._tokens >= self.capacity:
            # Discard time accumulated while full.  If tokens are consumed
            # later, only time after that consumption can refill the bucket.
            self._tokens = self.capacity
            self._last = now
            return

        self._tokens = min(self.capacity, self._tokens + elapsed * self.rate)
        self._last = now

    @property
    def tokens(self):
        self._refill()
        return self._tokens

    def try_acquire(self, n=1):
        if n <= 0 or n > self.capacity:
            raise ValueError("invalid token count")
        self._refill()
        if self._tokens >= n:
            self._tokens -= n
            return True
        return False

    def time_until(self, n=1):
        if n <= 0 or n > self.capacity:
            raise ValueError("invalid token count")
        self._refill()
        missing = n - self._tokens
        return 0.0 if missing <= 0 else missing / self.rate


# --------------------------------------------------------------------------
# cache
# --------------------------------------------------------------------------
class TTLCache:
    """LRU cache with time-to-live, driven by an injectable clock.

    * An entry stored at time t with ttl T is expired at every time >= t + T
      (the boundary itself is already expired).  ``ttl=None`` never expires.
    * ``put(key, value, ttl=None)``: ``ttl=None`` means "use the cache default",
      ``ttl=0`` means "expires immediately" (it can never be read back).
      Putting resets the TTL and makes the key the most recently used one.
      Replacing an existing key NEVER evicts anything.  Inserting a NEW key
      into a full cache first drops all expired entries and only then evicts
      least recently used live entries until there is room.
    * ``get(key, default=None)``: expired entries are removed and ``default`` is
      returned.  A successful get makes the key most recently used but does NOT
      extend its TTL.
    * ``len()``, ``in`` and ``keys()`` only ever see live (non-expired)
      entries.  ``keys()`` lists them from least to most recently used.
      ``in`` does not change recency.
    """

    def __init__(self, capacity, ttl=None, clock=None):
        if capacity < 1:
            raise ValueError("capacity must be >= 1")
        self.capacity = capacity
        self.ttl = ttl
        self.clock = clock if clock is not None else Clock()
        self._data = OrderedDict()  # key -> (value, expires_at or None)

    def _expired(self, entry):
        exp = entry[1]
        return exp is not None and self.clock.now() >= exp

    def _purge(self):
        now = self.clock.now()
        expired = [key for key, entry in self._data.items()
                   if entry[1] is not None and now >= entry[1]]
        for key in expired:
            del self._data[key]

    def get(self, key, default=None):
        entry = self._data.get(key)
        if entry is None:
            return default
        if self._expired(entry):
            del self._data[key]
            return default
        self._data.move_to_end(key)
        return entry[0]

    def put(self, key, value, ttl=None):
        effective_ttl = self.ttl if ttl is None else ttl
        expires_at = (None if effective_ttl is None
                      else self.clock.now() + effective_ttl)

        if key in self._data:
            if not self._expired(self._data[key]):
                self._data[key] = (value, expires_at)
                self._data.move_to_end(key)
                return
            # An expired entry is no longer a live existing key.
            del self._data[key]

        self._purge()
        while len(self._data) >= self.capacity:
            self._data.popitem(last=False)
        self._data[key] = (value, expires_at)

    def __len__(self):
        self._purge()
        return len(self._data)

    def __contains__(self, key):
        self._purge()
        return key in self._data

    def keys(self):
        self._purge()
        return list(self._data)


# --------------------------------------------------------------------------
# event bus
# --------------------------------------------------------------------------
class _Sub:
    __slots__ = ("token", "topic", "handler", "priority", "once", "active")

    def __init__(self, token, topic, handler, priority, once):
        self.token = token
        self.topic = topic
        self.handler = handler
        self.priority = priority
        self.once = once
        self.active = True


class EventBus:
    """Synchronous publish/subscribe.

    * ``subscribe(pattern, handler, priority=0, once=False)`` returns an int
      token.  A pattern is an exact topic, ``"*"`` (every topic) or
      ``"prefix.*"`` (every topic starting with ``"prefix."`` - so ``"job.*"``
      matches ``"job.started"`` and ``"job.a.b"`` but neither ``"job"`` nor
      ``"jobs.started"``).
    * ``emit(topic, payload=None)`` calls ``handler(topic, payload)`` for all
      matching subscriptions, exact and wildcard ones merged into ONE sequence
      ordered by priority (higher first); equal priorities run in subscription
      order.
    * The set of handlers is decided when emit starts, with two exceptions:
      a handler unsubscribed by an earlier handler of the same emit is NOT
      called any more; handlers subscribed during an emit are first called by
      the NEXT emit.
    * A ``once`` subscription is removed *before* its handler is invoked, so a
      handler that re-emits the same topic is not invoked a second time.
    * An exception raised by a handler never stops the dispatch.  ``emit``
      returns the list of exceptions raised (empty list if none).
    * ``unsubscribe(token)`` returns True if something was removed.
    """

    def __init__(self):
        self._subs = []
        self._next = 1

    def subscribe(self, topic, handler, priority=0, once=False):
        sub = _Sub(self._next, topic, handler, priority, once)
        self._next += 1
        self._subs.append(sub)
        return sub.token

    def unsubscribe(self, token):
        for sub in self._subs:
            if sub.token == token:
                sub.active = False
                self._subs.remove(sub)
                return True
        return False

    @staticmethod
    def _matches(pattern, topic):
        if pattern == "*":
            return True
        if pattern.endswith(".*"):
            return topic.startswith(pattern[:-1])
        return pattern == topic

    def emit(self, topic, payload=None):
        # This snapshot fixes membership for this dispatch.  The active flag
        # still lets an earlier callback cancel a later callback in the list.
        matching = [sub for sub in self._subs
                    if self._matches(sub.topic, topic)]
        matching.sort(key=lambda sub: (-sub.priority, sub.token))
        errors = []
        for sub in matching:
            if not sub.active:
                continue
            if sub.once:
                # Remove before calling: recursive emit must not see this sub.
                sub.active = False
                self._subs.remove(sub)
            try:
                sub.handler(topic, payload)
            except Exception as exc:  # noqa: BLE001
                errors.append(exc)
        return errors


# --------------------------------------------------------------------------
# dependency graph
# --------------------------------------------------------------------------
class DAG:
    """Dependency graph.  ``add(node, deps)`` means *node needs deps*.

    * Nodes may be added in any order; ``deps`` may name nodes that are added
      later.  Adding a node twice raises DuplicateNodeError.
    * Duplicate names inside ``deps`` collapse into one (first occurrence
      wins); ``deps(node)`` returns the de-duplicated tuple.  A node that
      depends on itself is a one-node cycle.
    * ``validate()``: raises MissingDependencyError for an unknown dependency
      (checked first, in insertion order), otherwise CycleError for a cycle.
      It must work for graphs with tens of thousands of nodes (no recursion).
    * ``order(targets=None)``: validates the whole graph, then returns a
      topological order of the targets and everything they transitively need
      (all nodes when targets is None).  Among the nodes that are ready at some
      moment the one that was ADDED EARLIEST goes first.  An unknown target
      raises KeyError.
    * ``levels()``: a node's level is 0 without dependencies, otherwise
      1 + the maximum level of its dependencies.  Returns a list of lists, one
      per level, each in insertion order.
    * ``descendants(node)``: set of all nodes that transitively depend on node
      (not including node itself).
    """

    def __init__(self):
        self._deps = {}
        self._index = {}

    def add(self, node, deps=()):
        if node in self._deps:
            raise DuplicateNodeError(node)
        unique_deps = []
        seen = set()
        for dep in deps:
            if dep not in seen:
                seen.add(dep)
                unique_deps.append(dep)
        self._deps[node] = tuple(unique_deps)
        self._index[node] = len(self._index)

    def __contains__(self, node):
        return node in self._deps

    def __len__(self):
        return len(self._deps)

    def nodes(self):
        return list(self._deps)

    def deps(self, node):
        return self._deps[node]

    def validate(self):
        # Missing dependencies take precedence over every cycle, and are
        # reported by the first owner/dependency pair in insertion order.
        for node, deps in self._deps.items():
            for dep in deps:
                if dep not in self._deps:
                    raise MissingDependencyError(node, dep)

        # Iterative depth-first search.  path_pos maps active nodes to their
        # position in path, allowing CycleError to contain only the cycle.
        state = {}  # absent/0 = unseen, 1 = active, 2 = complete
        for root in self._deps:
            if state.get(root, 0) == 2:
                continue
            path = [root]
            path_pos = {root: 0}
            state[root] = 1
            stack = [(root, 0)]

            while stack:
                node, dep_index = stack[-1]
                deps = self._deps[node]
                if dep_index >= len(deps):
                    stack.pop()
                    path.pop()
                    path_pos.pop(node)
                    state[node] = 2
                    continue

                dep = deps[dep_index]
                stack[-1] = (node, dep_index + 1)
                dep_state = state.get(dep, 0)
                if dep_state == 0:
                    state[dep] = 1
                    path_pos[dep] = len(path)
                    path.append(dep)
                    stack.append((dep, 0))
                elif dep_state == 1:
                    cycle_start = path_pos[dep]
                    raise CycleError(path[cycle_start:] + [dep])

    def _closure(self, targets):
        wanted = set()
        stack = list(targets)
        while stack:
            node = stack.pop()
            if node not in self._deps:
                raise KeyError(node)
            if node in wanted:
                continue
            wanted.add(node)
            stack.extend(self._deps[node])
        return wanted

    def order(self, targets=None):
        self.validate()
        wanted = set(self._deps) if targets is None else self._closure(targets)

        indegree = {node: len(self._deps[node]) for node in wanted}
        reverse = {node: [] for node in wanted}
        for node in wanted:
            for dep in self._deps[node]:
                reverse[dep].append(node)

        # Indices are unique, so node names never need to be comparable.
        ready = [(self._index[node], node)
                 for node in wanted if indegree[node] == 0]
        heapq.heapify(ready)
        result = []
        while ready:
            _, node = heapq.heappop(ready)
            result.append(node)
            for dependant in reverse[node]:
                indegree[dependant] -= 1
                if indegree[dependant] == 0:
                    heapq.heappush(ready, (self._index[dependant], dependant))
        return result

    def levels(self):
        ordered = self.order()
        level = {}
        for node in ordered:
            deps = self._deps[node]
            level[node] = 0 if not deps else 1 + max(level[dep] for dep in deps)
        if not level:
            return []
        result = [[] for _ in range(max(level.values()) + 1)]
        for node in self._deps:
            result[level[node]].append(node)
        return result

    def descendants(self, node):
        if node not in self._deps:
            raise KeyError(node)
        reverse = {item: [] for item in self._deps}
        for dependant, deps in self._deps.items():
            for dep in deps:
                if dep in reverse:
                    reverse[dep].append(dependant)

        seen = set()
        stack = list(reverse[node])
        while stack:
            current = stack.pop()
            if current in seen:
                continue
            seen.add(current)
            stack.extend(reverse[current])
        seen.discard(node)
        return seen


# --------------------------------------------------------------------------
# retry
# --------------------------------------------------------------------------
class RetryPolicy:
    """``max_attempts`` is the TOTAL number of attempts (1 = never retry).

    * ``delay(attempt)``: pause after the ``attempt``-th failed attempt
      (1-based): ``min(max_delay, base_delay * factor ** (attempt - 1))``.
      Must never overflow, whatever the attempt number is.
    * ``should_retry(exc, attempt)``: True iff attempts are left
      (``attempt < max_attempts``), ``exc`` is an instance of ``retry_on`` and
      NOT an instance of ``no_retry_on`` (the latter wins).
    """

    def __init__(self, max_attempts=3, base_delay=1.0, factor=2.0, max_delay=30.0,
                 retry_on=(Exception,), no_retry_on=()):
        if max_attempts < 1:
            raise ValueError("max_attempts must be >= 1")
        self.max_attempts = max_attempts
        self.base_delay = base_delay
        self.factor = factor
        self.max_delay = max_delay
        self.retry_on = tuple(retry_on)
        self.no_retry_on = tuple(no_retry_on)

    def delay(self, attempt):
        exponent = attempt - 1
        base = float(self.base_delay)
        factor = float(self.factor)
        limit = float(self.max_delay)
        if exponent <= 0:
            try:
                return min(limit, base * factor ** exponent)
            except OverflowError:
                return limit

        # Exponentiation by squaring is O(log(attempt)); unlike ``factor **
        # exponent`` it remains bounded in work for arbitrarily large integers.
        # Floating-point multiplication saturates to +/-inf rather than raising
        # OverflowError, after which min() applies the documented cap.
        multiplier = 1.0
        power = factor
        remaining = exponent
        while remaining:
            if remaining & 1:
                multiplier *= power
            remaining >>= 1
            if remaining:
                power *= power
        return min(limit, base * multiplier)

    def should_retry(self, exc, attempt):
        return (
            attempt < self.max_attempts
            and isinstance(exc, self.retry_on)
            and not isinstance(exc, self.no_retry_on)
        )


# --------------------------------------------------------------------------
# transactional store
# --------------------------------------------------------------------------
_TOMB = object()


class Store:
    """Key/value store with nested transactions.

    * ``get/set/delete/has/keys/snapshot``.  ``keys()`` is sorted.  ``delete``
      returns True if the key existed.  ``snapshot()`` is a plain dict.
    * Values are copied on the way in and on the way out: later mutation of an
      object that was passed to ``set`` or returned by ``get`` never changes
      the store.
    * ``begin()`` opens a (possibly nested) transaction.  Inside it reads see
      the transaction's own writes and deletions on top of the parent state.
    * ``commit()`` merges the innermost transaction into its PARENT.  Only when
      the outermost transaction commits do changes become permanent; if the
      outer one is rolled back, committed inner work is discarded too.
    * ``rollback()`` discards the innermost transaction completely.
    * ``commit``/``rollback`` without an open transaction raise TransactionError.
    * ``transaction()`` is a context manager: commit on normal exit, rollback
      on ANY exception including BaseException subclasses (the exception is
      re-raised).  ``depth`` is the number of open transactions.
    """

    def __init__(self):
        self._base = {}
        self._stack = []

    @property
    def depth(self):
        return len(self._stack)

    def _lookup(self, key):
        for layer in reversed(self._stack):
            if key in layer:
                return layer[key]
        return self._base.get(key, _TOMB)

    def get(self, key, default=None):
        value = self._lookup(key)
        return default if value is _TOMB else copy.deepcopy(value)

    def has(self, key):
        return self._lookup(key) is not _TOMB

    def set(self, key, value):
        stored = copy.deepcopy(value)
        (self._stack[-1] if self._stack else self._base)[key] = stored

    def delete(self, key):
        existed = self.has(key)
        if self._stack:
            self._stack[-1][key] = _TOMB
        else:
            self._base.pop(key, None)
        return existed

    def _merged(self):
        merged = dict(self._base)
        for layer in self._stack:
            for key, value in layer.items():
                if value is _TOMB:
                    merged.pop(key, None)
                else:
                    merged[key] = value
        return merged

    def keys(self):
        return sorted(self._merged())

    def snapshot(self):
        return copy.deepcopy(self._merged())

    def begin(self):
        self._stack.append({})

    def commit(self):
        if not self._stack:
            raise TransactionError("no open transaction")
        layer = self._stack.pop()
        parent = self._stack[-1] if self._stack else self._base
        parent.update(layer)

    def rollback(self):
        if not self._stack:
            raise TransactionError("no open transaction")
        self._stack.pop()

    @contextmanager
    def transaction(self):
        self.begin()
        try:
            yield self
        except BaseException:
            self.rollback()
            raise
        else:
            self.commit()


# --------------------------------------------------------------------------
# serialization with schema migrations
# --------------------------------------------------------------------------
CURRENT_VERSION = 3


def _encode(value):
    if isinstance(value, tuple):
        return {"__tuple__": [_encode(item) for item in value]}
    if isinstance(value, (set, frozenset)):
        encoded = [_encode(item) for item in value]
        encoded.sort(key=lambda item: json.dumps(
            item, sort_keys=True, separators=(",", ":")))
        return {"__set__": encoded}
    if isinstance(value, list):
        return [_encode(item) for item in value]
    if isinstance(value, dict):
        for key in value:
            if not isinstance(key, str):
                raise TypeError("dictionary keys must be str")
        encoded = {key: _encode(item) for key, item in value.items()}
        # Escape user dictionaries that would otherwise be mistaken for one
        # of the type tags by _decode.  The escape tag itself is also escaped
        # when it occurs as the sole user key.
        if len(encoded) == 1 and next(iter(encoded)) in {
                "__tuple__", "__set__", "__dict__"}:
            key, item = next(iter(encoded.items()))
            return {"__dict__": [[key, item]]}
        return encoded
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    raise TypeError(f"cannot serialize {type(value).__name__}")


def _decode(value):
    if isinstance(value, list):
        return [_decode(item) for item in value]
    if isinstance(value, dict):
        if set(value) == {"__dict__"}:
            return {key: _decode(item) for key, item in value["__dict__"]}
        if set(value) == {"__tuple__"}:
            return tuple(_decode(item) for item in value["__tuple__"])
        if set(value) == {"__set__"}:
            return {_decode(item) for item in value["__set__"]}
        return {key: _decode(item) for key, item in value.items()}
    return value


def serialize(data, meta=None):
    """Return a canonical JSON text (sorted keys, no whitespace) of version 3:
    ``{"version": 3, "data": ..., "meta": ...}``.  Tuples and sets survive a
    round trip at ANY nesting depth (frozensets come back as sets).  Dict keys
    must be str - anything else raises TypeError instead of being silently
    coerced; unsupported value types raise TypeError too.  Equal inputs give
    byte-identical output, whatever the insertion order and element types."""
    doc = {
        "version": CURRENT_VERSION,
        "data": _encode(data),
        "meta": _encode({} if meta is None else meta),
    }
    return json.dumps(doc, sort_keys=True, separators=(",", ":"))


def _v1_to_v2(doc):
    data = {}
    for key, value in doc.get("items", []):
        data[key] = value  # a later duplicate wins
    meta = dict(doc.get("meta", {}))
    meta["migrated_from"] = 1
    return {"version": 2, "data": data, "meta": meta}


def _v2_to_v3(doc):
    # Explicitly namespaced keys always win over their default namespace form,
    # independently of the insertion order in the source document.
    data = {key: value for key, value in doc["data"].items() if "/" in key}
    for key, value in doc["data"].items():
        if "/" not in key:
            data.setdefault("default/" + key, value)
    meta = dict(doc.get("meta", {}))
    meta["migrated_from"] = doc["version"]
    return {"version": 3, "data": data, "meta": meta}


_MIGRATIONS = {1: _v1_to_v2, 2: _v2_to_v3}


def deserialize(text):
    """Parse a document of any known version and return ``(data, meta)`` in the
    CURRENT format.

    * a document without a ``version`` field is version 1;
    * v1: ``{"items": [[key, value], ...]}`` (later duplicates win);
    * v2: ``{"version": 2, "data": {...}, "meta": {...}}`` without namespaces;
    * v3: current.
    * Older documents are migrated step by step (1 -> 2 -> 3).  When a
      migration happened ``meta["migrated_from"]`` is the ORIGINAL version.
    * a version that is not an int in 1..3 (bool does not count) raises
      UnsupportedVersion.
    """
    doc = json.loads(text)
    if not isinstance(doc, dict):
        raise ValueError("document must be an object")

    version = doc.get("version", 1)
    if type(version) is not int or not 1 <= version <= CURRENT_VERSION:
        raise UnsupportedVersion(version)
    original_version = version

    while version < CURRENT_VERSION:
        doc = _MIGRATIONS[version](doc)
        version = doc["version"]

    meta = dict(doc.get("meta", {}))
    if original_version != CURRENT_VERSION:
        meta["migrated_from"] = original_version
    return _decode(doc["data"]), _decode(meta)


# --------------------------------------------------------------------------
# engine
# --------------------------------------------------------------------------
_MISSING = object()
_GOOD = ("ok", "cached")


def _cache_normalize(value):
    """Build a deterministic, JSON-compatible representation for cache keys."""
    if isinstance(value, dict):
        pairs = [[_cache_normalize(key), _cache_normalize(item)]
                 for key, item in value.items()]
        pairs.sort(key=lambda pair: json.dumps(
            pair, sort_keys=True, separators=(",", ":"), default=repr))
        return {"$dict": pairs}
    if isinstance(value, list):
        return {"$list": [_cache_normalize(item) for item in value]}
    if isinstance(value, tuple):
        return {"$tuple": [_cache_normalize(item) for item in value]}
    if isinstance(value, (set, frozenset)):
        items = [_cache_normalize(item) for item in value]
        items.sort(key=lambda item: json.dumps(
            item, sort_keys=True, separators=(",", ":"), default=repr))
        return {"$set": items}
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    return {"$type": type(value).__qualname__, "$repr": repr(value)}


class TaskContext:
    """What a task function receives: ``name``, ``inputs`` (dependency name ->
    result), ``params``, ``store``, ``attempt`` (1-based), ``clock`` and
    ``cancel()``."""

    def __init__(self, name, inputs, params, store, attempt, clock, engine):
        self.name = name
        self.inputs = inputs
        self.params = params
        self.store = store
        self.attempt = attempt
        self.clock = clock
        self._engine = engine

    def cancel(self):
        self._engine.cancel()


@dataclass
class RunReport:
    """status values: ``ok``, ``cached``, ``failed``, ``skipped``, ``cancelled``.
    ``results`` only holds ok/cached tasks, ``errors`` only failed ones,
    ``attempts`` has an entry for every task (0 if the function never ran),
    ``order`` lists every task in the order it was processed and ``elapsed`` is
    the clock time the whole run took."""

    results: dict = field(default_factory=dict)
    status: dict = field(default_factory=dict)
    errors: dict = field(default_factory=dict)
    attempts: dict = field(default_factory=dict)
    order: list = field(default_factory=list)
    elapsed: float = 0.0

    @property
    def ok(self):
        return all(status in _GOOD for status in self.status.values())


class _Task:
    __slots__ = ("name", "func", "retry", "timeout", "cacheable", "version")

    def __init__(self, name, func, retry, timeout, cacheable, version):
        self.name = name
        self.func = func
        self.retry = retry
        self.timeout = timeout
        self.cacheable = cacheable
        self.version = version


class Engine:
    """Runs a graph of tasks.

    ``add_task(name, func, deps=(), retry=None, timeout=None, cacheable=False,
    version="1")``; ``retry=None`` means one single attempt.  ``func(ctx)``
    returns the task's result.

    ``run(targets=None, params=None)`` processes the targets and everything
    they need, in ``DAG.order`` order, and returns a RunReport.

    * The graph is validated BEFORE anything runs: CycleError /
      MissingDependencyError leave no side effects and emit no events.
    * A task whose dependency is not ok/cached is ``skipped`` - transitively,
      its function is never called.
    * Results are deep-copied when a task returns, when they are handed to
      dependants (``ctx.inputs``), and when they enter / leave the cache, so no
      consumer can ever corrupt anybody else's data.
    * Each ATTEMPT runs inside a store transaction: it is committed if the
      function returns in time and rolled back on any exception.
    * ``timeout``: an attempt whose clock time (end - start) is strictly greater
      than ``timeout`` fails with TaskTimeout and its store changes are rolled
      back; it can be retried like any other failure.
    * Retries: after a failed attempt, if ``retry.should_retry``, the engine
      emits ``task.retry`` and sleeps ``retry.delay(attempt)``; otherwise the
      task is ``failed`` (no sleeping at the end) and ``errors[name]`` is the
      last exception.
    * ``cacheable`` tasks are looked up in the cache by a key derived from the
      task name, its ``version``, its (deep) inputs and the run ``params``;
      key order inside dicts must not matter.  A hit gives status ``cached``
      with 0 attempts.  Only successful attempts are cached.
    * ``rate_limiter`` (a TokenBucket): every real ATTEMPT takes one token,
      waiting on the clock if needed (``clock.sleep(limiter.time_until(1))``).
      Cache hits and skipped tasks take none.
    * ``cancel()`` (e.g. from ``ctx.cancel()``) affects the current run only:
      the running task finishes normally, all tasks not processed yet become
      ``cancelled``.  The request is forgotten when the next run starts, and a
      ``cancel()`` while nothing is running has no effect.
    * a custom ``cache`` / ``bus`` / ``store`` / ``rate_limiter`` passed to the
      constructor is the one that gets used, even if it is empty.

    Events (``bus.emit(topic, payload)``): ``run.started {tasks}``,
    ``task.started {name, attempt}``, ``task.retry {name, attempt, delay,
    error}``, ``task.succeeded {name, attempt}``, ``task.failed {name, attempt,
    error}``, ``task.cached {name}``, ``task.skipped {name, blocked_by}``,
    ``task.cancelled {name}``, ``run.finished {ok, elapsed}``.
    """

    def __init__(self, clock=None, bus=None, cache=None, store=None, rate_limiter=None):
        self.clock = clock if clock is not None else Clock()
        self.bus = bus if bus is not None else EventBus()
        self.cache = cache if cache is not None else TTLCache(1024, None, self.clock)
        self.store = store if store is not None else Store()
        self.rate_limiter = rate_limiter
        self.dag = DAG()
        self._tasks = {}
        self._cancel_requested = False
        self._run_stack = []

    # -- registration ------------------------------------------------------
    def add_task(self, name, func, deps=(), retry=None, timeout=None,
                 cacheable=False, version="1"):
        self.dag.add(name, deps)
        self._tasks[name] = _Task(
            name, func,
            retry if retry is not None else RetryPolicy(max_attempts=1),
            timeout, cacheable, version,
        )

    def cancel(self):
        # Cancellation outside a run is deliberately a no-op.  A stack also
        # makes nested run() calls isolate their cancellation requests.
        if self._run_stack:
            self._run_stack[-1]["cancel_requested"] = True
            self._cancel_requested = True

    # -- running -----------------------------------------------------------
    def run(self, targets=None, params=None):
        # DAG.order validates the WHOLE graph before params are copied, events
        # are emitted, or any other externally visible work is performed.
        order = self.dag.order(targets)
        run_params = {} if params is None else copy.deepcopy(params)

        state = {"cancel_requested": False}
        self._run_stack.append(state)
        self._cancel_requested = False
        report = RunReport()
        try:
            started = self.clock.now()
            self.bus.emit("run.started", {"tasks": list(order)})
            for name in order:
                report.order.append(name)
                if state["cancel_requested"]:
                    report.status[name] = "cancelled"
                    report.attempts[name] = 0
                    self.bus.emit("task.cancelled", {"name": name})
                    continue

                deps = self.dag.deps(name)
                blocked = [dep for dep in deps
                           if report.status.get(dep) not in _GOOD]
                if blocked:
                    report.status[name] = "skipped"
                    report.attempts[name] = 0
                    self.bus.emit("task.skipped", {
                        "name": name, "blocked_by": blocked,
                    })
                    continue

                # Each dependant receives its own copy, never the producer's
                # report value or another consumer's copy.
                inputs = {dep: copy.deepcopy(report.results[dep]) for dep in deps}
                self._execute(name, inputs, run_params, report)

            report.elapsed = self.clock.now() - started
            self.bus.emit("run.finished", {
                "ok": report.ok, "elapsed": report.elapsed,
            })
            return report
        finally:
            self._run_stack.pop()
            self._cancel_requested = (
                self._run_stack[-1]["cancel_requested"] if self._run_stack else False
            )

    def _cache_key(self, task, inputs, params):
        payload = {
            "task": _cache_normalize(task.name),
            "version": _cache_normalize(task.version),
            "inputs": _cache_normalize(inputs),
            "params": _cache_normalize(params),
        }
        blob = json.dumps(payload, sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(blob.encode("utf-8")).hexdigest()

    def _acquire_token(self):
        limiter = self.rate_limiter
        if limiter is None:
            return
        while not limiter.try_acquire(1):
            self.clock.sleep(limiter.time_until(1))

    def _execute(self, name, inputs, params, report):
        task = self._tasks[name]
        key = None
        if task.cacheable:
            key = self._cache_key(task, inputs, params)
            cached = self.cache.get(key, _MISSING)
            if cached is not _MISSING:
                report.results[name] = copy.deepcopy(cached)
                report.status[name] = "cached"
                report.attempts[name] = 0
                self.bus.emit("task.cached", {"name": name})
                return

        attempt = 0
        while True:
            self._acquire_token()
            attempt += 1
            self.bus.emit("task.started", {"name": name, "attempt": attempt})
            began = self.clock.now()
            ctx = TaskContext(
                name,
                copy.deepcopy(inputs),
                copy.deepcopy(params),
                self.store,
                attempt,
                self.clock,
                self,
            )
            try:
                with self.store.transaction():
                    value = task.func(ctx)
                    if (task.timeout is not None
                            and self.clock.now() - began > task.timeout):
                        raise TaskTimeout(f"{name} exceeded {task.timeout}")
                    # Copy before leaving the transaction so a copy failure is
                    # also an attempt failure and rolls back store writes.
                    value = copy.deepcopy(value)
            except BaseException as exc:  # noqa: BLE001
                # A task that has crossed its timeout is a timeout failure even
                # if it also raised another exception before returning.
                if (task.timeout is not None
                        and self.clock.now() - began > task.timeout):
                    exc = TaskTimeout(f"{name} exceeded {task.timeout}")
                if task.retry.should_retry(exc, attempt):
                    delay = task.retry.delay(attempt)
                    self.bus.emit("task.retry", {
                        "name": name,
                        "attempt": attempt,
                        "delay": delay,
                        "error": exc,
                    })
                    self.clock.sleep(delay)
                    continue

                report.status[name] = "failed"
                report.errors[name] = exc
                report.attempts[name] = attempt
                self.bus.emit("task.failed", {
                    "name": name, "attempt": attempt, "error": exc,
                })
                return

            report.results[name] = value
            report.status[name] = "ok"
            report.attempts[name] = attempt
            if key is not None:
                # The cache owns a distinct copy; later mutation of the report
                # or a dependant's inputs cannot alter the cached result.
                self.cache.put(key, copy.deepcopy(value))
            self.bus.emit("task.succeeded", {"name": name, "attempt": attempt})
            return
