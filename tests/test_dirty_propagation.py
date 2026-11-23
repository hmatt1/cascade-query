"""Dirty propagation: memos unaffected by an input change stay valid without a
dependency walk, and every way a memo can silently go stale is covered."""

from __future__ import annotations

import random
import threading
from collections import Counter
from typing import Any

import pytest

from cascade import Engine


def _store(engine: Engine) -> Any:
    return engine._internals._store  # noqa: SLF001


def assert_clean_invariants(engine: Engine) -> None:
    """A clean memo's query deps are clean and reachable through ``dependents``."""
    store = _store(engine)
    with store.lock:
        for key, memo in store.memos.items():
            if not memo.head_clean:
                continue
            assert not memo.cycle_nodes
            for dep in memo.deps:
                assert key in store.dependents.get(dep.key, ()), (key, dep.key)
                if dep.key[0] == "query":
                    dep_memo = store.memos.get(dep.key)
                    assert dep_memo is not None and dep_memo.head_clean, (key, dep.key)


def _events(engine: Engine) -> Counter[str]:
    return Counter(ev.event for ev in engine.traces())


def _chain(engine: Engine, runs: Counter[str]) -> tuple[Any, Any, Any, Any]:
    @engine.input
    def a() -> int:
        return 1

    @engine.input
    def b() -> int:
        return 10

    @engine.query
    def mid() -> int:
        runs["mid"] += 1
        return a() * 2

    @engine.query
    def top() -> int:
        runs["top"] += 1
        return mid() + 1

    return a, b, mid, top


def test_unrelated_input_change_needs_no_walk_or_recompute() -> None:
    engine = Engine()
    runs: Counter[str] = Counter()
    a, b, _, top = _chain(engine, runs)
    a.set(1)
    b.set(10)
    assert top() == 3
    runs.clear()
    engine.clear_traces()

    b.set(11)
    assert top() == 3
    assert runs == {}
    events = _events(engine)
    assert events["cache_hit"] == 1
    assert events["cache_green"] == 0 and events["recompute_start"] == 0
    assert_clean_invariants(engine)


def test_same_value_set_keeps_everything_clean() -> None:
    engine = Engine()
    runs: Counter[str] = Counter()
    a, _, _, top = _chain(engine, runs)
    a.set(1)
    assert top() == 3
    runs.clear()
    engine.clear_traces()

    a.set(1)
    assert top() == 3
    assert runs == {}
    assert _events(engine)["cache_green"] == 0


def test_related_input_change_recomputes_the_cone() -> None:
    engine = Engine()
    runs: Counter[str] = Counter()
    a, _, _, top = _chain(engine, runs)
    a.set(1)
    assert top() == 3
    runs.clear()

    a.set(5)
    assert top() == 11
    assert runs == {"mid": 1, "top": 1}
    assert_clean_invariants(engine)


def test_backdated_middle_result_stops_recompute_but_stays_correct() -> None:
    engine = Engine()
    runs: Counter[str] = Counter()

    @engine.input
    def n() -> int:
        return 0

    @engine.query
    def parity() -> int:
        runs["parity"] += 1
        return n() % 2

    @engine.query
    def label() -> str:
        runs["label"] += 1
        return "odd" if parity() else "even"

    n.set(2)
    assert label() == "even"
    runs.clear()
    n.set(4)
    assert label() == "even"
    assert runs == {"parity": 1}
    n.set(5)
    assert label() == "odd"
    assert runs == {"parity": 2, "label": 1}
    assert_clean_invariants(engine)


def test_dynamic_dependencies_follow_the_branch_actually_read() -> None:
    engine = Engine()
    runs: Counter[str] = Counter()

    @engine.input
    def flag() -> bool:
        return True

    @engine.input
    def left() -> int:
        return 1

    @engine.input
    def right() -> int:
        return 2

    @engine.query
    def pick() -> int:
        runs["pick"] += 1
        return left() if flag() else right()

    @engine.query
    def out() -> int:
        runs["out"] += 1
        return pick() * 10

    flag.set(True)
    left.set(1)
    right.set(2)
    assert out() == 10
    runs.clear()

    right.set(20)  # not read while flag is True
    assert out() == 10 and runs == {}
    flag.set(False)
    assert out() == 200
    left.set(100)  # no longer read
    runs.clear()
    assert out() == 200 and runs == {}
    right.set(3)
    assert out() == 30
    assert_clean_invariants(engine)


def test_dropping_a_middle_memo_dirties_its_dependents() -> None:
    engine = Engine()
    runs: Counter[str] = Counter()
    a, _, mid, top = _chain(engine, runs)
    a.set(1)
    assert top() == 3
    store = _store(engine)
    top_key = ("query", top.id, ())
    assert store.memos[top_key].head_clean

    # With ``mid`` gone, the edge from input ``a`` no longer leads to ``top``;
    # ``top`` must already be dirty or the next change to ``a`` would be missed.
    with store.lock:
        store.drop_memo_locked(("query", mid.id, ()))
    assert not store.memos[top_key].head_clean
    a.set(4)
    assert top() == 9
    assert_clean_invariants(engine)


def test_lru_eviction_never_serves_stale_results() -> None:
    engine = Engine(max_entries=3)

    @engine.input
    def src(i: int) -> int:
        return 0

    @engine.query
    def leaf(i: int) -> int:
        return src(i) + 1

    @engine.query
    def total() -> int:
        return sum(leaf(i) for i in range(6))

    values = [0] * 6
    for i in range(6):
        src.set(i, 0)
    assert total() == 6
    rng = random.Random(3)
    for step in range(60):
        i = rng.randrange(6)
        values[i] = step
        src.set(i, step)
        assert total() == sum(values) + 6
        assert_clean_invariants(engine)


def test_pinned_snapshot_and_head_reads_interleave_correctly() -> None:
    engine = Engine()
    runs: Counter[str] = Counter()
    a, _, _, top = _chain(engine, runs)
    a.set(1)
    assert top() == 3
    old = engine.snapshot()
    a.set(5)
    for _ in range(3):
        assert top() == 11
        assert top(snapshot=old) == 3
        assert_clean_invariants(engine)
    a.set(6)
    assert top(snapshot=old) == 3
    assert top() == 13


def test_old_snapshot_recompute_with_different_deps_dirties_head_readers() -> None:
    engine = Engine()

    @engine.input
    def flag() -> bool:
        return True

    @engine.input
    def left() -> int:
        return 1

    @engine.input
    def right() -> int:
        return 2

    @engine.query
    def pick() -> int:
        return left() if flag() else right()

    @engine.query
    def out() -> int:
        return pick() * 10

    flag.set(False)
    left.set(1)
    right.set(2)
    old = engine.snapshot()  # here ``pick`` reads ``right``
    flag.set(True)
    assert out() == 10  # at head ``pick`` reads ``left``

    # Recomputing at the old snapshot replaces the memo for ``pick`` with one
    # whose edges point at ``right``, not ``left``.
    assert out(snapshot=old) == 20
    left.set(7)
    assert out() == 70
    assert out(snapshot=old) == 20
    assert_clean_invariants(engine)


def test_ttl_dependency_keeps_dependents_on_the_slow_path() -> None:
    clock = {"now": 1000.0}
    engine = Engine()
    runs: Counter[str] = Counter()

    @engine.input
    def tick() -> int:
        return 0

    @engine.query(ttl=10.0)
    def volatile() -> float:
        runs["volatile"] += 1
        return clock["now"]

    @engine.query
    def reader() -> float:
        runs["reader"] += 1
        return volatile()

    import cascade._evaluator as evaluator_mod

    real_time = evaluator_mod.time.time
    evaluator_mod.time.time = lambda: clock["now"]  # type: ignore[assignment]
    try:
        assert reader() == 1000.0
        store = _store(engine)
        assert not store.memos[("query", volatile.id, ())].head_clean
        assert not store.memos[("query", reader.id, ())].head_clean

        clock["now"] = 1100.0  # past the TTL
        tick.set(1)  # unrelated revision bump: the walk must still notice expiry
        assert reader() == 1100.0
        assert runs == {"volatile": 2, "reader": 2}
    finally:
        evaluator_mod.time.time = real_time  # type: ignore[assignment]


def test_default_input_materialized_later_is_noticed() -> None:
    engine = Engine()

    @engine.input
    def cfg() -> int:
        return 5

    @engine.query
    def doubled() -> int:
        return cfg() * 2

    assert doubled() == 10  # reads the default
    assert doubled() == 10
    cfg.set(8)
    assert doubled() == 16
    assert_clean_invariants(engine)


def test_reregistering_a_query_dirties_its_dependents() -> None:
    engine = Engine()

    @engine.input
    def a() -> int:
        return 1

    def base_v1() -> int:
        return a() + 1

    def base_v2() -> int:
        return a() + 100

    base_v1.__qualname__ = base_v2.__qualname__ = "base"
    base_v1.__name__ = base_v2.__name__ = "base"
    base = engine.query(base_v1)

    @engine.query
    def top() -> int:
        return base() * 2

    a.set(1)
    assert top() == 4
    # Rebind the name ``top`` reads, as a reload does: the handle from the
    # first registration is stale once another function takes its name.
    base = engine.query(base_v2)
    a.set(2)
    assert top() == 204


def test_fixed_point_cycle_memos_are_never_marked_clean() -> None:
    engine = Engine()

    @engine.input
    def limit() -> int:
        return 3

    @engine.query(fixed_point=0)
    def grow() -> int:
        return min(grow() + 1, limit())

    limit.set(3)
    assert grow() == 3
    store = _store(engine)
    assert not store.memos[("query", grow.id, ())].head_clean
    limit.set(5)
    assert grow() == 5


# --- randomized differential test -------------------------------------------

N_INPUTS = 6
N_NODES = 14


def _graph(seed: int) -> list[tuple[str, int, int, int, int]]:
    """Per node: (shape, selector, when_odd, when_even, modulus).

    Operands index earlier nodes (>= 0) or inputs (encoded as ``-1 - i``).
    """
    rng = random.Random(seed)
    nodes = []
    for j in range(N_NODES):

        def operand() -> int:
            if j == 0 or rng.random() < 0.4:
                return -1 - rng.randrange(N_INPUTS)
            return rng.randrange(j)

        shape = rng.choice(["sum", "branch", "branch"])
        nodes.append((shape, operand(), operand(), operand(), rng.choice([2, 3, 1000])))
    return nodes


def _model(nodes: list[Any], inputs: list[int], j: int) -> int:
    def get(ref: int) -> int:
        return inputs[-1 - ref] if ref < 0 else _model(nodes, inputs, ref)

    shape, sel, odd, even, modulus = nodes[j]
    if shape == "sum":
        return (get(sel) + get(odd)) % modulus
    return (get(odd) if get(sel) % 2 else get(even)) % modulus


@pytest.mark.parametrize("seed", range(8))
@pytest.mark.parametrize("max_entries", [4, 10_000])
def test_random_operations_match_plain_recomputation(
    seed: int, max_entries: int
) -> None:
    nodes = _graph(seed)
    engine = Engine(max_entries=max_entries)

    @engine.input
    def src(i: int) -> int:
        return 0

    @engine.query
    def node(j: int) -> int:
        def get(ref: int) -> int:
            return src(-1 - ref) if ref < 0 else node(ref)

        shape, sel, odd, even, modulus = nodes[j]
        if shape == "sum":
            return (get(sel) + get(odd)) % modulus
        return (get(odd) if get(sel) % 2 else get(even)) % modulus

    rng = random.Random(seed * 7919 + max_entries)
    inputs = [0] * N_INPUTS
    for i in range(N_INPUTS):
        src.set(i, 0)
    snapshots: list[tuple[Any, list[int]]] = []

    for _ in range(300):
        op = rng.random()
        if op < 0.35:
            i = rng.randrange(N_INPUTS)
            # Small value range so same-value sets and backdating are common.
            inputs[i] = rng.randrange(4)
            src.set(i, inputs[i])
        elif op < 0.75:
            j = rng.randrange(N_NODES)
            assert node(j) == _model(nodes, inputs, j)
        elif op < 0.85:
            snapshots.append((engine.snapshot(), list(inputs)))
            del snapshots[:-4]
        elif op < 0.95 and snapshots:
            snap, frozen = rng.choice(snapshots)
            j = rng.randrange(N_NODES)
            assert node(j, snapshot=snap) == _model(nodes, frozen, j)
        else:
            keep = rng.sample(range(N_NODES), 3)
            engine.prune([("query", node.id, (j,)) for j in keep])
        assert_clean_invariants(engine)

    for j in range(N_NODES):
        assert node(j) == _model(nodes, inputs, j)


def test_concurrent_writers_and_readers_only_see_real_states() -> None:
    engine = Engine()

    @engine.input
    def src(i: int) -> int:
        return 0

    @engine.query
    def leaf(i: int) -> int:
        return src(i) * 2

    @engine.query
    def total() -> int:
        return sum(leaf(i) for i in range(4))

    for i in range(4):
        src.set(i, 0)
    stop = threading.Event()
    errors: list[str] = []

    def writer() -> None:
        # Always sets all four inputs to the same value in one revision, so any
        # consistent snapshot sums to a multiple of 8.
        value = 0
        while not stop.is_set():
            value += 1
            with engine.transaction():
                for i in range(4):
                    src.set(i, value)

    def reader() -> None:
        from cascade import QueryCancelled

        while not stop.is_set():
            try:
                result = total()
            except QueryCancelled:
                continue
            if result % 8 != 0:
                errors.append(f"inconsistent total {result}")
                stop.set()

    threads = [threading.Thread(target=writer)] + [
        threading.Thread(target=reader) for _ in range(3)
    ]
    for t in threads:
        t.start()
    stop.wait(timeout=1.0)
    stop.set()
    for t in threads:
        t.join(timeout=10)
    assert errors == []
    final = engine._internals._store.inputs[(src.id, (0,))][-1].value  # noqa: SLF001
    assert total() == final * 8
    assert_clean_invariants(engine)
