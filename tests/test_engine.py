from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from cascade import CycleError, Engine

# Black-box behavior coverage for core query/input semantics.
# Concurrency, compute_many behavior detail, and persistence/trace assertions
# live in dedicated test modules to keep this file focused and navigable.


def test_smart_recalculation_and_selective_updates() -> None:
    engine = Engine()
    calls = {"parse": 0, "names": 0}

    @engine.input
    def source(name: str) -> str:
        return ""

    @engine.query
    def parse(name: str) -> tuple[str, ...]:
        calls["parse"] += 1
        return tuple(line.strip() for line in source(name).splitlines() if line.strip())

    @engine.query
    def names(name: str) -> tuple[str, ...]:
        calls["names"] += 1
        return tuple(item.split("=")[0].strip() for item in parse(name))

    source.set("main", "a = 1\nb = 2")
    assert names("main") == ("a", "b")
    assert calls == {"parse": 1, "names": 1}

    # Demand-driven: querying parse directly does not force names to recompute.
    assert parse("main") == ("a = 1", "b = 2")
    assert calls == {"parse": 1, "names": 1}

    # Whitespace-only change backdates parse result hash, so parent remains green.
    source.set("main", "a = 1  \nb = 2")
    assert names("main") == ("a", "b")
    assert calls["parse"] == 2
    assert calls["names"] == 1


def test_cycle_detection() -> None:
    engine = Engine()

    @engine.query
    def a(x: int) -> int:
        return b(x)

    @engine.query
    def b(x: int) -> int:
        return a(x)

    with pytest.raises(CycleError):
        a(1)


def test_snapshot_isolation_mvcc() -> None:
    engine = Engine()

    @engine.input
    def source(name: str) -> str:
        return ""

    @engine.query
    def parse(name: str) -> tuple[str, ...]:
        return tuple(source(name).splitlines())

    source.set("main", "a\nb")
    snap = engine.snapshot()
    source.set("main", "x\ny")

    assert parse("main", snapshot=snap) == ("a", "b")
    assert parse("main") == ("x", "y")


def test_snapshot_default_input_read_does_not_create_redundant_versions() -> None:
    engine = Engine()

    @engine.input
    def missing(name: str) -> str:
        return f"default:{name}"

    @engine.query
    def q1(name: str) -> str:
        return missing(name) + ":q1"

    @engine.query
    def q2(name: str) -> str:
        return missing(name) + ":q2"

    snap = engine.snapshot()
    assert engine.revision == 0

    # Snapshot reads are read-only: missing defaults are virtual at that snapshot
    # and must not mutate the live input timeline.
    assert q1("x", snapshot=snap) == "default:x:q1"
    assert engine.revision == 0
    input_set_events_before_live_write = [
        event
        for event in engine.traces()
        if event.event == "input_set" and event.key == f"input:{missing.id}('x',)"
    ]
    assert input_set_events_before_live_write == []

    # A second same-snapshot read for the same key also must remain non-mutating.
    assert q2("x", snapshot=snap) == "default:x:q2"
    assert engine.revision == 0
    input_set_events_after_snapshot_reads = [
        event
        for event in engine.traces()
        if event.event == "input_set" and event.key == f"input:{missing.id}('x',)"
    ]
    assert input_set_events_after_snapshot_reads == []

    # Live writes remain authoritative even after stale-snapshot default reads.
    missing.set("x", value="live")
    assert engine.revision == 1
    assert q1("x") == "live:q1"
    assert q1("x", snapshot=snap) == "default:x:q1"
    assert q1("x") == "live:q1"

    input_set_events = [
        event
        for event in engine.traces()
        if event.event == "input_set" and event.key == f"input:{missing.id}('x',)"
    ]
    assert len(input_set_events) == 1
    assert input_set_events[0].revision == 1
    assert input_set_events[0].detail == "changed_at=1"


def test_side_effect_replay_on_cache_hits() -> None:
    engine = Engine()
    warns = engine.accumulator("warnings")

    @engine.input
    def source() -> str:
        return ""

    @engine.query
    def lint() -> int:
        text = source()
        if "todo" in text:
            warns.push("contains todo")
        return len(text)

    source.set("todo item")
    effects_1: dict[str, list[str]] = {}
    assert lint(effects=effects_1) == len("todo item")
    assert effects_1["warnings"] == ["contains todo"]

    effects_2: dict[str, list[str]] = {}
    assert lint(effects=effects_2) == len("todo item")
    assert effects_2["warnings"] == ["contains todo"]


def test_nested_side_effect_replay_propagates_to_parent_effects() -> None:
    engine = Engine()
    warns = engine.accumulator("warnings")
    events = {"child": 0}

    @engine.input
    def source() -> str:
        return ""

    @engine.query
    def child() -> int:
        events["child"] += 1
        text = source()
        if "todo" in text:
            warns.push("child warned")
        return len(text)

    @engine.query
    def parent() -> int:
        return child() + 1

    source.set("todo")
    effects_1: dict[str, list[str]] = {}
    assert parent(effects=effects_1) == len("todo") + 1
    assert effects_1["warnings"] == ["child warned"]
    assert events["child"] == 1

    # Child is cached; parent recomputes due to an independent input dependency
    # while still receiving replayed child effects transitively.
    @engine.input
    def toggle() -> int:
        return 0

    @engine.query
    def parent_with_toggle() -> int:
        _ = toggle()
        return child() + 2

    toggle.set(0)
    effects_2: dict[str, list[str]] = {}
    assert parent_with_toggle(effects=effects_2) == len("todo") + 2
    assert effects_2["warnings"] == ["child warned"]
    assert events["child"] == 1

    toggle.set(1)
    effects_3: dict[str, list[str]] = {}
    assert parent_with_toggle(effects=effects_3) == len("todo") + 2
    assert effects_3["warnings"] == ["child warned"]
    # Still a single concrete child execution; subsequent effects came from replay.
    assert events["child"] == 1


def test_dynamic_graph_expansion() -> None:
    engine = Engine()
    expand_calls = 0

    @engine.input
    def source() -> str:
        return ""

    @engine.query
    def expand() -> tuple[str, ...]:
        nonlocal expand_calls
        expand_calls += 1
        lines = []
        for row in source().splitlines():
            if row.startswith("macro "):
                lines.append(row.replace("macro ", "fn ").strip())
            else:
                lines.append(row.strip())
        return tuple(lines)

    @engine.query
    def lowered() -> tuple[str, ...]:
        return tuple(line.lower() for line in expand())

    source.set("macro Add\nmacro Sub")
    assert lowered() == ("fn add", "fn sub")
    source.set("macro Add\nmacro Mul")
    assert lowered() == ("fn add", "fn mul")
    assert expand_calls >= 2


def test_input_set_supports_keyword_and_positional_value_forms() -> None:
    engine = Engine()

    @engine.input
    def scalar() -> int:
        return 0

    @engine.input
    def keyed(name: str) -> str:
        return ""

    scalar.set(value=11)
    assert scalar() == 11
    scalar.set(13)
    assert scalar() == 13

    keyed.set("alpha", value="A")
    keyed.set("beta", "B")
    assert keyed("alpha") == "A"
    assert keyed("beta") == "B"


def test_long_chain_cycle_detection_behavior() -> None:
    engine = Engine()
    chain_length = 25
    nodes: list = []

    def make_node(index: int):
        def node(x: int) -> int:
            if index == chain_length - 1:
                return nodes[0](x)
            return nodes[index + 1](x)

        node.__name__ = f"node_{index}"
        node.__qualname__ = f"node_{index}"
        return engine.query(node)

    for index in range(chain_length):
        nodes.append(make_node(index))

    with pytest.raises(CycleError) as exc:
        nodes[0](1)

    message = str(exc.value)
    assert "cycle detected:" in message
    assert "node_0" in message
    assert f"node_{chain_length - 1}" in message


def test_misc_api_edges_and_default_input_paths(tmp_path: Path) -> None:
    engine = Engine()
    warnings = engine.accumulator("warnings")

    @engine.input
    def number(x: int) -> int:
        return x * 2

    @engine.query
    def plus_one(x: int) -> int:
        return number(x) + 1

    # Unset input falls back to its default function and initializes a version.
    assert plus_one(3) == 7
    assert engine.revision == 1
    assert number.id in repr(number)
    assert plus_one.id in repr(plus_one)
    assert repr(warnings) == "<Accumulator warnings>"

    with pytest.raises(TypeError):
        number.set()

    with pytest.raises(RuntimeError):
        warnings.push("outside query")

    # Empty dispatch is explicitly supported.
    assert engine.compute_many([]) == []

    # submit() without an external executor takes the owned-executor path.
    assert engine.submit(plus_one, 3).result(timeout=2.0) == 7

    # clear_traces() resets the event buffer.
    assert engine.traces()
    engine.clear_traces()
    assert engine.traces() == []

    # load() gracefully handles an empty persistence table.
    empty_path = tmp_path / "empty.db"
    conn = sqlite3.connect(empty_path)
    try:
        conn.execute("create table if not exists cascade_state (id integer primary key, payload blob not null)")
        conn.commit()
    finally:
        conn.close()
    engine.load(str(empty_path))

    # prune() tolerates unreachable roots and keeps memo graph consistent.
    engine.prune([("query", plus_one.id, (999,))])
    assert isinstance(engine.inspect_graph(), dict)


def test_input_set_with_keyword_value_and_no_positional_value() -> None:
    engine = Engine()

    @engine.input
    def setting() -> int:
        return 0

    @engine.input
    def threshold(name: str) -> int:
        return len(name)

    setting.set(value=7)
    threshold.set("prod", value=11)

    assert setting() == 7
    assert threshold("prod") == 11

def test_compute_many_with_zero_workers_falls_back_to_default_worker_selection() -> None:
    engine = Engine()

    @engine.input
    def base(i: int) -> int:
        return 0

    @engine.query
    def plus(i: int) -> int:
        return base(i) + 1

    for i in range(6):
        base.set(i, i)

    # workers=0 uses the default worker-count branch.
    result = engine.compute_many([(plus, (i,)) for i in range(6)], workers=0)
    assert result == [i + 1 for i in range(6)]


def test_compute_many_deduplicates_identical_calls_within_batch() -> None:
    engine = Engine()
    started = threading.Event()
    release = threading.Event()
    lock = threading.Lock()
    runs = 0

    @engine.input
    def base() -> int:
        return 0

    @engine.query
    def expensive() -> int:
        nonlocal runs
        with lock:
            runs += 1
        started.set()
        release.wait(timeout=2.0)
        time.sleep(0.01)
        return base() + 1

    base.set(41)
    result_box: list[list[int]] = []
    error_box: list[BaseException] = []

    def run_batch() -> None:
        try:
            result_box.append(engine.compute_many([(expensive, ()) for _ in range(40)], workers=12))
        except BaseException as exc:  # pragma: no cover - defensive
            error_box.append(exc)

    runner = threading.Thread(target=run_batch, daemon=True)
    runner.start()

    assert started.wait(timeout=1.0)
    time.sleep(0.03)
    release.set()

    runner.join(timeout=3.0)
    assert not runner.is_alive()
    assert not error_box
    assert result_box == [[42] * 40]
    assert runs == 1
    assert any(event.event == "dedup_wait" for event in engine.traces())


def test_compute_many_shared_dependency_recomputes_once_per_invalidation_wave() -> None:
    engine = Engine()
    width = 36
    lock = threading.Lock()
    runs = {"shared": 0, "parent": 0}

    @engine.input
    def base() -> int:
        return 0

    @engine.query
    def shared_root() -> int:
        with lock:
            runs["shared"] += 1
        # Keep the shared compute in flight long enough for many parent calls
        # to contend on the same memo key.
        time.sleep(0.02)
        return base() * 100

    @engine.query
    def parent(index: int) -> int:
        with lock:
            runs["parent"] += 1
        return shared_root() + index

    calls = [(parent, (index,)) for index in range(width)]

    base.set(1)
    first = engine.compute_many(calls, workers=12)
    assert first == [100 + index for index in range(width)]
    assert runs["parent"] == width
    assert runs["shared"] == 1

    base.set(2)
    second = engine.compute_many(calls, workers=12)
    assert second == [200 + index for index in range(width)]
    assert runs["parent"] == width * 2
    assert runs["shared"] == 2
    assert any(event.event == "dedup_wait" for event in engine.traces())


def test_load_missing_state_table_raises_operational_error(tmp_path: Path) -> None:
    db_path = tmp_path / "missing-table.db"
    conn = sqlite3.connect(db_path)
    try:
        # Intentionally leave out cascade_state table.
        conn.execute("create table if not exists unrelated (id integer primary key)")
        conn.commit()
    finally:
        conn.close()

    engine = Engine()
    with pytest.raises(sqlite3.OperationalError, match="no such table: cascade_state"):
        engine.load(str(db_path))


def test_load_corrupt_payload_raises_and_keeps_existing_state(tmp_path: Path) -> None:
    db_path = tmp_path / "corrupt.db"

    engine = Engine()

    @engine.input
    def source() -> int:
        return 0

    @engine.query
    def value() -> int:
        return source() + 1

    source.set(10)
    assert value() == 11
    revision_before = engine.revision
    snapshot_before = engine.snapshot()

    conn = sqlite3.connect(db_path)
    try:
        conn.execute("create table if not exists cascade_state (id integer primary key, payload blob not null)")
        conn.execute("delete from cascade_state")
        conn.execute("insert into cascade_state(id, payload) values (1, ?)", (b"not-a-pickle",))
        conn.commit()
    finally:
        conn.close()

    with pytest.raises(Exception):
        engine.load(str(db_path))

    # Failed load must not clobber in-memory state.
    assert engine.revision == revision_before
    assert value(snapshot=snapshot_before) == 11
    assert value() == 11


def test_trace_event_sequence_for_recompute_then_cache_hit() -> None:
    engine = Engine()

    @engine.input
    def source() -> int:
        return 0

    @engine.query
    def compute() -> int:
        return source() + 2

    source.set(5)
    assert compute() == 7
    assert compute() == 7

    events = [event.event for event in engine.traces()]
    # Deterministic happy-path trace ordering for one recompute then cache hit.
    assert events == ["input_set", "recompute_start", "input_read", "recompute_done", "cache_hit"]


def test_inspect_graph_full_summary_oracle_across_varied_graphs() -> None:
    def key(kind: str, function_id: str, args: tuple[object, ...]) -> str:
        return f"{kind}:{function_id}{args}"

    def assert_graph_matches(
        graph: dict[str, object],
        *,
        expected_nodes: set[str],
        expected_edges: set[tuple[str, str]],
        expected_memo_count: int,
        expected_input_count: int,
    ) -> None:
        nodes = set(graph["nodes"])  # type: ignore[arg-type]
        edges = set(tuple(edge) for edge in graph["edges"])  # type: ignore[arg-type]
        assert graph["memo_count"] == expected_memo_count
        assert graph["input_count"] == expected_input_count
        assert nodes == expected_nodes
        assert edges == expected_edges

    # Scenario 1: linear chain.
    linear = Engine()

    @linear.input
    def source(name: str) -> str:
        return ""

    @linear.query
    def parse(name: str) -> tuple[str, ...]:
        return tuple(row.strip() for row in source(name).splitlines() if row.strip())

    @linear.query
    def symbol_count(name: str) -> int:
        return len(parse(name))

    source.set("main", "a\nb")
    assert symbol_count("main") == 2
    assert_graph_matches(
        linear.inspect_graph(),
        expected_nodes={
            key("query", parse.id, ("main",)),
            key("query", symbol_count.id, ("main",)),
        },
        expected_edges={
            (key("query", parse.id, ("main",)), key("input", source.id, ("main",))),
            (key("query", symbol_count.id, ("main",)), key("query", parse.id, ("main",))),
        },
        expected_memo_count=2,
        expected_input_count=1,
    )

    # Scenario 2: shared fan-out.
    fanout = Engine()

    @fanout.input
    def leaf(index: int) -> int:
        return 0

    @fanout.query
    def inc(index: int) -> int:
        return leaf(index) + 1

    @fanout.query
    def total() -> int:
        return inc(0) + inc(1) + inc(2)

    for idx in range(3):
        leaf.set(idx, idx * 10)
    assert total() == 33
    assert_graph_matches(
        fanout.inspect_graph(),
        expected_nodes={
            key("query", inc.id, (0,)),
            key("query", inc.id, (1,)),
            key("query", inc.id, (2,)),
            key("query", total.id, ()),
        },
        expected_edges={
            (key("query", inc.id, (0,)), key("input", leaf.id, (0,))),
            (key("query", inc.id, (1,)), key("input", leaf.id, (1,))),
            (key("query", inc.id, (2,)), key("input", leaf.id, (2,))),
            (key("query", total.id, ()), key("query", inc.id, (0,))),
            (key("query", total.id, ()), key("query", inc.id, (1,))),
            (key("query", total.id, ()), key("query", inc.id, (2,))),
        },
        expected_memo_count=4,
        expected_input_count=3,
    )

    # Scenario 3: dynamic branch rewrite removes stale dependency edges.
    dynamic = Engine()

    @dynamic.input
    def mode() -> int:
        return 0

    @dynamic.input
    def left() -> int:
        return 0

    @dynamic.input
    def right() -> int:
        return 0

    @dynamic.query
    def choose() -> int:
        return left() if mode() == 0 else right()

    mode.set(0)
    left.set(10)
    right.set(20)
    assert choose() == 10
    assert_graph_matches(
        dynamic.inspect_graph(),
        expected_nodes={key("query", choose.id, ())},
        expected_edges={
            (key("query", choose.id, ()), key("input", mode.id, ())),
            (key("query", choose.id, ()), key("input", left.id, ())),
        },
        expected_memo_count=1,
        expected_input_count=3,
    )

    mode.set(1)
    assert choose() == 20
    assert_graph_matches(
        dynamic.inspect_graph(),
        expected_nodes={key("query", choose.id, ())},
        expected_edges={
            (key("query", choose.id, ()), key("input", mode.id, ())),
            (key("query", choose.id, ()), key("input", right.id, ())),
        },
        expected_memo_count=1,
        expected_input_count=3,
    )
