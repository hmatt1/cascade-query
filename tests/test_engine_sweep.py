from cascade.engine import Engine


def test_sweep_unaccessed() -> None:
    engine = Engine()

    @engine.input
    def file_content(path: str) -> str:
        return "content"

    @engine.query
    def compile_file(path: str) -> str:
        return file_content(path).upper()

    @engine.query
    def compile_project(paths: tuple[str, ...]) -> str:
        return " + ".join(compile_file(p) for p in paths)

    # 1. Populate cache with A, B, C
    res = compile_project(("A", "B", "C"))
    assert res == "CONTENT + CONTENT + CONTENT"

    # Store access ID after the first pass
    start_id = engine.access_id

    # 2. Simulate second pass: file "C" is deleted, only "A" and "B" remain.
    res2 = compile_project(("A", "B"))
    assert res2 == "CONTENT + CONTENT"

    # Verify that file_content("C") and compile_file("C") are still in cache
    graph_before = engine.inspect_graph()
    nodes_before = set(graph_before["nodes"])
    assert (
        "query:tests.test_engine_sweep:test_sweep_unaccessed.<locals>.compile_file('C',)"
        in nodes_before
    )

    # 3. Sweep
    engine.sweep_unaccessed(start_id)

    # 4. Verify unaccessed nodes are dropped, accessed are kept
    graph_after = engine.inspect_graph()
    nodes_after = set(graph_after["nodes"])

    assert (
        "query:tests.test_engine_sweep:test_sweep_unaccessed.<locals>.compile_file('C',)"
        not in nodes_after
    )
    assert (
        "query:tests.test_engine_sweep:test_sweep_unaccessed.<locals>.compile_file('A',)"
        in nodes_after
    )
    assert (
        "query:tests.test_engine_sweep:test_sweep_unaccessed.<locals>.compile_file('B',)"
        in nodes_after
    )

    # Note: sweep_unaccessed operates on query memos (Memos), not input versions.
    # The input nodes reported by inspect_graph might still show up since they
    # are tracked separately in inputs, but the memo table eviction is what frees up memory.


def test_sweep_empty_cache() -> None:
    engine = Engine()
    start_id = engine.access_id

    # sweeping an empty cache should not crash
    engine.sweep_unaccessed(start_id)

    graph_after = engine.inspect_graph()
    assert len(graph_after["nodes"]) == 0


def test_sweep_resets_lru() -> None:
    engine = Engine(max_entries=2)

    @engine.query
    def q(x: int) -> int:
        return x

    q(1)
    q(2)
    start_id = engine.access_id
    q(3)

    # 3 pushes out 1 because max_entries=2 (2 and 3 remain)
    graph = engine.inspect_graph()
    assert len(graph["nodes"]) == 2

    engine.sweep_unaccessed(start_id)
    # Only 3 remains since it was accessed after start_id
    graph_after = engine.inspect_graph()
    assert len(graph_after["nodes"]) == 1
