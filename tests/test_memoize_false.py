from cascade import Engine


def test_memoize_false():
    engine = Engine()

    # Track execution count
    eval_count = 0

    @engine.input
    def raw_data() -> int:
        return 1

    @engine.query(memoize=False)
    def mapped_data() -> int:
        nonlocal eval_count
        eval_count += 1
        return raw_data() * 2

    @engine.query
    def aggregated_result() -> int:
        return mapped_data() + 10

    # First run
    assert aggregated_result() == 12
    assert eval_count == 1

    # Second run (cache hit for aggregated_result)
    assert aggregated_result() == 12
    # mapped_data is NOT evaluated because aggregated_result is cached and green
    assert eval_count == 1

    # Update input
    raw_data.set(5)

    # Third run (raw_data changed)
    assert aggregated_result() == 20
    # mapped_data MUST be re-evaluated
    assert eval_count == 3

    # Verify that mapped_data doesn't hold the value in the cache
    keys = [k for k in engine._store.memos if k[0] == "query" and "mapped_data" in k[1]]
    assert len(keys) == 1
    memo = engine._store.memos.get(keys[0])
    assert memo is not None
    # We strip the value to save memory
    assert memo.value is None


def test_memoize_false_multiple_calls_same_revision():
    engine = Engine()

    eval_count = 0

    @engine.input
    def root() -> int:
        return 10

    @engine.query(memoize=False)
    def unmemoized() -> int:
        nonlocal eval_count
        eval_count += 1
        return root()

    @engine.query
    def d1() -> int:
        return unmemoized()

    @engine.query
    def d2() -> int:
        return unmemoized()

    @engine.query
    def final() -> int:
        return d1() + d2()

    assert final() == 20
    # unmemoized should be called twice because its result isn't cached in memos
    assert eval_count == 2
