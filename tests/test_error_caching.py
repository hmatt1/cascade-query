import pytest

from cascade.engine import Engine


def test_error_is_cached():
    engine = Engine()
    calls = 0

    @engine.query(cache_exceptions=True)
    def throws_error():
        nonlocal calls
        calls += 1
        raise ValueError("Something went wrong")

    with pytest.raises(ValueError, match="Something went wrong"):
        throws_error()
    assert calls == 1

    # Should hit cache and raise again without incrementing calls
    with pytest.raises(ValueError, match="Something went wrong"):
        throws_error()
    assert calls == 1


def test_error_cache_invalidation():
    engine = Engine()

    @engine.input
    def get_input():
        return "bad"

    calls = 0

    @engine.query(cache_exceptions=True)
    def process():
        nonlocal calls
        calls += 1
        val = get_input()
        if val == "bad":
            raise ValueError("bad input")
        return val

    with pytest.raises(ValueError, match="bad input"):
        process()
    assert calls == 1

    with pytest.raises(ValueError, match="bad input"):
        process()
    assert calls == 1

    # Invalidate by changing input
    get_input.set(value="good")
    assert process() == "good"
    assert calls == 2


def test_dependent_query_rethrows():
    engine = Engine()
    b_calls = 0

    @engine.query(cache_exceptions=True)
    def b():
        nonlocal b_calls
        b_calls += 1
        raise RuntimeError("b failed")

    a_calls = 0

    @engine.query(cache_exceptions=True)
    def a():
        nonlocal a_calls
        a_calls += 1
        return b()

    with pytest.raises(RuntimeError, match="b failed"):
        a()
    assert a_calls == 1
    assert b_calls == 1

    with pytest.raises(RuntimeError, match="b failed"):
        a()
    assert a_calls == 1
    assert b_calls == 1


class MyError(Exception):
    pass


def test_selective_exception_caching():
    engine = Engine()
    calls = 0

    @engine.input
    def error_type():
        return "my"

    @engine.query(cache_exceptions=(MyError,))
    def throws_specific():
        nonlocal calls
        calls += 1
        err = error_type()
        if err == "my":
            raise MyError("my error")
        else:
            raise ValueError("value error")

    with pytest.raises(MyError):
        throws_specific()
    assert calls == 1

    with pytest.raises(MyError):
        throws_specific()
    assert calls == 1  # cached

    error_type.set(value="value")
    with pytest.raises(ValueError):
        throws_specific()
    assert calls == 2

    # Should NOT be cached
    with pytest.raises(ValueError):
        throws_specific()
    assert calls == 3
