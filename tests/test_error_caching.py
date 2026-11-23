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


def test_caught_cached_error_does_not_escape_on_reverification():
    """Re-checking a parent must not raise an error the parent handles."""
    engine = Engine()

    @engine.input
    def source():
        return 1

    @engine.query(cache_exceptions=True)
    def failing():
        raise ValueError(f"bad {source()}")

    @engine.query
    def guarded():
        try:
            return failing()
        except ValueError:
            return "fallback"

    @engine.query
    def top():
        return guarded()

    assert guarded() == "fallback"
    assert top() == "fallback"

    # The cached error changes; the parent is re-verified, not just re-run.
    source.set(value=2)
    assert top() == "fallback"
    source.set(value=3)
    assert guarded() == "fallback"

    # The error is still raised to a caller that asks for it directly.
    with pytest.raises(ValueError, match="bad 3"):
        failing()


def test_unchanged_cached_error_keeps_handling_parent_cached():
    engine = Engine()
    calls = 0

    @engine.input
    def source():
        return 1

    @engine.query(cache_exceptions=True)
    def failing():
        source()
        raise ValueError("bad")

    @engine.query
    def guarded():
        nonlocal calls
        calls += 1
        try:
            return failing()
        except ValueError:
            return "fallback"

    assert guarded() == "fallback"
    source.set(value=2)
    assert guarded() == "fallback"
    assert calls == 1  # same error as before, so the parent is not re-run


@pytest.mark.parametrize("cache_exceptions", [False, (KeyError,)])
def test_caught_uncached_error_still_tracks_dependencies(cache_exceptions):
    """A fallback for an uncached error must refresh when the inputs change."""
    engine = Engine()

    @engine.input
    def source():
        return "bad"

    @engine.query(cache_exceptions=cache_exceptions)
    def parse():
        value = source()
        if value == "bad":
            raise ValueError("bad input")
        return value

    @engine.query
    def guarded():
        try:
            return parse()
        except ValueError:
            return "fallback"

    assert guarded() == "fallback"
    source.set(value="good")
    assert guarded() == "good"
    source.set(value="bad")
    assert guarded() == "fallback"


@pytest.mark.asyncio
async def test_async_caught_cached_error_does_not_escape_on_reverification():
    engine = Engine()
    calls = 0

    @engine.input
    def source():
        return 1

    @engine.query(cache_exceptions=True)
    async def failing():
        raise ValueError(f"bad {source()}")

    @engine.query(cache_exceptions=True)
    async def steady():
        source()
        raise ValueError("bad")

    @engine.query
    async def guarded():
        try:
            return await failing()
        except ValueError:
            return "fallback"

    @engine.query
    async def steady_guarded():
        nonlocal calls
        calls += 1
        try:
            return await steady()
        except ValueError:
            return "fallback"

    @engine.query
    async def top():
        return await guarded()

    assert await guarded() == "fallback"
    assert await top() == "fallback"
    assert await steady_guarded() == "fallback"

    source.set(2)
    assert await top() == "fallback"
    assert await steady_guarded() == "fallback"
    assert calls == 1  # the error did not change, so the parent is not re-run
    source.set(3)
    assert await guarded() == "fallback"

    with pytest.raises(ValueError, match="bad 3"):
        await failing()


@pytest.mark.asyncio
@pytest.mark.parametrize("cache_exceptions", [False, (KeyError,)])
async def test_async_caught_uncached_error_still_tracks_dependencies(
    cache_exceptions,
):
    engine = Engine()

    @engine.input
    def source():
        return "bad"

    @engine.query(cache_exceptions=cache_exceptions)
    async def parse():
        value = source()
        if value == "bad":
            raise ValueError("bad input")
        return value

    @engine.query
    async def guarded():
        try:
            return await parse()
        except ValueError:
            return "fallback"

    assert await guarded() == "fallback"
    source.set("good")
    assert await guarded() == "good"
    source.set("bad")
    assert await guarded() == "fallback"


def test_uncached_error_two_levels_down_reaches_the_handler():
    """The error surfaces in the query that handles it, not in the check."""
    engine = Engine()

    @engine.input
    def source():
        return "good"

    @engine.query(cache_exceptions=False)
    def leaf():
        value = source()
        if value == "bad":
            raise ValueError("bad input")
        return value

    @engine.query(cache_exceptions=False)
    def middle():
        return leaf().upper()

    @engine.query
    def guarded():
        try:
            return middle()
        except ValueError:
            return "fallback"

    assert guarded() == "GOOD"
    source.set("bad")
    assert guarded() == "fallback"
    with pytest.raises(ValueError, match="bad input"):
        middle()
    source.set("fine")
    assert guarded() == "FINE"


def test_disk_cached_parent_survives_dependency_that_now_raises(tmp_path):
    """Checking a disk entry against a dependency that raises is a miss."""

    def build(engine):
        @engine.input
        def source():
            return "good"

        @engine.query(cache_exceptions=False)
        def parse():
            value = source()
            if value == "bad":
                raise ValueError("bad input")
            return value

        @engine.query
        def guarded():
            try:
                return parse()
            except ValueError:
                return "fallback"

        return source, guarded

    first = Engine(cache_dir=str(tmp_path))
    source, guarded = build(first)
    assert guarded() == "good"
    first.shutdown()

    second = Engine(cache_dir=str(tmp_path))
    source, guarded = build(second)
    source.set("bad")
    assert guarded() == "fallback"
    second.shutdown()
