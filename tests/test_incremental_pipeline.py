from __future__ import annotations

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from cascade import CascadeDict, CascadeList, CascadeSet, Engine


@pytest.fixture
def engine():
    return Engine()


def pipes_for(engine, name):
    return [
        p
        for p in engine.inspect_pipelines()
        if f".{name}:" in p["site"] or p["site"].startswith(f"{name}:")
    ]


# --- the headline behavior: one diff means one unit of work ---


def test_single_append_runs_map_and_filter_once(engine):
    docs = CascadeList(engine, list(range(1000)))
    map_calls = []
    filter_calls = []

    def score(d):
        map_calls.append(d)
        return d * 2

    def valid(d):
        filter_calls.append(d)
        return d % 2 == 0

    @engine.query
    def total():
        return sum(score(d) for d in docs if valid(d))

    assert total() == sum(d * 2 for d in range(1000) if d % 2 == 0)
    assert len(map_calls) == 500 and len(filter_calls) == 1000

    map_calls.clear()
    filter_calls.clear()
    docs.append(5000)
    assert total() == sum(d * 2 for d in range(1000) if d % 2 == 0) + 10000
    assert filter_calls == [5000]
    assert map_calls == [5000]


def test_unchanged_collection_serves_query_from_cache(engine):
    xs = CascadeList(engine, [1, 2, 3])
    runs = []

    @engine.query
    def total():
        runs.append(1)
        return sum(x for x in xs)

    total()
    total()
    assert len(runs) == 1


def test_noop_churn_preserves_downstream_via_early_bailout(engine):
    xs = CascadeList(engine, [1, 2, 3])
    downstream_runs = []

    @engine.query
    def total():
        return sum(x for x in xs)

    @engine.query
    def downstream():
        downstream_runs.append(1)
        return total() + 1

    assert downstream() == 7
    xs.append(9)
    xs.remove(9)
    assert downstream() == 7
    assert len(downstream_runs) == 1


# --- reducer matrix: incremental result always equals a fresh recompute ---


def _mixed_mutations(xs):
    xs.append(11)
    xs.insert(0, -3)
    xs[2] = 40
    xs.pop(1)
    xs.extend([7, 7, 0])
    xs.remove(7)
    xs.sort()
    xs.append(2)


REDUCER_QUERIES = {
    "sum": lambda xs: sum(x * 3 for x in xs if x % 2 == 0),
    "len": lambda xs: len([x for x in xs if x > 0]),
    "any": lambda xs: any(x > 30 for x in xs),
    "all": lambda xs: all(x >= 0 for x in xs),
    "min": lambda xs: min(x * 2 for x in xs),
    "max": lambda xs: max(x * 2 for x in xs),
    "list": lambda xs: [x + 1 for x in xs if x != 0],
    "set": lambda xs: {x % 5 for x in xs},
    "sorted": lambda xs: sorted(x for x in xs if x < 50),
    "sorted_key_rev": lambda xs: sorted(xs, key=lambda x: (x % 3, x), reverse=True),
    "join": lambda xs: ",".join(str(x) for x in xs),
    "reversed": lambda xs: [x * 10 for x in reversed(xs)],
}


@pytest.mark.parametrize("name", sorted(REDUCER_QUERIES))
def test_list_reducers_match_recompute_through_mutations(engine, name):
    fn = REDUCER_QUERIES[name]
    xs = CascadeList(engine, [5, 1, 4, 2, 3])

    @engine.query
    def q():
        return fn(xs)

    # fn(xs) inside q is a plain call; incrementality is exercised separately.
    # Here the point is semantic equivalence across every mutation kind.
    assert q() == fn(list(xs))
    _mixed_mutations(xs)
    assert q() == fn(list(xs))
    xs.clear()
    if name in ("min", "max"):
        with pytest.raises(ValueError):
            q()
    else:
        assert q() == fn([])


def test_rewritten_reducers_match_recompute_through_mutations(engine):
    xs = CascadeList(engine, [5, 1, 4, 2, 3])

    @engine.query
    def q():
        return (
            sum(x * 3 for x in xs if x % 2 == 0),
            len([x for x in xs if x > 0]),
            any(x > 30 for x in xs),
            all(x >= 0 for x in xs),
            min(x * 2 for x in xs),
            max(x * 2 for x in xs),
            [x + 1 for x in xs if x != 0],
            {x % 5 for x in xs},
            sorted(x for x in xs if x < 50),
            sorted(xs, key=lambda x: (x % 3, x), reverse=True),
            ",".join(str(x) for x in xs),
            [x * 10 for x in reversed(xs)],
        )

    assert q.raw.__cascade_rewritten__

    def expect(values):
        return (
            sum(x * 3 for x in values if x % 2 == 0),
            len([x for x in values if x > 0]),
            any(x > 30 for x in values),
            all(x >= 0 for x in values),
            min(x * 2 for x in values),
            max(x * 2 for x in values),
            [x + 1 for x in values if x != 0],
            {x % 5 for x in values},
            sorted(x for x in values if x < 50),
            sorted(values, key=lambda x: (x % 3, x), reverse=True),
            ",".join(str(x) for x in values),
            [x * 10 for x in reversed(values)],
        )

    assert q() == expect(list(xs))
    _mixed_mutations(xs)
    assert q() == expect(list(xs))
    xs.pop()
    xs[0] = 100
    assert q() == expect(list(xs))


def test_min_max_handle_removal_of_current_extreme(engine):
    xs = CascadeList(engine, [3, 9, 1])

    @engine.query
    def extremes():
        return (min(x for x in xs), max(x for x in xs))

    assert extremes() == (1, 9)
    xs.remove(1)
    xs.remove(9)
    assert extremes() == (3, 3)
    xs.clear()
    with pytest.raises(ValueError):
        extremes()


def test_any_all_flip_on_updates(engine):
    xs = CascadeList(engine, [2, 4])

    @engine.query
    def flags():
        return (any(x % 2 for x in xs), all(x % 2 == 0 for x in xs))

    assert flags() == (False, True)
    xs[0] = 3
    assert flags() == (True, False)
    xs[0] = 6
    assert flags() == (False, True)


def test_empty_collection_reducers(engine):
    xs = CascadeList(engine)

    @engine.query
    def q():
        return (
            sum(x for x in xs),
            len(xs),
            any(x for x in xs),
            all(x for x in xs),
            [x for x in xs],
            sorted(xs),
            ",".join(str(x) for x in xs),
        )

    assert q() == (0, 0, False, True, [], [], "")


def test_dict_reducer_over_pairs_last_wins(engine):
    pairs = CascadeList(engine, [("a", 1), ("a", 2), ("b", 3)])

    @engine.query
    def as_dict():
        return dict((k, v) for k, v in pairs)

    assert as_dict() == {"a": 2, "b": 3}
    pairs.pop(1)
    assert as_dict() == {"a": 1, "b": 3}
    pairs.append(("b", 9))
    assert as_dict() == {"a": 1, "b": 9}


def test_sum_of_strings_raises_typeerror_like_builtin(engine):
    xs = CascadeList(engine, ["a"])

    @engine.query
    def q():
        return sum(x for x in xs)

    with pytest.raises(TypeError):
        q()


def test_join_of_non_string_raises_typeerror(engine):
    xs = CascadeList(engine, [1])

    @engine.query
    def q():
        return "".join(x for x in xs)

    with pytest.raises(TypeError):
        q()


# --- dict views ---


def test_dict_views_reduce_keys_values_items(engine):
    d = CascadeDict(engine, {"a": 1, "b": 2})

    @engine.query
    def q():
        return (
            sum(len(k) for k in d),
            sorted(d),
            len(d),
            sum(d.values()),
            max(d.values()),
            [k + str(v) for k, v in d.items()],
            dict(d),
            set(d.keys()),
        )

    assert q() == (2, ["a", "b"], 2, 3, 2, ["a1", "b2"], {"a": 1, "b": 2}, {"a", "b"})
    d["cc"] = 10
    del d["a"]
    assert q() == (
        3,
        ["b", "cc"],
        2,
        12,
        10,
        ["b2", "cc10"],
        {"b": 2, "cc": 10},
        {"b", "cc"},
    )


def test_value_upsert_is_noop_for_keys_projection(engine):
    d = CascadeDict(engine, {"a": 1})
    key_maps = []

    @engine.query
    def key_lengths():
        return sum(len(k) for k in d if key_maps.append(k) is None)

    assert key_lengths() == 1
    key_maps.clear()
    d["a"] = 999
    assert key_lengths() == 1
    assert key_maps == []
    d["bb"] = 1
    assert key_lengths() == 3
    assert key_maps == ["bb"]


def test_values_projection_replaces_on_upsert(engine):
    d = CascadeDict(engine, {"a": 1, "b": 2})

    @engine.query
    def total():
        return sum(v * 10 for v in d.values())

    assert total() == 30
    d["a"] = 5
    assert total() == 70


# --- checkpointing: consumers advance independently, monotonic revisions ---


def test_multi_consumer_checkpoints_advance_independently(engine):
    xs = CascadeList(engine, [1])

    @engine.query
    def a():
        return sum(x for x in xs)

    @engine.query
    def b():
        return len([x for x in xs])

    a(), b()
    xs.append(2)
    a()
    assert pipes_for(engine, "a")[0]["last_rev"] == 2
    assert pipes_for(engine, "b")[0]["last_rev"] == 1
    xs.append(3)
    assert b() == 3
    assert pipes_for(engine, "b")[0]["last_rev"] == 3


def test_lagging_consumer_requests_only_newer_diffs(engine):
    xs = CascadeList(engine, [1])
    seen = []

    def spy(x):
        seen.append(x)
        return x

    @engine.query
    def a():
        return sum(x for x in xs)

    @engine.query
    def b():
        return sum(map(spy, xs))

    a(), b()
    seen.clear()
    xs.append(2)
    xs.append(3)
    a()  # advances only a's pipeline
    b()
    assert seen == [2, 3]


def test_same_site_different_collections_get_separate_states(engine):
    left = CascadeList(engine, [1, 2])
    right = CascadeList(engine, [10])

    @engine.query
    def total(which):
        source = left if which == "l" else right
        return sum(x for x in source)

    assert total("l") == 3
    assert total("r") == 10
    left.append(4)
    assert total("l") == 7
    assert total("r") == 10


# --- snapshots ---


def test_snapshot_reads_replay_history(engine):
    xs = CascadeList(engine, [1, 2])

    @engine.query
    def total():
        return sum(x for x in xs)

    assert total() == 3
    snap = engine.snapshot()
    xs.append(10)
    xs[0] = 100
    assert total() == 112
    assert total(snapshot=snap) == 3
    assert total() == 112
    old = total(snapshot=snap)
    assert old == 3


# --- correctness safety nets ---


def test_closure_value_change_rebuilds_state(engine):
    xs = CascadeList(engine, [1, 2, 3])
    factor_input = engine.input(lambda: 2)

    @engine.query
    def scaled():
        f = factor_input()
        return sum(x * f for x in xs)

    assert scaled() == 12
    factor_input.set(10)
    assert scaled() == 60
    xs.append(4)
    assert scaled() == 100


def test_tracked_read_inside_stage_marks_pipeline_impure(engine):
    xs = CascadeList(engine, [1, 2])
    offset = engine.input(lambda: 100)

    @engine.query
    def total():
        return sum(x + offset() for x in xs)

    assert total() == 203
    offset.set(1000)
    assert total() == 2003
    xs.append(3)
    assert total() == 3006
    assert pipes_for(engine, "total")[0]["last_rev"] == 3


def test_query_called_per_item_stays_fresh(engine):
    xs = CascadeList(engine, [1, 2])
    base = engine.input(lambda: 10)

    @engine.query
    def shifted(x):
        return x + base()

    @engine.query
    def agg():
        return sum(shifted(x) for x in xs)

    assert agg() == 23
    base.set(0)
    assert agg() == 3
    xs.append(3)
    assert agg() == 6


def test_unhashable_closure_makes_pipeline_volatile_but_correct(engine):
    xs = CascadeList(engine, [1, 2])
    lens = object()
    weights = {"w": 3}

    @engine.query(memoize=False)
    def weighted():
        return sum(x * weights["w"] for x in xs if lens is not None)

    assert weighted() == 9
    weights["w"] = 5
    xs.append(3)
    assert weighted() == 30


def test_stage_helper_defaults_participate_in_fingerprint(engine):
    xs = CascadeList(engine, [1, 2])

    def scale(x, factor=10):
        return x * factor

    @engine.query
    def q():
        return sum(map(scale, xs))

    assert q() == 30
    xs.append(3)
    assert q() == 60


def test_error_in_stage_poisons_state_then_recovers(engine):
    xs = CascadeList(engine, [1, 0, 2])

    @engine.query(cache_exceptions=False)
    def q():
        return sum(10 // x for x in xs)

    with pytest.raises(ZeroDivisionError):
        q()
    xs.remove(0)
    assert q() == 15
    xs.append(5)
    assert q() == 17


# --- fallback: plain sources keep exact python semantics ---


def test_fallback_matches_python_for_plain_sources(engine):
    plain_list = [3, 1, 2]
    plain_set = {2, 1}
    plain_dict = {"b": 2, "a": 1}

    @engine.query
    def q():
        return (
            sum(x * 2 for x in plain_list),
            len([x for x in plain_list if x > 1]),
            sorted(plain_list, key=lambda x: -x),
            list(reversed(plain_list)),
            "|".join(str(x) for x in plain_list),
            min(plain_set),
            max(plain_set),
            sorted(plain_dict),
            dict(plain_dict),
            list(plain_dict.items()),
            list(filter(None, [0, 1, "", "a"])),
            list(map(str, plain_list)),
            any(x > 2 for x in plain_list),
            all(plain_set),
            list(range(3)),
            len(plain_list),
        )

    assert q.raw.__cascade_rewritten__
    assert q() == (
        12,
        2,
        [3, 2, 1],
        [2, 1, 3],
        "3|1|2",
        1,
        2,
        ["a", "b"],
        {"b": 2, "a": 1},
        [("b", 2), ("a", 1)],
        [1, "a"],
        ["3", "1", "2"],
        True,
        True,
        [0, 1, 2],
        3,
    )


def test_fallback_errors_match_python(engine):
    @engine.query(cache_exceptions=False)
    def empty_min():
        return min(x for x in [])

    with pytest.raises(ValueError):
        empty_min()

    gen = (i for i in range(3))

    @engine.query(cache_exceptions=False)
    def bad_len():
        return len(gen)

    with pytest.raises(TypeError):
        bad_len()


def test_reversed_of_cascade_set_raises_like_builtin(engine):
    s = CascadeSet(engine, {1, 2})

    @engine.query(cache_exceptions=False)
    def q():
        return [x for x in reversed(s)]

    with pytest.raises(TypeError):
        q()


def test_reversed_dict_projections(engine):
    d = CascadeDict(engine, {"a": 1, "b": 2})

    @engine.query
    def q():
        return (list(reversed(d)), [v for v in reversed(d.values())])

    assert q() == (["b", "a"], [2, 1])
    d["c"] = 3
    assert q() == (["c", "b", "a"], [3, 2, 1])


# --- fusion ---


def test_adjacent_stages_condense_into_one(engine):
    xs = CascadeList(engine, [1, 2, 3, 4])

    @engine.query
    def q():
        return sum(x + 1 for x in map(lambda v: v * 2, xs) if x > 4)

    assert q() == sum(x + 1 for x in map(lambda v: v * 2, [1, 2, 3, 4]) if x > 4)
    pipe = pipes_for(engine, "q")[0]
    assert pipe["stages"] == ["map", "filter", "map"]
    assert pipe["fused_stage_count"] == 1


def test_fusion_stops_at_first_failing_filter(engine):
    xs = CascadeList(engine, [1])
    later = []

    @engine.query
    def q():
        return sum(later.append(x) or 0 for x in xs if x > 100)

    assert q() == 0
    assert later == []


# --- set-sourced ordered output is insertion-ordered ---


def test_set_source_list_output_is_insertion_ordered(engine):
    s = CascadeSet(engine)
    s.add(30)
    s.add(10)
    s.add(20)

    @engine.query
    def as_list():
        return [x for x in s]

    assert as_list() == [30, 10, 20]
    s.discard(10)
    s.add(10)
    assert as_list() == [30, 20, 10]


# --- property tests: random mutations, incremental equals recompute ---


@settings(max_examples=40, deadline=None)
@given(
    initial=st.lists(st.integers(-20, 20), max_size=6),
    ops=st.lists(
        st.one_of(
            st.tuples(st.just("append"), st.integers(-20, 20)),
            st.tuples(st.just("insert"), st.integers(-3, 6), st.integers(-20, 20)),
            st.tuples(st.just("set"), st.integers(0, 5), st.integers(-20, 20)),
            st.tuples(st.just("pop"), st.integers(-3, 5)),
            st.tuples(st.just("sort")),
        ),
        max_size=12,
    ),
)
def test_property_list_pipeline_equals_recompute(initial, ops):
    engine = Engine()
    xs = CascadeList(engine, initial)

    @engine.query
    def agg():
        return (
            sum(x * 2 for x in xs if x % 3 != 0),
            len([x for x in xs if x < 0]),
            sorted(x for x in xs),
            [str(x) for x in xs],
            any(x > 15 for x in xs),
        )

    def expected(values):
        return (
            sum(x * 2 for x in values if x % 3 != 0),
            len([x for x in values if x < 0]),
            sorted(x for x in values),
            [str(x) for x in values],
            any(x > 15 for x in values),
        )

    assert agg() == expected(list(xs))
    for op in ops:
        try:
            if op[0] == "append":
                xs.append(op[1])
            elif op[0] == "insert":
                xs.insert(op[1], op[2])
            elif op[0] == "set":
                xs[op[1]] = op[2]
            elif op[0] == "pop":
                xs.pop(op[1])
            else:
                xs.sort()
        except IndexError:
            continue
        assert agg() == expected(list(xs))


@settings(max_examples=40, deadline=None)
@given(
    ops=st.lists(
        st.one_of(
            st.tuples(st.just("set"), st.integers(0, 4), st.integers(-9, 9)),
            st.tuples(st.just("del"), st.integers(0, 4)),
        ),
        max_size=12,
    ),
)
def test_property_dict_pipeline_equals_recompute(ops):
    engine = Engine()
    d = CascadeDict(engine, {0: 1, 1: -2})

    @engine.query
    def agg():
        return (sum(d.values()), sorted(d), dict(d.items()))

    def expected(m):
        return (sum(m.values()), sorted(m), dict(m.items()))

    assert agg() == expected(dict(d))
    for op in ops:
        if op[0] == "set":
            d[op[1]] = op[2]
        elif op[1] in d:
            del d[op[1]]
        assert agg() == expected(dict(d))


# --- large collections: ordered structure crosses block boundaries ---


def test_ordered_reducers_across_block_boundaries(engine):
    n = 2500  # SortedKeyList blocks hold 512 entries; this forces splits
    xs = CascadeList(engine, list(range(n)))

    @engine.query
    def ordered():
        return (min(x for x in xs), max(x for x in xs), sorted(x for x in xs))

    lo, hi, srt = ordered()
    assert (lo, hi) == (0, n - 1)
    assert srt == list(range(n))

    xs.append(-5)
    xs.append(n + 5)
    lo, hi, srt = ordered()
    assert (lo, hi) == (-5, n + 5)
    assert srt[0] == -5 and srt[-1] == n + 5

    # remove a contiguous run wide enough to empty interior blocks
    del xs[100:800]
    xs.remove(-5)
    xs.remove(n + 5)
    lo, hi, srt = ordered()
    expected = list(range(0, 100)) + list(range(800, n))
    assert (lo, hi) == (0, n - 1)
    assert srt == expected


def test_plain_reversed_reads_track_dependencies(engine):
    xs = CascadeList(engine, [1, 2])
    d = CascadeDict(engine, {"a": 1, "b": 2})
    runs = []

    @engine.query(incremental=False)
    def q():
        runs.append(1)
        return (
            list(reversed(xs)),
            list(reversed(d)),
            list(reversed(d.values())),
            list(reversed(d.items())),
            xs != [1],
        )

    assert q() == ([2, 1], ["b", "a"], [2, 1], [("b", 2), ("a", 1)], True)
    q()
    assert len(runs) == 1
    xs.append(3)
    assert q()[0] == [3, 2, 1]
    d["c"] = 3
    assert q()[1] == ["c", "b", "a"]
    assert len(runs) == 3


def test_fallback_sorted_kwargs_and_join_on_plain_iterables(engine):
    rows = [3, 1, 2]

    @engine.query
    def q():
        return (
            sorted(map(str, rows), key=len, reverse=True),
            "-".join(map(str, rows)),
        )

    assert q.raw.__cascade_rewritten__
    assert q() == (["3", "1", "2"], "3-1-2")
