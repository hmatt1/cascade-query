from __future__ import annotations

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from cascade import CascadeDict, CascadeList, CascadeSet, Engine

from typing import Any

@pytest.fixture(params=["mdbx", "sqlite"], autouse=True)
def engine_backend(request: pytest.FixtureRequest, monkeypatch: pytest.MonkeyPatch) -> str:
    original_init = Engine.__init__

    def new_init(self: Any, *args: Any, **kwargs: Any) -> None:
        kwargs.setdefault("cache_backend", request.param)
        original_init(self, *args, **kwargs)

    monkeypatch.setattr(Engine, "__init__", new_init)
    return request.param



@pytest.fixture
def engine():
    return Engine()


def diffs(collection):
    return list(collection._core.log.entries)


def replayed(collection):
    log = collection._core.log
    contents = log.contents_at(log.head)
    if isinstance(collection, CascadeList):
        return [v for _, v in contents]
    if isinstance(collection, CascadeSet):
        return set(contents)
    return contents


# --- CascadeList ---


def test_list_append_emits_insert_with_uid_index_and_rev(engine):
    xs = CascadeList(engine, [10])
    xs.append(20)
    d = diffs(xs)[-1]
    assert d["action"] == "insert"
    assert d["value"] == 20
    assert d["index"] == 1
    assert isinstance(d["uid"], int)
    assert d["rev"] == 2


def test_list_diff_revisions_are_monotonic(engine):
    xs = CascadeList(engine)
    xs.extend([1, 2])
    xs.append(3)
    xs.pop()
    revs = [d["rev"] for d in diffs(xs)]
    assert revs == [1, 2, 3, 4]
    assert xs._core.log.head == 4


def test_list_uid_is_stable_across_positional_shifts(engine):
    xs = CascadeList(engine, ["a", "b", "c"])
    uid_b = diffs(xs)[1]["uid"]
    xs.insert(0, "z")
    xs[2] = "B"
    update = diffs(xs)[-1]
    assert update == {"action": "update", "uid": uid_b, "value": "B", "rev": 5}


def test_list_pop_translates_position_to_uid(engine):
    xs = CascadeList(engine, list(range(6)))
    uid_5 = diffs(xs)[5]["uid"]
    xs.pop(5)
    d = diffs(xs)[-1]
    assert d["action"] == "remove"
    assert d["uid"] == uid_5
    assert list(xs) == [0, 1, 2, 3, 4]


def test_list_negative_pop_and_delitem(engine):
    xs = CascadeList(engine, [1, 2, 3])
    xs.pop(-2)
    assert list(xs) == [1, 3]
    del xs[-1]
    assert list(xs) == [1]
    assert replayed(xs) == [1]


def test_list_remove_targets_first_match(engine):
    xs = CascadeList(engine, [7, 8, 7])
    first_uid = diffs(xs)[0]["uid"]
    xs.remove(7)
    assert diffs(xs)[-1]["uid"] == first_uid
    assert list(xs) == [8, 7]


def test_list_slice_assignment_and_delete(engine):
    xs = CascadeList(engine, [0, 1, 2, 3, 4])
    xs[1:3] = [10, 11, 12]
    assert list(xs) == [0, 10, 11, 12, 3, 4]
    assert replayed(xs) == list(xs)
    del xs[::2]
    assert replayed(xs) == list(xs)


def test_list_extended_slice_assignment_updates_in_place(engine):
    xs = CascadeList(engine, [0, 1, 2, 3])
    xs[::2] = [100, 102]
    assert list(xs) == [100, 1, 102, 3]
    assert all(d["action"] == "update" for d in diffs(xs)[-2:])
    assert replayed(xs) == list(xs)


def test_list_sort_and_reverse_keep_uids(engine):
    xs = CascadeList(engine, [3, 1, 2])
    uid_of_1 = diffs(xs)[1]["uid"]
    xs.sort()
    assert list(xs) == [1, 2, 3]
    assert xs._uids[0] == uid_of_1
    xs.reverse()
    assert list(xs) == [3, 2, 1]
    assert replayed(xs) == [3, 2, 1]


def test_list_sort_with_key_and_reverse(engine):
    xs = CascadeList(engine, ["bb", "a", "ccc"])
    xs.sort(key=len, reverse=True)
    assert list(xs) == ["ccc", "bb", "a"]
    assert replayed(xs) == list(xs)


def test_list_inplace_operators(engine):
    xs = CascadeList(engine, [1])
    xs += [2, 3]
    assert list(xs) == [1, 2, 3]
    xs *= 2
    assert list(xs) == [1, 2, 3, 1, 2, 3]
    xs *= 0
    assert list(xs) == []
    assert replayed(xs) == []


def test_list_insert_clamps_like_builtin(engine):
    xs = CascadeList(engine, [1, 2])
    xs.insert(100, 3)
    xs.insert(-100, 0)
    assert list(xs) == [0, 1, 2, 3]
    assert replayed(xs) == list(xs)


def test_list_clear_emits_removes(engine):
    xs = CascadeList(engine, [1, 2])
    xs.clear()
    assert [d["action"] for d in diffs(xs)[-2:]] == ["remove", "remove"]
    assert list(xs) == []


# --- CascadeSet ---


def test_set_diff_format(engine):
    s = CascadeSet(engine)
    s.add(1)
    s.remove(1)
    assert diffs(s) == [
        {"action": "add", "value": 1, "rev": 1},
        {"action": "remove", "value": 1, "rev": 2},
    ]


def test_set_duplicate_add_and_absent_discard_emit_nothing(engine):
    s = CascadeSet(engine, {1})
    before = len(diffs(s))
    s.add(1)
    s.discard(99)
    assert len(diffs(s)) == before


def test_set_remove_missing_raises_before_emitting(engine):
    s = CascadeSet(engine)
    with pytest.raises(KeyError):
        s.remove(5)
    assert diffs(s) == []


def test_set_bulk_updates(engine):
    s = CascadeSet(engine, {1, 2})
    s.update({2, 3}, [4])
    assert s == {1, 2, 3, 4}
    s.difference_update({1, 99})
    assert s == {2, 3, 4}
    s.intersection_update({3, 4, 5})
    assert s == {3, 4}
    s.symmetric_difference_update({4, 6})
    assert s == {3, 6}
    assert replayed(s) == {3, 6}


def test_set_inplace_operators_and_pop(engine):
    s = CascadeSet(engine, {1})
    s |= {2}
    s -= {1}
    s &= {2, 3}
    s ^= {2, 5}
    assert s == {5}
    assert s.pop() == 5
    assert s == set()
    assert replayed(s) == set()


# --- CascadeDict ---


def test_dict_diff_format(engine):
    d = CascadeDict(engine)
    d["k"] = 1
    del d["k"]
    assert diffs(d) == [
        {"action": "upsert", "key": "k", "value": 1, "rev": 1},
        {"action": "remove", "key": "k", "value": 1, "rev": 2},
    ]


def test_dict_update_pop_popitem_setdefault_clear(engine):
    d = CascadeDict(engine, {"a": 1})
    d.update({"b": 2}, c=3)
    d.update([("d", 4)])
    assert d.pop("a") == 1
    assert d.pop("missing", "dflt") == "dflt"
    with pytest.raises(KeyError):
        d.pop("missing")
    assert d.setdefault("b", 99) == 2
    assert d.setdefault("e", 5) == 5
    key, _ = d.popitem()
    assert key == "e"
    d |= {"f": 6}
    assert replayed(d) == dict(d)
    d.clear()
    assert replayed(d) == {}


def test_dict_views_delegate_like_builtin_views(engine):
    d = CascadeDict(engine, {"a": 1, "b": 2})
    assert list(d.keys()) == ["a", "b"]
    assert list(d.values()) == [1, 2]
    assert list(d.items()) == [("a", 1), ("b", 2)]
    assert "a" in d.keys()
    assert 2 in d.values()
    assert len(d.items()) == 2
    assert "cascade_keys" in repr(d.keys())


# --- read tracking: plain reads inside queries record dependencies ---


@pytest.mark.parametrize(
    "read",
    [
        lambda xs: sum(1 for _ in iter(xs)),
        lambda xs: len(xs),
        lambda xs: xs[0],
        lambda xs: 1 in xs,
        lambda xs: xs == [1],
        lambda xs: xs.copy(),
    ],
    ids=["iter", "len", "getitem", "contains", "eq", "copy"],
)
def test_list_plain_reads_invalidate_queries(engine, read):
    xs = CascadeList(engine, [1])
    runs = []

    @engine.query(incremental=False)
    def q():
        runs.append(1)
        read(xs)
        return len(runs)

    q()
    q()
    assert len(runs) == 1
    xs.append(2)
    q()
    assert len(runs) == 2


def test_set_and_dict_plain_reads_invalidate_queries(engine):
    s = CascadeSet(engine, {1})
    d = CascadeDict(engine, {"a": 1})
    runs = []

    @engine.query(incremental=False)
    def q():
        runs.append(1)
        return (
            len(s),
            1 in s,
            sorted(iter(s)),
            len(d),
            d["a"],
            d.get("a"),
            "a" in d,
            list(iter(d)),
        )

    q()
    q()
    assert len(runs) == 1
    s.add(2)
    q()
    assert len(runs) == 2
    d["b"] = 2
    q()
    assert len(runs) == 3


def test_reads_outside_queries_do_not_touch_engine(engine):
    xs = CascadeList(engine, [1, 2])
    rev = engine.revision
    assert len(xs) == 2 and xs[0] == 1 and list(xs) == [1, 2]
    assert engine.revision == rev


def test_mutations_inside_transaction_batch_invalidation(engine):
    xs = CascadeList(engine, [1])
    rev = engine.revision
    with engine.transaction():
        xs.append(2)
        xs.append(3)
        assert engine.revision == rev
    assert engine.revision == rev + 1
    assert list(xs) == [1, 2, 3]


def test_collection_name_rejects_nul(engine):
    with pytest.raises(ValueError):
        CascadeList(engine, name="bad\x00name")


# --- property: materialized contents always equal a replay of the diff log ---

_list_ops = st.lists(
    st.one_of(
        st.tuples(st.just("append"), st.integers(-5, 5)),
        st.tuples(st.just("insert"), st.integers(-4, 8), st.integers(-5, 5)),
        st.tuples(st.just("setitem"), st.integers(0, 6), st.integers(-5, 5)),
        st.tuples(st.just("pop"), st.integers(-3, 6)),
        st.tuples(st.just("delslice"), st.integers(0, 4), st.integers(0, 6)),
        st.tuples(st.just("sort")),
        st.tuples(st.just("reverse")),
        st.tuples(st.just("clear")),
    ),
    max_size=25,
)


@settings(max_examples=60, deadline=None)
@given(ops=_list_ops)
def test_list_log_replay_matches_contents(ops):
    engine = Engine()
    xs = CascadeList(engine, [0])
    mirror = [0]
    for op in ops:
        try:
            if op[0] == "append":
                xs.append(op[1])
                mirror.append(op[1])
            elif op[0] == "insert":
                xs.insert(op[1], op[2])
                mirror.insert(op[1], op[2])
            elif op[0] == "setitem":
                mirror[op[1]] = op[2]
                xs[op[1]] = op[2]
            elif op[0] == "pop":
                mirror.pop(op[1])
                xs.pop(op[1])
            elif op[0] == "delslice":
                del mirror[op[1] : op[2]]
                del xs[op[1] : op[2]]
            elif op[0] == "sort":
                xs.sort()
                mirror.sort()
            elif op[0] == "reverse":
                xs.reverse()
                mirror.reverse()
            else:
                xs.clear()
                mirror.clear()
        except IndexError:
            continue
        assert list(xs) == mirror
        assert replayed(xs) == mirror


_dict_ops = st.lists(
    st.one_of(
        st.tuples(st.just("set"), st.integers(0, 5), st.integers(-5, 5)),
        st.tuples(st.just("del"), st.integers(0, 5)),
        st.tuples(st.just("pop"), st.integers(0, 5)),
        st.tuples(st.just("setdefault"), st.integers(0, 5), st.integers(-5, 5)),
        st.tuples(st.just("clear")),
    ),
    max_size=25,
)


@settings(max_examples=60, deadline=None)
@given(ops=_dict_ops)
def test_dict_log_replay_matches_contents(ops):
    engine = Engine()
    d = CascadeDict(engine, {0: 0})
    mirror = {0: 0}
    for op in ops:
        if op[0] == "set":
            d[op[1]] = op[2]
            mirror[op[1]] = op[2]
        elif op[0] == "del":
            if op[1] in mirror:
                del d[op[1]]
                del mirror[op[1]]
        elif op[0] == "pop":
            assert d.pop(op[1], None) == mirror.pop(op[1], None)
        elif op[0] == "setdefault":
            assert d.setdefault(op[1], op[2]) == mirror.setdefault(op[1], op[2])
        else:
            d.clear()
            mirror.clear()
        assert dict(d) == mirror
        assert list(d) == list(mirror)
        assert replayed(d) == mirror


_set_ops = st.lists(
    st.one_of(
        st.tuples(st.just("add"), st.integers(0, 6)),
        st.tuples(st.just("discard"), st.integers(0, 6)),
        st.tuples(st.just("update"), st.sets(st.integers(0, 6), max_size=4)),
        st.tuples(st.just("difference_update"), st.sets(st.integers(0, 6), max_size=4)),
        st.tuples(
            st.just("symmetric_difference_update"),
            st.sets(st.integers(0, 6), max_size=4),
        ),
        st.tuples(st.just("clear")),
    ),
    max_size=25,
)


@settings(max_examples=60, deadline=None)
@given(ops=_set_ops)
def test_set_log_replay_matches_contents(ops):
    engine = Engine()
    s = CascadeSet(engine, {0})
    mirror = {0}
    for op in ops:
        if op[0] == "clear":
            s.clear()
            mirror.clear()
        else:
            getattr(s, op[0])(*op[1:])
            getattr(mirror, op[0])(*op[1:])
        assert set(s) == mirror
        assert replayed(s) == mirror
