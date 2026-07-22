from __future__ import annotations

import pytest

from cascade import CascadeDict, CascadeList, CascadeSet, Engine, PersistentCacheError


@pytest.fixture
def cache_dir(tmp_path):
    return tmp_path / "cache"


def make_engine(cache_dir):
    return Engine(cache_dir=cache_dir)


def test_named_collections_restore_across_engines(cache_dir):
    e1 = make_engine(cache_dir)
    xs = CascadeList(e1, [1, 2], name="xs")
    tags = CascadeSet(e1, {"a"}, name="tags")
    cfg = CascadeDict(e1, {"k": 1}, name="cfg")
    xs.append(3)
    tags.add("b")
    cfg["k2"] = 2
    e1.shutdown()

    e2 = make_engine(cache_dir)
    assert list(CascadeList(e2, name="xs")) == [1, 2, 3]
    assert set(CascadeSet(e2, name="tags")) == {"a", "b"}
    assert dict(CascadeDict(e2, name="cfg")) == {"k": 1, "k2": 2}
    e2.shutdown()


def test_restored_list_keeps_uid_continuity(cache_dir):
    e1 = make_engine(cache_dir)
    CascadeList(e1, ["a", "b"], name="items")
    e1.shutdown()

    e2 = make_engine(cache_dir)
    xs2 = CascadeList(e2, name="items")
    xs2[0] = "A"
    log = xs2._core.log
    update = log.entries[-1]
    replay = log.contents_at(log.head)
    assert [v for _, v in replay] == ["A", "b"]
    assert update["action"] == "update"
    e2.shutdown()


def test_initial_argument_ignored_when_disk_state_exists(cache_dir):
    e1 = make_engine(cache_dir)
    CascadeList(e1, [1, 2, 3], name="xs")
    e1.shutdown()

    e2 = make_engine(cache_dir)
    xs = CascadeList(e2, [99], name="xs")
    assert list(xs) == [1, 2, 3]
    e2.shutdown()


def test_cross_session_memo_verifies_against_collection_head(cache_dir):
    runs = []

    def body():
        runs.append(1)
        return sum(i * i for i in items)  # noqa: F821

    e1 = make_engine(cache_dir)
    items = CascadeList(e1, [1, 2, 3], name="items")
    q1 = e1.query(body)
    assert q1() == 14
    assert len(runs) == 1
    e1.shutdown()

    e2 = make_engine(cache_dir)
    items = CascadeList(e2, name="items")
    q2 = e2.query(body)
    assert q2() == 14
    assert len(runs) == 1  # served from disk memo, head unchanged
    items.append(4)
    assert q2() == 30
    assert len(runs) == 2
    e2.shutdown()


def test_compaction_squashes_log_tail(cache_dir):
    e = make_engine(cache_dir)
    xs = CascadeList(e, name="xs", compact_every=5)
    for i in range(12):
        xs.append(i)
    snapshot, tail = e._disk.collection_load("xs")
    assert snapshot is not None
    assert snapshot["base_rev"] >= 5
    assert len(tail) < 12
    assert all(rev > snapshot["base_rev"] for rev, _ in tail)

    e.shutdown()
    e2 = make_engine(cache_dir)
    assert list(CascadeList(e2, name="xs")) == list(range(12))
    e2.shutdown()


def test_manual_compact(cache_dir):
    e = make_engine(cache_dir)
    xs = CascadeList(e, [1], name="xs", compact_every=10_000)
    xs.append(2)
    xs.compact()
    snapshot, tail = e._disk.collection_load("xs")
    assert snapshot is not None and tail == []
    xs.append(3)
    _, tail = e._disk.collection_load("xs")
    assert len(tail) == 1
    e.shutdown()

    e2 = make_engine(cache_dir)
    assert list(CascadeList(e2, name="xs")) == [1, 2, 3]
    e2.shutdown()


def test_unnamed_collections_do_not_persist(cache_dir):
    e = make_engine(cache_dir)
    xs = CascadeList(e, [1, 2])
    xs.append(3)
    name = xs._core.name
    snapshot, tail = e._disk.collection_load(name)
    assert snapshot is None and tail == []
    e.shutdown()


def test_named_collections_without_cache_dir_stay_in_memory():
    e = Engine()
    xs = CascadeList(e, [1], name="xs")
    xs.append(2)
    assert list(xs) == [1, 2]


def test_unserializable_value_raises_persistent_cache_error(cache_dir):
    e = make_engine(cache_dir)
    xs = CascadeList(e, name="xs")
    with pytest.raises(PersistentCacheError):
        xs.append(object())
    assert list(xs) == []  # failed mutation left no trace
    xs.append(1)
    assert list(xs) == [1]

    s = CascadeSet(e, {1}, name="s")
    with pytest.raises(PersistentCacheError):
        s.add(object())
    assert set(s) == {1}

    d = CascadeDict(e, {"a": 1}, name="d")
    with pytest.raises(PersistentCacheError):
        d["b"] = object()
    assert dict(d) == {"a": 1}
    e.shutdown()


def test_unserializable_in_unnamed_collection_is_fine():
    e = Engine()
    xs = CascadeList(e)
    marker = object()
    xs.append(marker)
    assert xs[0] is marker


def test_kind_mismatch_on_restore_raises(cache_dir):
    e1 = make_engine(cache_dir)
    CascadeList(e1, [1], name="thing")
    e1.shutdown()

    e2 = make_engine(cache_dir)
    with pytest.raises(PersistentCacheError):
        CascadeSet(e2, name="thing")
    e2.shutdown()


def test_clear_disk_cache_wipes_collections(cache_dir):
    e1 = make_engine(cache_dir)
    CascadeList(e1, [1, 2], name="xs")
    e1.clear_disk_cache()
    snapshot, tail = e1._disk.collection_load("xs")
    assert snapshot is None and tail == []
    e1.shutdown()

    e2 = make_engine(cache_dir)
    assert list(CascadeList(e2, [7], name="xs")) == [7]
    e2.shutdown()


def test_two_engines_same_dir_share_collection_state(cache_dir):
    e1 = make_engine(cache_dir)
    e2 = make_engine(cache_dir)
    a = CascadeList(e1, [1], name="shared")
    a.append(2)
    b = CascadeList(e2, name="shared")
    assert list(b) == [1, 2]
    e1.shutdown()
    e2.shutdown()


def test_pipeline_over_restored_collection_is_incremental(cache_dir, tmp_path):
    program = tmp_path / "prog.py"
    program.write_text(
        "def make(engine, items, spy):\n"
        "    @engine.query\n"
        "    def total():\n"
        "        return sum(spy(i) for i in items)\n"
        "    return total\n"
    )
    import importlib.util

    spec = importlib.util.spec_from_file_location("prog", program)
    prog = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(prog)

    e1 = make_engine(cache_dir)
    items = CascadeList(e1, [1, 2, 3], name="items")
    seen1 = []
    q1 = prog.make(e1, items, lambda i: seen1.append(i) or i)
    assert q1() == 6
    assert seen1 == [1, 2, 3]
    e1.shutdown()

    e2 = make_engine(cache_dir)
    items2 = CascadeList(e2, name="items")
    seen2 = []
    q2 = prog.make(e2, items2, lambda i: seen2.append(i) or i)
    assert q2() == 6
    items2.append(10)
    assert q2() == 16
    assert seen2.count(10) == 1
    e2.shutdown()
