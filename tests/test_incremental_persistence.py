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


@pytest.mark.parametrize("backend", ["mdbx", "sqlite"])
@pytest.mark.parametrize("compact_before_last_change", [False, True])
def test_item_added_to_a_reopened_list_gets_a_uid_of_its_own(
    tmp_path, backend, compact_before_last_change
):
    # A change names its item by uid, and a later session rebuilds the list by
    # replaying the stored changes. So an item added after reopening must not
    # get the uid of an item stored since the last compaction: a change to
    # the new item would then land on the old one in the replay.
    def open_engine() -> Engine:
        return Engine(cache_dir=tmp_path / "cache", cache_backend=backend)

    with open_engine() as engine:
        xs = CascadeList(engine, ["a", "b"], name="xs", compact_every=10_000)
        if compact_before_last_change:
            xs.compact()
        xs.append("c")

    with open_engine() as engine:
        # What this test is about: there are changes that are in no snapshot.
        _, tail = engine._disk.collection_load("xs")
        assert tail
        xs = CascadeList(engine, name="xs", compact_every=10_000)
        xs.append("d")
        xs[3] = "D"
        assert list(xs) == ["a", "b", "c", "D"]

    with open_engine() as engine:
        assert list(CascadeList(engine, name="xs")) == ["a", "b", "c", "D"]


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


def test_clear_disk_cache_keeps_collections(cache_dir):
    # Was test_clear_disk_cache_wipes_collections. Clearing the cache used to
    # delete collection logs too, which lost data nothing can recompute and
    # left live collections restoring as fragments (D11).
    e1 = make_engine(cache_dir)
    CascadeList(e1, [1, 2], name="xs")
    e1.clear_disk_cache()
    snapshot, tail = e1._disk.collection_load("xs")
    assert snapshot is not None
    assert [diff["value"] for _rev, diff in tail] == [1, 2]
    e1.shutdown()

    e2 = make_engine(cache_dir)
    assert list(CascadeList(e2, [7], name="xs")) == [1, 2]
    e2.shutdown()


@pytest.mark.parametrize("backend", ["mdbx", "sqlite"])
def test_clear_disk_cache_drops_results_but_not_collections(tmp_path, backend):
    def session(clear: bool) -> tuple[list, int, int]:
        engine = Engine(cache_dir=tmp_path / "cache", cache_backend=backend)
        runs = [0]
        try:
            xs = CascadeList(engine, [1, 2, 3], name="xs", compact_every=4)
            ys = CascadeDict(engine, {"a": 1}, name="ys")
            zs = CascadeSet(engine, [5], name="zs")

            @engine.query(incremental=False)
            def total() -> int:
                runs[0] += 1
                return sum(list(xs)) + sum(ys.values()) + sum(zs)

            value = total()
            if clear:
                engine.flush_disk()
                engine.clear_disk_cache()
                # Mutations after the clear, enough to cross a compaction.
                for i in range(6):
                    xs.append(10 + i)
                del xs[0]
                ys["b"] = 2
                zs.add(6)
            return [list(xs), dict(ys.items()), sorted(zs)], value, runs[0]
        finally:
            engine.shutdown()

    session(clear=False)
    cleared_state, _value, _runs = session(clear=True)
    assert cleared_state[0] == [2, 3, 10, 11, 12, 13, 14, 15]
    # The next run restores exactly what the collections held at shutdown,
    # and recomputes the result the clear dropped.
    restored_state, value, runs = session(clear=False)
    assert restored_state == cleared_state
    assert value == sum(cleared_state[0]) + 3 + 11
    assert runs == 1
    # Nothing was cleared since, so this run reuses the result.
    assert session(clear=False)[2] == 0


def test_clear_disk_cache_inside_a_transaction_keeps_pending_collection_writes(
    cache_dir,
):
    e1 = make_engine(cache_dir)
    xs = CascadeList(e1, [1], name="xs")
    with e1.transaction():
        xs.append(2)
        e1.clear_disk_cache()
        xs.append(3)
    e1.shutdown()

    e2 = make_engine(cache_dir)
    assert list(CascadeList(e2, name="xs")) == [1, 2, 3]
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


# --- a cache that is full (mdbx: cache_map_size) ------------------------------

_SMALL_MAP = 1 << 22
_CHUNK = 256 * 1024
_FULL = "is full; pass a larger cache_map_size"


def _chunk(i: int) -> bytes:
    return bytes([i]) * _CHUNK


def test_append_that_does_not_fit_the_cache_is_refused_and_changes_nothing(
    cache_dir,
):
    e = Engine(cache_dir=cache_dir, cache_backend="mdbx", cache_map_size=_SMALL_MAP)
    xs = CascadeList(e, name="xs", compact_every=10_000)

    @e.query
    def count() -> int:
        return len(xs)

    accepted = 0
    with pytest.raises(PersistentCacheError, match=_FULL):
        for i in range(64):  # 16 MiB for a map of 4
            xs.append(_chunk(i))
            accepted += 1
    assert accepted >= 2
    # The refused chunk is nowhere: not in the list, not in what a query sees.
    assert list(xs) == [_chunk(i) for i in range(accepted)]
    assert count() == accepted
    e.shutdown()

    # Nor on disk. With the larger map the message asks for, the directory
    # restores what was accepted and takes the chunk that was refused.
    e2 = Engine(cache_dir=cache_dir, cache_backend="mdbx", cache_map_size=1 << 26)
    ys = CascadeList(e2, name="xs")
    assert list(ys) == [_chunk(i) for i in range(accepted)]
    ys.append(_chunk(accepted))
    e2.shutdown()

    e3 = Engine(cache_dir=cache_dir, cache_backend="mdbx", cache_map_size=1 << 26)
    assert list(CascadeList(e3, name="xs")) == [_chunk(i) for i in range(accepted + 1)]
    e3.shutdown()


def test_compaction_that_does_not_fit_the_cache_is_refused_and_keeps_the_log(
    cache_dir,
):
    e = Engine(cache_dir=cache_dir, cache_backend="mdbx", cache_map_size=_SMALL_MAP)
    xs = CascadeList(e, name="xs", compact_every=10_000)
    chunks = [_chunk(i) for i in range(10)]  # 2.5 MiB: fits as a log
    for chunk in chunks:
        xs.append(chunk)
    # The snapshot is a second copy of the contents, written before the log
    # entries it replaces are dropped. There is no room for 2.5 MiB more.
    with pytest.raises(PersistentCacheError, match=_FULL):
        xs.compact()
    assert list(xs) == chunks
    # The log is as it was: it takes another change, and a restart replays it.
    xs.append(b"tail")
    e.shutdown()

    e2 = Engine(cache_dir=cache_dir, cache_backend="mdbx", cache_map_size=_SMALL_MAP)
    assert list(CascadeList(e2, name="xs")) == chunks + [b"tail"]
    e2.shutdown()
