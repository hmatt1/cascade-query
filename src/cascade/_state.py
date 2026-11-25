from __future__ import annotations

import concurrent.futures
from dataclasses import dataclass
from typing import Any

type QueryKey = tuple[str, str, tuple[Any, ...]]
type InputKey = tuple[str, tuple[Any, ...]]


@dataclass(frozen=True)
class TraceEvent:
    event: str
    key: str
    revision: int
    detail: str
    timestamp: float


@dataclass(frozen=True)
class Dependency:
    key: QueryKey
    observed_changed_at: int


@dataclass
class MemoEntry:
    value: Any
    value_hash: str
    # A stamp that is new every time this query's value changes, and kept
    # when a re-run produces an equal value. Dependents record the stamp they
    # saw and compare it for equality only. It is not a revision: a value can
    # change while the revision stands still (an expired TTL, new code).
    changed_at: int
    verified_at: int
    deps: tuple[Dependency, ...]
    effects: dict[str, tuple[Any, ...]]
    last_access: int
    computed_at_time: float = 0.0
    cycle_nodes: tuple[QueryKey, ...] = ()
    error: BaseException | None = None
    # True while this memo is known valid at the store's newest revision
    # without re-walking its dependencies. Maintained by GraphStore (see
    # "dirty propagation" there); process-local and never persisted.
    head_clean: bool = False
    # Wall-clock time after which this memo must be verified again even at
    # the revision it was verified at: the earliest moment a TTL query it
    # read, directly or through other queries, expires. None when nothing
    # beneath it has a TTL.
    expires_at: float | None = None
    # Set while this is a provisional result inside a fixed-point cycle: the
    # cycle heads whose guesses it was computed from. Such an entry lives in
    # a runtime's overlay, never in the store. None for every stored memo.
    cycle_heads: frozenset[QueryKey] | None = None


@dataclass(frozen=True)
class InputVersion:
    revision: int
    changed_at: int
    value_hash: str
    value: Any


_set_slot = object.__setattr__


class Snapshot:
    """A point-in-time view of the engine's inputs, identified by revision.

    Snapshots from ``Engine.snapshot()`` keep the input history they need
    until they are released: by ``release()``, at the end of a ``with`` block,
    or when the object is garbage-collected. ``Snapshot(revision=n)`` can be
    built by hand; it holds nothing, so reading through it raises
    ``SnapshotExpiredError`` once that history has been trimmed.
    """

    # ``revision`` is a plain slot, not a property: the evaluator reads it
    # several times per node, and a property cost ~12% of a cold compute.
    __slots__ = ("revision", "_finalizer", "__weakref__")
    revision: int

    def __init__(self, revision: int) -> None:
        _set_slot(self, "revision", revision)

    def __setattr__(self, name: str, value: object) -> None:
        if name == "revision":
            raise AttributeError("Snapshot.revision is read-only")
        object.__setattr__(self, name, value)

    def release(self) -> None:
        """Stop keeping this snapshot's history. Safe to call more than once."""
        finalizer = getattr(self, "_finalizer", None)
        if finalizer is None:
            return
        detached = finalizer.detach()
        if detached is not None:
            _, callback, args, _ = detached
            release_now = getattr(callback, "__self__", None)
            if release_now is not None:  # pragma: no branch - always the store's method
                release_now.unpin(*args)

    def __enter__(self) -> Snapshot:
        return self

    def __exit__(self, *exc: object) -> None:
        self.release()

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Snapshot):
            return NotImplemented
        return self.revision == other.revision

    def __hash__(self) -> int:
        return hash((self.revision,))

    def __repr__(self) -> str:
        return f"Snapshot(revision={self.revision})"

    def __reduce__(self) -> tuple[Any, ...]:
        return (Snapshot, (self.revision,))


class InFlight:
    """Dedup slot for a query being computed by one owner thread or task.

    ``future`` stays ``None`` until a second caller asks for the same key at the
    same revision; only then is a ``concurrent.futures.Future`` allocated.
    """

    __slots__ = ("future", "owner", "owner_thread")

    def __init__(self, owner: Any = None, owner_thread: int | None = None) -> None:
        self.future: concurrent.futures.Future[MemoEntry] | None = None
        # Who runs it: a thread id for a synchronous run, else the asyncio
        # task; and the thread it runs on. See Evaluator._enter_wait_locked.
        self.owner = owner
        self.owner_thread = owner_thread
