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


@dataclass(frozen=True)
class InputVersion:
    revision: int
    changed_at: int
    value_hash: str
    value: Any


@dataclass(frozen=True)
class Snapshot:
    revision: int


class InFlight:
    """Dedup slot for a query being computed by one owner thread or task.

    ``future`` stays ``None`` until a second caller asks for the same key at the
    same revision; only then is a ``concurrent.futures.Future`` allocated.
    """

    __slots__ = ("future",)

    def __init__(self) -> None:
        self.future: concurrent.futures.Future[MemoEntry] | None = None
