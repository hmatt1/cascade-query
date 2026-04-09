from __future__ import annotations

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


@dataclass(frozen=True)
class InputVersion:
    revision: int
    changed_at: int
    value_hash: str
    value: Any


@dataclass(frozen=True)
class Snapshot:
    revision: int
