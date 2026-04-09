from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any

from ._state import QueryKey, Snapshot


@dataclass
class RuntimeFrame:
    key: QueryKey
    deps: dict[QueryKey, int] = field(default_factory=dict)
    effects: dict[str, list[Any]] = field(default_factory=lambda: defaultdict(list))


@dataclass
class RuntimeState:
    snapshot: Snapshot
    stack: list[RuntimeFrame]
    root_effects: dict[str, list[Any]] | None
    staged_root_effects: dict[str, list[Any]]
    cancel_epoch: int | None
    snapshot_pinned: bool
