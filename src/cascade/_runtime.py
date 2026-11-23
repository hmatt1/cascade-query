from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any

from ._state import QueryKey, Snapshot


UNSET = object()


@dataclass
class RuntimeFrame:
    key: QueryKey
    deps: dict[QueryKey, int] = field(default_factory=dict)
    effects: dict[str, list[Any]] = field(default_factory=lambda: defaultdict(list))
    # Fixed points. ``cycle_guess`` is set once this query has been called
    # again from beneath itself; ``is_cycle_root`` says whether the current
    # run read the guess. ``cycle_heads`` are the running queries whose
    # guesses this run has read, directly or through provisional results.
    cycle_guess: Any = UNSET
    is_cycle_root: bool = False
    cycle_heads: set[QueryKey] | None = None
    # The runtime this frame runs in (for async, the one made for it).
    runtime: RuntimeState | None = field(default=None, repr=False, compare=False)
    # Earliest expiry among the TTL-bound results this frame has read.
    expires_at: float | None = None


@dataclass
class RuntimeState:
    snapshot: Snapshot
    stack: list[RuntimeFrame]
    root_effects: dict[str, list[Any]] | None
    staged_root_effects: dict[str, list[Any]]
    cancel_epoch: int | None
    # True when the caller chose the snapshot; such reads never materialize
    # an input default into the store.
    snapshot_explicit: bool
    is_async: bool = False
    # Revisions this runtime pinned in the store; released when it ends.
    pins: list[int] | None = None
    # Provisional results of fixed-point cycles being iterated in this
    # runtime, by query key. See "fixed points" in the evaluator.
    provisional: dict[QueryKey, Any] | None = None
    # Set on a shadow runtime (one that brings a dependency up to date for
    # a verification): the runtime that started the verification.
    outer: RuntimeState | None = None
