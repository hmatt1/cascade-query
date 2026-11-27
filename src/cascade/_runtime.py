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
    # Tracked reads made by this frame, repeated ones included. ``deps``
    # cannot say whether a stretch of code read anything: a second read of
    # a dependency leaves it unchanged. Pipelines compare this counter.
    reads: int = 0
    effects: dict[str, list[Any]] = field(default_factory=lambda: defaultdict(list))
    # Fixed points. ``cycle_guess`` is set once this query has been called
    # again from beneath itself; ``is_cycle_root`` says whether the current
    # run read the guess. ``cycle_heads`` are the running queries whose
    # guesses this run has read, directly or through provisional results.
    cycle_guess: Any = UNSET
    # Digest of ``cycle_guess``, once computed.
    cycle_guess_hash: str | None = None
    # What the last run of this cycle head read from outside the cycle,
    # with the ``changed_at`` each read saw (``Evaluator._next_iteration``).
    cycle_outside: dict[QueryKey, int] | None = None
    is_cycle_root: bool = False
    cycle_heads: set[QueryKey] | None = None
    # The runtime this frame runs in (for async, the one made for it).
    runtime: RuntimeState | None = field(default=None, repr=False, compare=False)
    # Earliest expiry among the TTL-bound results this frame has read.
    expires_at: float | None = None
    # True for a frame run by the async evaluator (its body may await).
    is_async_body: bool = False
    # False while a run that reaches itself through a fixed-point head may
    # still start again from that head (``Evaluator._head_to_enter_through``).
    routed: bool = True
    # A memoize=False fixed-point head whose stored row was verified at this
    # revision runs once on its cached members instead of iterating from the
    # guess: this is that row (``Evaluator._reusable_cycle_locked``).
    reused_cycle: Any = None


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
    # True for a runtime that brings a dependency up to date for a
    # verification (a shadow runtime), and for everything run inside it.
    verifying: bool = False
