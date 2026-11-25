from __future__ import annotations

from typing import Any


class CycleError(RuntimeError):
    """Raised when a query cycle is detected."""


class CancellationError(RuntimeError):
    """Base cancellation exception for obsolete computations."""


class QueryCancelled(CancellationError):
    """Raised when a running query becomes obsolete."""


class PersistentCacheError(RuntimeError):
    """Raised when the persistent disk cache cannot be provisioned or used."""


class SnapshotExpiredError(RuntimeError):
    """Raised when reading at a revision whose input history was already trimmed.

    History is kept for every revision still in use: snapshots returned by
    ``Engine.snapshot()`` until they are released, and calls that are still
    running. A snapshot used after ``release()`` (or one built by hand from a
    revision number) can refer to history that is gone.
    """


class StaleHandleError(RuntimeError):
    """Raised when a query or input handle is used after being replaced.

    A handle is replaced when a different function is registered under its
    name: the module was reloaded with changed code, or several functions
    made from one body (a factory, a loop, lambdas) were registered without
    distinct ``name=`` values. The old handle would run code the engine no
    longer knows about and store its results where the new handle reads
    them, so using it is an error. Never cached.
    """


class UnencodableValueError(TypeError):
    """Raised when a query result or an input value cannot be encoded.

    Cascade encodes every result and input value to tell whether it changed,
    with or without ``cache_dir``. The message names the query or input,
    the path to the offending part of the value (``result[2]['k'].field``)
    and its type. A ``TypeError``, as the codec's own error was. Never
    cached: a query whose result cannot be encoded runs again on every call.
    """


class MutatedValueError(RuntimeError):
    """Raised when a cached result or an input value was changed in place.

    A query returns its cached object itself, not a copy, and an input
    returns the value it was given. Code that changes such an object (a
    caller appending to a returned list, a query changing the dict another
    query returned, ``cfg()["k"] = 1`` instead of ``cfg.set(...)``) changes
    what every later reader gets, and Cascade never learns of it. A query
    checks, when its body finishes, that every value it read still has the
    digest it had when it was stored; ``Engine.check_mutations()`` checks all
    of them. A changed query result is dropped, so the next call computes it
    again; a changed input value stays wrong until ``set()`` gives the input
    a new one. Never cached.
    """

    def __init__(self, message: str, keys: tuple[Any, ...] = ()) -> None:
        super().__init__(message)
        self.keys = keys


class CollectionBypassedError(RuntimeError):
    """Raised when a collection's storage was changed behind its back.

    ``CascadeList``, ``CascadeSet`` and ``CascadeDict`` record every change
    made through their own methods. C code that writes a list, set or dict
    subclass's storage directly (``heapq.heappush(xs, v)``,
    ``heapq.heappop(xs)``, or a builtin's method called by hand, such as
    ``list.append(xs, v)``) records nothing, so queries could never see the
    change. The next use of the collection notices that its size no longer
    matches what it recorded, undoes the change (the collection holds what
    it held before) and raises this. Never cached.
    """


class EventLoopBlockedError(RuntimeError):
    """Raised instead of blocking an asyncio event loop for ever.

    Synchronous code running on the thread of an event loop (a sync query
    called from a coroutine, say) cannot wait for an ``async def`` query:
    not for one that has to run (that needs the loop, which the wait
    blocks), and not for one already running as a task on that loop. Await
    the async query from async code, or make the synchronous call from
    another thread (``await asyncio.to_thread(...)``). Never cached.
    """
