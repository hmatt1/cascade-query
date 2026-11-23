from __future__ import annotations


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
