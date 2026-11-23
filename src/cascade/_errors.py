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
