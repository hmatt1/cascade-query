from __future__ import annotations


class CycleError(RuntimeError):
    """Raised when a query cycle is detected."""


class CancellationError(RuntimeError):
    """Base cancellation exception for obsolete computations."""


class QueryCancelled(CancellationError):
    """Raised when a running query becomes obsolete."""


class PersistentCacheError(RuntimeError):
    """Raised when the persistent disk cache cannot be provisioned or used."""
