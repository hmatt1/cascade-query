# Changelog

Releases before 0.4.0 were published from every push to `main` without
notes; `docs/guarantees.md` records the defects fixed in them (D1–D37).
Defects fixed since are listed under their release (D38 on).

## 0.4.5

### Changed

- **A comprehension whose elements are values that can be changed in place
  is no longer incremental.** `[{"id": x} for x in xs]`, `[[x] for x in xs]`
  and the like, under `list`, `set`, `dict`, `sorted`, `min` or `max`, now
  run their element for every element on every run, as plain Python does.
  Handing the same objects to the next run let a body that changed one see
  it changed (D62). Elements that are ints, floats, strings, bytes, `None`,
  or tuples and frozensets of those stay incremental.
- **Early cutoff compares effects too.** A query that runs again to an equal
  value with other effects counts as changed, so its callers collect the
  new effects (D55). Stored results whose query dependencies push effects
  are computed once more after the upgrade.
- **Crashes and several processes on one `cache_dir` are out of scope, by
  design.** After a crash, clear the cache. Use one process per directory;
  engines in one process may share it.

### Fixed

- **Cancelling the task that ran a query cancelled every caller waiting for
  it (D52)**, and a caller that joined a run blocked on another thread's
  event loop got that loop's `EventLoopBlockedError` (D53). Waiters now run
  the query themselves.
- **A fixed point could settle on a wrong value (D54)** when something the
  cycle read from outside changed while it iterated, such as a TTL result
  dropped and run again. The iteration now starts again from the guess.
- **Callers replayed stale effects after a dependency ran again to an equal
  value (D55)**, in memory and from disk.
- **A caller that joined a run failing with an uncached error lost the run's
  dependencies and effects (D56)**, so its own result was served after the
  input changed.
- **Threads entering a cycle of async queries from both ends hung (D57).**
- **A submitted call that raised after an input changed returned the stale
  error instead of `QueryCancelled` (D58).**
- **A result that read a default kept meanwhile at a newer revision was not
  stored on disk (D59).**
- **Rewritten comprehensions differed from Python in three cases:** lambdas
  made per element each kept their own loop value (D60); an element that
  appended to a local list ran for changed elements only (D61); a stage that
  made a NaN handed out one NaN object twice (D63). The first two are now
  left as written, the third rebuilds on every call.

## 0.4.4

### Changed

- **Collection format 3: named collections written by 0.4.3 and earlier
  are not read.** Every set element and dict entry now has an id on disk,
  as list elements had. Opening a `cache_dir` that holds named collections
  in format 1 or 2 raises `PersistentCacheError` and changes nothing;
  delete the directory, or its collections, to start again. A directory
  with no named collections is restamped, and its cached results are kept.

### Fixed

- **A NaN key removed from a named `CascadeDict` or `CascadeSet` came back
  after a restart (D50).** The replay of the log on disk matched the removal
  by equality, and a NaN equals nothing. Diffs now name the entry by id.
  Two different NaN keys stay two keys.
- **Setting an input to its default could count as a change after a result
  was reused from disk (D51).** The check of the stored result read the
  default without keeping it, so a later `set` to the same value moved the
  revision and cancelled submitted calls. Values were never wrong.

## 0.4.3

### Fixed

- **A pipeline whose stage called a query another thread was running could
  hang both threads for good (D42).** The stage waited for the run while
  holding the pipelines' lock, and the run waited for that lock. A thread
  inside a pipeline now runs the query itself instead of waiting.
- **A fixed-point member did not expire with its head (D43).** After the
  TTL of a query the cycle read had elapsed, a direct read of a member
  still gave its old value. Members now take the head's expiry.
- **Dropping a fixed-point head's result left its members cached (D44).**
  After an LRU drop, `sweep_unaccessed` or a direct drop, a cycle with a
  TTL query in it could return one step of the iteration instead of the
  fixed point. Dropping a head now drops its members.
- **A rewritten pipeline over `d.items()` took the key from the update
  (D45).** After `d[1.0] = "b"` where `1` is a key, `dict(d)` inside a
  query gave `{1.0: "b"}` where Python gives `{1: "b"}`.
- **A pipeline that captured a NaN mixed NaN objects across runs (D46).**
  `set([m for _ in d.values()])` with `m = float("nan")` could hold two
  NaNs. Such a pipeline now rebuilds on every run.
- **`compact()` on a named collection could delete another writer's change
  on disk (D47).** It read the head outside the collection's lock; it now
  holds the lock until the snapshot is written.
- **`compact()` while a transaction held the collection wrote the block's
  uncommitted changes to disk (D48).** They survived a rollback, and a
  commit wrote them twice. It now snapshots only what is committed, and a
  collection with nothing committed yet gets no record.
- **The fix for D41 could still lose another engine's collection (D49).**
  The check for an existing record and the write of the empty one were two
  disk operations; they are now one (`collection_stamp`).

### Known issue

- A NaN key removed from a named `CascadeDict` or `CascadeSet` comes back
  after a restart (D50).

## 0.4.2

### Changed

- **A `memoize=False` fixed-point head called again no longer iterates its
  whole cycle.** It keeps no value, so every call runs it, and each run used
  to drop the cycle's members and iterate from the guess. When nothing the
  cycle read has changed, the head now runs once over its cached members:
  for a three-query cycle, one run of the head where there were six of each
  query. If that run reaches the head again or gives another value, it falls
  back to iterating from the guess. Values are unchanged.

## 0.4.1

### Fixed

- **A query under a wrapping decorator kept its stored result after its
  body was edited (D40).** With `cache_dir`, a query decorated with a
  `functools.wraps` wrapper was hashed by the wrapper's code only, so an
  edit to the body between runs served the old result. Every wrapped layer
  is now part of the hash. On upgrade, stored results of decorated queries
  are computed once more; other queries keep theirs.
- **The fix for D38 in 0.4.0 could lose another engine's collection data
  (D41).** A named collection created in a transaction writes its record
  at commit; if another engine had written and compacted that collection
  in the meantime, the empty record replaced its contents on disk. The
  record is now written only if the name has none, and a record of another
  kind fails the commit.

## 0.4.0

This release turns several ways of getting a silently wrong answer into
errors, and makes stored results follow the code a query uses. Code that
followed the rules in the README keeps working, except for the renamed
option below.

### Changed

- **`incremental=` is now `incremental_per_element=`**, on `Engine(...)` and
  `@engine.query(...)`. The old name is gone. The option never turned off
  incremental computation: it decides whether a comprehension or reducer
  over a Cascade collection redoes only the changed elements or all of them.
- **Stored results follow the helpers and constants a query uses.** With
  `cache_dir`, editing a helper function a query calls (or one that helper
  calls), a class it names, a module constant, a closure value or a default
  argument now recomputes the stored results that used it. Before, the old
  result was served until `version=` was bumped. `version=` is still needed
  for what the hash cannot follow: an upgraded library, a method called on
  an object, a mutable global.
- **On upgrade, stored results of queries that use helpers or constants are
  computed once more**, because the hash they are stored under now covers
  those. Nothing is served wrongly in the meantime, and named collections
  are untouched.

### Now an error

- **A query that returns a generator, `map`, `zip` or other one-shot
  iterator** raises `UnencodableValueError`. Every caller after the first
  used to get it used up. `memoize=False` queries may still return one.
- **An ordinary object as an argument**, including `self` of a method query:
  an object of a class without its own `__eq__` and `__hash__` is keyed by
  identity, so changing one of its attributes left old results in place. It
  now raises `TypeError` before the body runs. Use a frozen dataclass or a
  `NamedTuple`. Classes, functions, modules and a bare `object()` sentinel
  are still accepted.

### Fixed

- **A named collection created in a transaction that rolled back left a
  record on disk (D38).** A later engine restored it empty and ignored the
  initial contents it was given, and refused another kind under the name.
  The record is now written at commit, or at the collection's first change
  after a rollback.
- **On a free-threaded build, `engine.submit` could return an old value
  (D39).** A pool thread started inside an async query kept that query's
  context, and a call submitted to it later ran at the query's old revision
  and could not be cancelled. Each submitted call now runs in a context of
  its own.

### Added

- **`MutatedValueError`, `engine.check_mutations()` and
  `Engine(check_mutations=True)`.** Queries and inputs hand out the stored
  object, not a copy. `check_mutations()` finds cached results and input
  values that were changed in place; a changed result is dropped and
  computed again. With `check_mutations=True` each query also checks the
  values it read when its body finishes. Off by default, since it costs one
  digest of each value checked: use it in tests and development.
- **A same-value `set()` repairs an input changed in place**: the given
  object replaces the changed one, without a new revision.
- **README: "The rules your code must follow"** and a table of how well each
  part of the library is tested; docstrings for `Engine` and `engine.query`.
