# Changelog

Releases before 0.4.0 were published from every push to `main` without
notes; `docs/guarantees.md` records the defects fixed in them (D1–D37).
Defects fixed since are listed under their release (D38 on).

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
