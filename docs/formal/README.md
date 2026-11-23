# Lightweight formal model: core engine invariants

This directory contains a small TLA+ model for high-assurance sanity checks of
core correctness semantics. It is intentionally abstract (not a line-by-line
implementation model), but it tracks the state transitions that matter for:

- snapshot consistency
- dependency validity / red-green behavior
- cancellation epoch monotonicity

## Files

- `CascadeCore.tla` - state machine model and invariants
- `CascadeCore.cfg` - TLC constants and invariant check configuration

## What the model covers

1. **Snapshot consistency**
   - Snapshot reads (`ReadSnapshot`) return the value pinned at the captured
     revision (`TakeSnapshot` + `ChooseAt(snapRev)`).
   - Later writes do not retroactively change snapshot results.

2. **Dependency validity / red-green agreement**
   - `activeDep` must always match the current branch selector (`mode`).
   - `depObservedChangedAt` must track the changed-at revision of the active
     dependency branch.
   - `chooseVal` must agree with the selected active dependency value.

3. **Cancellation epoch monotonicity**
   - Every write action bumps `cancelEpoch`.
   - `cancelEpoch` is checked to never decrease (`CancelEpochMonotone`).

4. **Bounded metadata sanity checks**
   - Changed-at markers (`modeChangedAt`, `leftChangedAt`, `rightChangedAt`,
     `depObservedChangedAt`, `chooseChangedAt`) must stay bounded by current
     revision.
   - This catches regressions where version/changed-at metadata could jump ahead
     of `rev` under write transitions.

## Running TLC locally

Install any TLA+ runtime you prefer (Toolbox or `tla2tools.jar`), then run:

```bash
java -cp tla2tools.jar tlc2.TLC docs/formal/CascadeCore.tla -config docs/formal/CascadeCore.cfg
```

Optional flags for faster local iteration:

```bash
java -cp tla2tools.jar tlc2.TLC -workers auto -deadlock docs/formal/CascadeCore.tla -config docs/formal/CascadeCore.cfg
```

## Notes

- `MaxRev` and `ValueSet` are bounded in `CascadeCore.cfg` to keep model
  checking fast.
- `CascadeCore.cfg` enables `ChangedAtBoundedByRevision` in addition to the
  original snapshot/dependency/cancellation invariants.
- This model is a safety net for invariants, not a full proof of total engine
  behavior.
