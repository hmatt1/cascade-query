---- MODULE CascadeCore ----
EXTENDS Naturals

CONSTANTS MaxRev, ValueSet, NullResult

Modes == {"left", "right"}

VARIABLES
    rev,
    cancelEpoch,
    prevCancelEpoch,
    mode,
    leftVal,
    rightVal,
    modeChangedAt,
    leftChangedAt,
    rightChangedAt,
    activeDep,
    depObservedChangedAt,
    chooseVal,
    chooseChangedAt,
    modeHist,
    leftHist,
    rightHist,
    snapRev,
    snapResult

vars ==
    <<rev, cancelEpoch, prevCancelEpoch, mode, leftVal, rightVal, modeChangedAt, leftChangedAt, rightChangedAt,
      activeDep, depObservedChangedAt, chooseVal, chooseChangedAt, modeHist, leftHist, rightHist, snapRev, snapResult>>

UpdateAt(hist, idx, value) ==
    [hist EXCEPT ![idx] = value]

ChooseAt(r) ==
    IF modeHist[r] = "left" THEN leftHist[r] ELSE rightHist[r]

Init ==
    /\ rev = 0
    /\ cancelEpoch = 0
    /\ prevCancelEpoch = 0
    /\ mode = "left"
    /\ leftVal = 0
    /\ rightVal = 0
    /\ modeChangedAt = -1
    /\ leftChangedAt = -1
    /\ rightChangedAt = -1
    /\ activeDep = "left"
    /\ depObservedChangedAt = -1
    /\ chooseVal = 0
    /\ chooseChangedAt = -1
    /\ modeHist = [i \in 0..MaxRev |-> "left"]
    /\ leftHist = [i \in 0..MaxRev |-> 0]
    /\ rightHist = [i \in 0..MaxRev |-> 0]
    /\ snapRev = -1
    /\ snapResult = NullResult

WriteLeft(v) ==
    /\ rev < MaxRev
    /\ v \in ValueSet
    /\ LET newRev == rev + 1
           newLeftChangedAt == IF v = leftVal THEN leftChangedAt ELSE newRev
           newChoose == IF mode = "left" THEN v ELSE rightVal
           newObservedDepChangedAt == IF mode = "left" THEN newLeftChangedAt ELSE rightChangedAt
           newChooseChangedAt == IF newChoose = chooseVal THEN chooseChangedAt ELSE newRev
       IN
           /\ rev' = newRev
           /\ cancelEpoch' = cancelEpoch + 1
           /\ prevCancelEpoch' = cancelEpoch
           /\ mode' = mode
           /\ leftVal' = v
           /\ rightVal' = rightVal
           /\ modeChangedAt' = modeChangedAt
           /\ leftChangedAt' = newLeftChangedAt
           /\ rightChangedAt' = rightChangedAt
           /\ activeDep' = mode
           /\ depObservedChangedAt' = newObservedDepChangedAt
           /\ chooseVal' = newChoose
           /\ chooseChangedAt' = newChooseChangedAt
           /\ modeHist' = UpdateAt(modeHist, newRev, mode)
           /\ leftHist' = UpdateAt(leftHist, newRev, v)
           /\ rightHist' = UpdateAt(rightHist, newRev, rightVal)
           /\ snapRev' = snapRev
           /\ snapResult' = snapResult

WriteRight(v) ==
    /\ rev < MaxRev
    /\ v \in ValueSet
    /\ LET newRev == rev + 1
           newRightChangedAt == IF v = rightVal THEN rightChangedAt ELSE newRev
           newChoose == IF mode = "right" THEN v ELSE leftVal
           newObservedDepChangedAt == IF mode = "right" THEN newRightChangedAt ELSE leftChangedAt
           newChooseChangedAt == IF newChoose = chooseVal THEN chooseChangedAt ELSE newRev
       IN
           /\ rev' = newRev
           /\ cancelEpoch' = cancelEpoch + 1
           /\ prevCancelEpoch' = cancelEpoch
           /\ mode' = mode
           /\ leftVal' = leftVal
           /\ rightVal' = v
           /\ modeChangedAt' = modeChangedAt
           /\ leftChangedAt' = leftChangedAt
           /\ rightChangedAt' = newRightChangedAt
           /\ activeDep' = mode
           /\ depObservedChangedAt' = newObservedDepChangedAt
           /\ chooseVal' = newChoose
           /\ chooseChangedAt' = newChooseChangedAt
           /\ modeHist' = UpdateAt(modeHist, newRev, mode)
           /\ leftHist' = UpdateAt(leftHist, newRev, leftVal)
           /\ rightHist' = UpdateAt(rightHist, newRev, v)
           /\ snapRev' = snapRev
           /\ snapResult' = snapResult

WriteMode(newMode) ==
    /\ rev < MaxRev
    /\ newMode \in Modes
    /\ LET newRev == rev + 1
           newModeChangedAt == IF newMode = mode THEN modeChangedAt ELSE newRev
           newChoose == IF newMode = "left" THEN leftVal ELSE rightVal
           newObservedDepChangedAt == IF newMode = "left" THEN leftChangedAt ELSE rightChangedAt
           newChooseChangedAt == IF newChoose = chooseVal THEN chooseChangedAt ELSE newRev
       IN
           /\ rev' = newRev
           /\ cancelEpoch' = cancelEpoch + 1
           /\ prevCancelEpoch' = cancelEpoch
           /\ mode' = newMode
           /\ leftVal' = leftVal
           /\ rightVal' = rightVal
           /\ modeChangedAt' = newModeChangedAt
           /\ leftChangedAt' = leftChangedAt
           /\ rightChangedAt' = rightChangedAt
           /\ activeDep' = newMode
           /\ depObservedChangedAt' = newObservedDepChangedAt
           /\ chooseVal' = newChoose
           /\ chooseChangedAt' = newChooseChangedAt
           /\ modeHist' = UpdateAt(modeHist, newRev, newMode)
           /\ leftHist' = UpdateAt(leftHist, newRev, leftVal)
           /\ rightHist' = UpdateAt(rightHist, newRev, rightVal)
           /\ snapRev' = snapRev
           /\ snapResult' = snapResult

TakeSnapshot ==
    /\ snapRev' = rev
    /\ UNCHANGED <<cancelEpoch, prevCancelEpoch, rev, mode, leftVal, rightVal, modeChangedAt, leftChangedAt,
                   rightChangedAt, activeDep, depObservedChangedAt, chooseVal, chooseChangedAt,
                   modeHist, leftHist, rightHist, snapResult>>

ReadSnapshot ==
    /\ snapRev # -1
    /\ snapResult' = ChooseAt(snapRev)
    /\ UNCHANGED <<cancelEpoch, prevCancelEpoch, rev, mode, leftVal, rightVal, modeChangedAt, leftChangedAt,
                   rightChangedAt, activeDep, depObservedChangedAt, chooseVal, chooseChangedAt,
                   modeHist, leftHist, rightHist, snapRev>>

ReadLive ==
    /\ snapResult' = chooseVal
    /\ UNCHANGED <<cancelEpoch, prevCancelEpoch, rev, mode, leftVal, rightVal, modeChangedAt, leftChangedAt,
                   rightChangedAt, activeDep, depObservedChangedAt, chooseVal, chooseChangedAt,
                   modeHist, leftHist, rightHist, snapRev>>

Next ==
    \/ \E v \in ValueSet: WriteLeft(v)
    \/ \E v \in ValueSet: WriteRight(v)
    \/ \E m \in Modes: WriteMode(m)
    \/ TakeSnapshot
    \/ ReadSnapshot
    \/ ReadLive

Spec ==
    Init /\ [][Next]_vars

TypeOK ==
    /\ rev \in 0..MaxRev
    /\ cancelEpoch \in Nat
    /\ prevCancelEpoch \in Nat
    /\ mode \in Modes
    /\ leftVal \in ValueSet
    /\ rightVal \in ValueSet
    /\ modeChangedAt \in -1..MaxRev
    /\ leftChangedAt \in -1..MaxRev
    /\ rightChangedAt \in -1..MaxRev
    /\ activeDep \in Modes
    /\ depObservedChangedAt \in -1..MaxRev
    /\ chooseVal \in ValueSet
    /\ chooseChangedAt \in -1..MaxRev
    /\ modeHist \in [0..MaxRev -> Modes]
    /\ leftHist \in [0..MaxRev -> ValueSet]
    /\ rightHist \in [0..MaxRev -> ValueSet]
    /\ snapRev \in -1..MaxRev
    /\ snapResult \in ValueSet \cup {NullResult}

SnapshotConsistency ==
    snapRev = -1 \/ snapResult = NullResult \/ snapResult = ChooseAt(snapRev)

DependencyValidity ==
    depObservedChangedAt = IF activeDep = "left" THEN leftChangedAt ELSE rightChangedAt

RedGreenValueAgreement ==
    /\ activeDep = mode
    /\ chooseVal = IF activeDep = "left" THEN leftVal ELSE rightVal

CancelEpochMonotone ==
    cancelEpoch >= prevCancelEpoch

====
