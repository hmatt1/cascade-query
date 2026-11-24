---- MODULE CascadeCore ----
(***************************************************************************)
(* Lazy re-verification with concurrent readers.                           *)
(*                                                                         *)
(* Three inputs (a mode switch and two data inputs) and two queries:       *)
(*                                                                         *)
(*     choose = left if mode = "left" else right                           *)
(*     top    = 0 if choose = 0 else 1                                     *)
(*                                                                         *)
(* A write changes an input and nothing else.  Cached results are brought  *)
(* up to date by the readers, one critical section at a time, as in        *)
(* Evaluator.compute_or_get_memo: look the row up, verify its recorded     *)
(* dependencies at the caller's revision, mark it green or claim the       *)
(* in-flight slot and run the body, store, release.  Each action below is  *)
(* one acquisition of the store lock, so TLC explores every interleaving   *)
(* of readers at different revisions, writers and evictions.               *)
(* docs/formal/README.md maps the actions to the code.                     *)
(***************************************************************************)
EXTENDS Integers, FiniteSets, TLC

CONSTANTS
    MaxRev,         \* writes stop at this revision
    ValueSet,       \* values of the data inputs; contains 0
    Readers,        \* concurrent callers
    SnapshotReads,  \* TRUE: readers may also read at older revisions
    Evictions,      \* TRUE: cached rows may be dropped at any time
    None            \* no row, no entry, no result

ASSUME 0 \in ValueSet /\ ValueSet \subseteq Nat

Modes == {"left", "right"}
Queries == {"choose", "top"}
Revs == 0..MaxRev
TopValues == {0, 1}

VARIABLES
    rev,          \* newest revision                       (GraphStore.revision)
    cancelEpoch,  \* bumped by every write                 (GraphStore.cancel_epoch)
    modeHist,     \* [revision -> [val, at]] of the mode input
    dataHist,     \* [input -> [revision -> [val, at]]] of the data inputs
    chooseRow,    \* the one cached row of choose, or None (GraphStore.memos)
    topRow,       \* the one cached row of top, or None
    diskChoose,   \* the persisted record of choose, or None
    inflight,     \* <<query, revision>> pairs being computed (GraphStore.in_flight)
    pc,           \* per reader: where it is in the call
    at,           \* per reader: the revision it reads at
    goal,         \* per reader: the query it called
    sub,          \* per reader: why it is inside a nested call of choose
    ent,          \* per reader: the entry of choose it holds
    liveC,        \* per reader: the row it looked up is still the stored one
    seenT,        \* per reader: the row of top it looked up ("existing")
    liveT,        \* per reader: that row is still the stored one
    mail,         \* per reader: the entry handed over by the run it joined
    result        \* per reader: what the call returned

inputVars == <<rev, cancelEpoch, modeHist, dataHist>>
storeVars == <<chooseRow, topRow, diskChoose, inflight>>
readerVars == <<pc, at, goal, sub, ent, liveC, seenT, liveT, mail, result>>
vars == <<inputVars, storeVars, readerVars>>

(***************************************************************************)
(* What a fresh run gives.                                                 *)
(***************************************************************************)
ChooseAt(s) == dataHist[modeHist[s].val][s].val
TopOf(c) == IF c = 0 THEN 0 ELSE 1
Eval(q, s) == IF q = "choose" THEN ChooseAt(s) ELSE TopOf(ChooseAt(s))

(***************************************************************************)
(* Rows.  ``ver`` is verified_at, ``clean`` is head_clean, ``stamp`` is    *)
(* changed_at.  The dependencies of choose are the mode input and the      *)
(* branch it selected, each with the marker it had.  top depends on choose *)
(* and records its stamp.  Two fields of its row are ghosts, kept to state  *)
(* properties: ``chooseVal`` is the value that stamp stood for, and        *)
(* ``moved`` says that choose has had another value since the row was      *)
(* computed.  Nothing reads top, so its own stamp is left out.  An entry   *)
(* is what a call of choose hands back.                                    *)
(***************************************************************************)
ChooseRows ==
    [val: ValueSet, stamp: Nat, ver: Revs, clean: BOOLEAN,
     modeAt: Revs, branch: Modes, branchAt: Revs]
TopRows ==
    [val: TopValues, ver: Revs, clean: BOOLEAN,
     chooseStamp: Nat, chooseVal: ValueSet, moved: BOOLEAN]
TopSeen == [val: TopValues, chooseStamp: Nat, chooseVal: ValueSet]
Entries == [val: Nat, stamp: Nat]

Entry(row) == [val |-> row.val, stamp |-> row.stamp]
TopEntry(v) == [val |-> v, stamp |-> 0]

\* GraphStore.is_current_locked: usable without a dependency walk.
Current(row, s) == row.ver = s \/ (row.clean /\ s = rev)

\* Evaluator.try_mark_green for choose: every recorded dependency, in the
\* order it was read, still has the marker that was recorded.
ChooseDepsUnchanged(row, s) ==
    /\ modeHist[s].at = row.modeAt
    /\ dataHist[row.branch][s].at = row.branchAt

AllFalse == [r \in Readers |-> FALSE]

(***************************************************************************)
(* Change stamps.  The code takes them from a counter                      *)
(* (GraphStore.new_change_stamp_locked) and only ever compares them for    *)
(* equality.  A stamp that no row and no caller holds any more can never   *)
(* be compared again, so the model hands out the smallest such number      *)
(* instead of counting up: the same behaviours, in a finite state space    *)
(* with no bound on the number of runs.                                    *)
(***************************************************************************)
StampsOf(x, field) == IF x = None THEN {} ELSE {x[field]}
UsedStamps ==
    StampsOf(chooseRow, "stamp")
    \cup StampsOf(topRow, "chooseStamp")
    \cup UNION {StampsOf(ent[r], "stamp") : r \in Readers}
    \cup UNION {StampsOf(seenT[r], "chooseStamp") : r \in Readers}
    \cup UNION {StampsOf(mail[r], "stamp") : r \in Readers}
FreshStamp == CHOOSE k \in 1..(3 + 3 * Cardinality(Readers)) : k \notin UsedStamps

Init ==
    /\ rev = 0
    /\ cancelEpoch = 0
    /\ modeHist = [s \in Revs |-> [val |-> "left", at |-> 0]]
    /\ dataHist = [i \in Modes |-> [s \in Revs |-> [val |-> 0, at |-> 0]]]
    /\ chooseRow = None
    /\ topRow = None
    /\ diskChoose = None
    /\ inflight = {}
    /\ pc = [r \in Readers |-> "idle"]
    /\ at = [r \in Readers |-> 0]
    /\ goal = [r \in Readers |-> "choose"]
    /\ sub = [r \in Readers |-> "none"]
    /\ ent = [r \in Readers |-> None]
    /\ liveC = AllFalse
    /\ seenT = [r \in Readers |-> None]
    /\ liveT = AllFalse
    /\ mail = [r \in Readers |-> None]
    /\ result = [r \in Readers |-> None]

(***************************************************************************)
(* Writers.  Setting an input to the value it has is a no-op (G3), so only *)
(* real changes are steps.  A write records the new value from the next    *)
(* revision on and clears head_clean on the rows that read the input       *)
(* (GraphStore.mark_dependents_dirty_locked): the walk follows the         *)
(* dependents edges of the stored rows and stops at a row that is already  *)
(* dirty.                                                                  *)
(***************************************************************************)
MarkDirty(i) ==
    IF chooseRow # None /\ chooseRow.clean /\ (i = "mode" \/ chooseRow.branch = i)
    THEN /\ chooseRow' = [chooseRow EXCEPT !.clean = FALSE]
         /\ topRow' = IF topRow # None THEN [topRow EXCEPT !.clean = FALSE] ELSE topRow
    ELSE UNCHANGED <<chooseRow, topRow>>

Bump ==
    /\ rev' = rev + 1
    /\ cancelEpoch' = cancelEpoch + 1
    /\ UNCHANGED <<diskChoose, inflight, readerVars>>

WriteMode(m) ==
    /\ rev < MaxRev
    /\ m # modeHist[rev].val
    /\ modeHist' = [s \in Revs |->
                        IF s > rev THEN [val |-> m, at |-> rev + 1] ELSE modeHist[s]]
    /\ MarkDirty("mode")
    /\ Bump
    /\ UNCHANGED dataHist

WriteData(i, v) ==
    /\ rev < MaxRev
    /\ v # dataHist[i][rev].val
    /\ dataHist' = [dataHist EXCEPT ![i] =
                        [s \in Revs |->
                            IF s > rev THEN [val |-> v, at |-> rev + 1] ELSE @[s]]]
    /\ MarkDirty(i)
    /\ Bump
    /\ UNCHANGED modeHist

(***************************************************************************)
(* Eviction, prune and sweep drop a row (GraphStore.drop_memo_locked).     *)
(* Whoever depends on it can no longer rely on its edges and turns dirty.  *)
(***************************************************************************)
EvictChoose ==
    /\ Evictions
    /\ chooseRow # None
    /\ chooseRow' = None
    /\ topRow' = IF topRow # None THEN [topRow EXCEPT !.clean = FALSE] ELSE topRow
    /\ liveC' = AllFalse
    /\ UNCHANGED <<inputVars, diskChoose, inflight,
                   pc, at, goal, sub, ent, seenT, liveT, mail, result>>

EvictTop ==
    /\ Evictions
    /\ topRow # None
    /\ topRow' = None
    /\ liveT' = AllFalse
    /\ UNCHANGED <<inputVars, chooseRow, diskChoose, inflight,
                   pc, at, goal, sub, ent, liveC, seenT, mail, result>>

(***************************************************************************)
(* A call starts by fixing its revision (GraphStore.pin_snapshot): the     *)
(* newest one (ReadLive) or one a snapshot was taken at (ReadSnapshot).    *)
(* Taking a snapshot needs no step of its own: any revision up to the      *)
(* newest may have one.                                                    *)
(***************************************************************************)
Begin(r, q, s) ==
    /\ pc[r] \in {"idle", "done"}
    /\ at' = [at EXCEPT ![r] = s]
    /\ goal' = [goal EXCEPT ![r] = q]
    /\ result' = [result EXCEPT ![r] = None]
    /\ pc' = [pc EXCEPT ![r] = IF q = "choose" THEN "c_lookup" ELSE "t_lookup"]
    /\ UNCHANGED <<inputVars, storeVars, sub, ent, liveC, seenT, liveT, mail>>

ReadLive(r, q) == Begin(r, q, rev)

ReadSnapshot(r, q, s) ==
    /\ SnapshotReads
    /\ s < rev
    /\ Begin(r, q, s)

(***************************************************************************)
(* A call of choose ends by handing its entry to whoever made it: the      *)
(* reader itself, the verification of top, or the body of top.  Return     *)
(* sets pc, sub, ent and result of reader r.                               *)
(***************************************************************************)
Return(r, e) ==
    CASE sub[r] = "none" ->
            /\ pc' = [pc EXCEPT ![r] = "done"]
            /\ result' = [result EXCEPT ![r] = e.val]
            /\ ent' = [ent EXCEPT ![r] = None]
            /\ sub' = sub
      [] sub[r] = "verify" ->
            \* Evaluator.try_mark_green for top: its one dependency carries
            \* the stamp that was recorded, or it does not.  The entry the
            \* nested call returned is compared, not whatever row is stored
            \* by now (Evaluator._dependency_entry).
            /\ pc' = [pc EXCEPT ![r] =
                        IF e.stamp = seenT[r].chooseStamp THEN "t_green" ELSE "t_claim"]
            /\ result' = result
            /\ ent' = [ent EXCEPT ![r] = None]
            /\ sub' = [sub EXCEPT ![r] = "none"]
      [] sub[r] = "body" ->
            /\ pc' = [pc EXCEPT ![r] = "t_store"]
            /\ result' = result
            /\ ent' = [ent EXCEPT ![r] = e]
            /\ sub' = [sub EXCEPT ![r] = "none"]

(***************************************************************************)
(* choose: look the row up under the lock.  A row that is current is a     *)
(* cache hit.  Any other row is verified against the input history at the  *)
(* caller's revision, which nothing can change any more, so the outcome of *)
(* the walk is known here; committing it is a later step, and the row may  *)
(* be replaced or dropped in between.                                      *)
(***************************************************************************)
CLookup(r) ==
    /\ pc[r] = "c_lookup"
    /\ IF chooseRow # None /\ Current(chooseRow, at[r])
       THEN /\ chooseRow' = [chooseRow EXCEPT !.ver = at[r]]
            /\ Return(r, Entry(chooseRow))
            /\ UNCHANGED liveC
       ELSE IF chooseRow # None /\ ChooseDepsUnchanged(chooseRow, at[r])
       THEN /\ pc' = [pc EXCEPT ![r] = "c_green"]
            /\ ent' = [ent EXCEPT ![r] = Entry(chooseRow)]
            /\ liveC' = [liveC EXCEPT ![r] = TRUE]
            /\ UNCHANGED <<chooseRow, sub, result>>
       ELSE \* Red, or no row at all: only then may a persisted record be used.
            /\ pc' = [pc EXCEPT ![r] = IF chooseRow = None THEN "c_claim_cold" ELSE "c_claim"]
            /\ liveC' = [liveC EXCEPT ![r] = chooseRow # None]
            /\ UNCHANGED <<chooseRow, sub, ent, result>>
    /\ UNCHANGED <<inputVars, topRow, diskChoose, inflight, at, goal, seenT, liveT, mail>>

\* Evaluator._commit_green: the row the caller verified is its answer.  If
\* it is still the stored row, it is now verified at this revision, and
\* clean when this is the newest revision.
CGreen(r) ==
    /\ pc[r] = "c_green"
    /\ chooseRow' = IF liveC[r]
                    THEN [chooseRow EXCEPT !.ver = at[r], !.clean = @ \/ at[r] = rev]
                    ELSE chooseRow
    /\ Return(r, ent[r])
    /\ liveC' = [liveC EXCEPT ![r] = FALSE]
    /\ UNCHANGED <<inputVars, topRow, diskChoose, inflight, at, goal, seenT, liveT, mail>>

\* The row was missing or red.  Under the lock again: use a result another
\* caller stored for this revision meanwhile (_finished_meanwhile_locked),
\* or join the caller that is computing one, or claim the slot.
CClaim(r) ==
    /\ pc[r] \in {"c_claim", "c_claim_cold"}
    /\ LET key == <<"choose", at[r]>> IN
       IF key \notin inflight /\ chooseRow # None /\ ~liveC[r] /\ chooseRow.ver = at[r]
       THEN /\ Return(r, Entry(chooseRow))
            /\ UNCHANGED inflight
       ELSE IF key \in inflight
       THEN /\ pc' = [pc EXCEPT ![r] = "c_wait"]
            /\ UNCHANGED <<inflight, sub, ent, result>>
       ELSE /\ inflight' = inflight \cup {key}
            /\ pc' = [pc EXCEPT ![r] = IF pc[r] = "c_claim_cold" THEN "c_load" ELSE "c_run"]
            /\ UNCHANGED <<sub, ent, result>>
    /\ liveC' = [liveC EXCEPT ![r] = FALSE]
    /\ UNCHANGED <<inputVars, chooseRow, topRow, diskChoose, at, goal, seenT, liveT, mail>>

\* The owner of the slot produces the row.  A caller that saw no row first
\* tries the persisted record (_try_hydrate_from_disk), which is used only
\* if the values of its dependencies at this revision are the ones it was
\* written with; a loaded row always gets a new stamp.  Otherwise the body
\* runs (it reads inputs at the caller's revision, so it is one step) and
\* the result keeps the stamp of the row it replaces when the value is
\* equal: the early bail-out (_finish_recompute).  Replacing a row dirties
\* its dependents.  The result is written to disk.
CRun(r) ==
    /\ pc[r] \in {"c_run", "c_load"}
    /\ LET s == at[r]
           m == modeHist[s]
           d == dataHist[m.val][s]
           load == /\ pc[r] = "c_load"
                   /\ diskChoose # None
                   /\ diskChoose.modeVal = m.val
                   /\ diskChoose.dataVal = d.val
           val == IF load THEN diskChoose.val ELSE d.val
           backdate == ~load /\ chooseRow # None /\ chooseRow.val = val
           newStamp == IF backdate THEN chooseRow.stamp ELSE FreshStamp
       IN
       /\ chooseRow' = [val |-> val, stamp |-> newStamp, ver |-> s, clean |-> s = rev,
                        modeAt |-> m.at, branch |-> m.val, branchAt |-> d.at]
       /\ diskChoose' = IF load THEN diskChoose
                        ELSE [val |-> val, modeVal |-> m.val, dataVal |-> d.val]
       /\ ent' = [ent EXCEPT ![r] = [val |-> val, stamp |-> newStamp]]
       /\ topRow' = IF topRow = None THEN topRow
                    ELSE [topRow EXCEPT !.clean = @ /\ chooseRow = None,
                                        !.moved = @ \/ val # topRow.chooseVal]
    /\ liveC' = AllFalse
    /\ pc' = [pc EXCEPT ![r] = "c_release"]
    /\ UNCHANGED <<inputVars, inflight, at, goal, sub, seenT, liveT, mail, result>>

\* Evaluator._release_in_flight: give the slot up and hand the entry to
\* every caller that joined.
CRelease(r) ==
    /\ pc[r] = "c_release"
    /\ inflight' = inflight \ {<<"choose", at[r]>>}
    /\ mail' = [w \in Readers |->
                    IF pc[w] = "c_wait" /\ at[w] = at[r] THEN ent[r] ELSE mail[w]]
    /\ Return(r, ent[r])
    /\ UNCHANGED <<inputVars, chooseRow, topRow, diskChoose, at, goal, liveC, seenT, liveT>>

CWait(r) ==
    /\ pc[r] = "c_wait"
    /\ mail[r] # None
    /\ Return(r, mail[r])
    /\ mail' = [mail EXCEPT ![r] = None]
    /\ UNCHANGED <<inputVars, storeVars, at, goal, liveC, seenT, liveT>>

(***************************************************************************)
(* top.  The same steps; its dependency is a query, so verifying it and    *)
(* running its body both go through a nested call of choose at the same    *)
(* revision.                                                               *)
(***************************************************************************)
TLookup(r) ==
    /\ pc[r] = "t_lookup"
    /\ IF topRow # None /\ Current(topRow, at[r])
       THEN /\ topRow' = [topRow EXCEPT !.ver = at[r]]
            /\ result' = [result EXCEPT ![r] = topRow.val]
            /\ pc' = [pc EXCEPT ![r] = "done"]
            /\ UNCHANGED <<seenT, liveT, sub>>
       ELSE IF topRow # None
       THEN /\ seenT' = [seenT EXCEPT ![r] =
                            [val |-> topRow.val, chooseStamp |-> topRow.chooseStamp,
                             chooseVal |-> topRow.chooseVal]]
            /\ liveT' = [liveT EXCEPT ![r] = TRUE]
            /\ sub' = [sub EXCEPT ![r] = "verify"]
            /\ pc' = [pc EXCEPT ![r] = "c_lookup"]
            /\ UNCHANGED <<topRow, result>>
       ELSE /\ pc' = [pc EXCEPT ![r] = "t_claim"]
            /\ UNCHANGED <<topRow, result, seenT, liveT, sub>>
    /\ UNCHANGED <<inputVars, chooseRow, diskChoose, inflight, at, goal, ent, liveC, mail>>

\* GraphStore.refresh_head_clean_locked: clean only at the newest revision
\* and only above a clean row of choose.
TGreen(r) ==
    /\ pc[r] = "t_green"
    /\ topRow' = IF liveT[r]
                 THEN [topRow EXCEPT
                          !.ver = at[r],
                          !.clean = @ \/ (at[r] = rev /\ chooseRow # None /\ chooseRow.clean)]
                 ELSE topRow
    /\ result' = [result EXCEPT ![r] = seenT[r].val]
    /\ pc' = [pc EXCEPT ![r] = "done"]
    /\ seenT' = [seenT EXCEPT ![r] = None]
    /\ liveT' = [liveT EXCEPT ![r] = FALSE]
    /\ UNCHANGED <<inputVars, chooseRow, diskChoose, inflight, at, goal, sub, ent, liveC, mail>>

TClaim(r) ==
    /\ pc[r] = "t_claim"
    /\ LET key == <<"top", at[r]>> IN
       IF key \notin inflight /\ topRow # None /\ ~liveT[r] /\ topRow.ver = at[r]
       THEN /\ result' = [result EXCEPT ![r] = topRow.val]
            /\ pc' = [pc EXCEPT ![r] = "done"]
            /\ UNCHANGED <<inflight, sub>>
       ELSE IF key \in inflight
       THEN /\ pc' = [pc EXCEPT ![r] = "t_wait"]
            /\ UNCHANGED <<inflight, sub, result>>
       ELSE /\ inflight' = inflight \cup {key}
            /\ sub' = [sub EXCEPT ![r] = "body"]
            /\ pc' = [pc EXCEPT ![r] = "c_lookup"]
            /\ UNCHANGED result
    /\ seenT' = [seenT EXCEPT ![r] = None]
    /\ liveT' = [liveT EXCEPT ![r] = FALSE]
    /\ UNCHANGED <<inputVars, chooseRow, topRow, diskChoose, at, goal, ent, liveC, mail>>

\* The body of top has its value of choose; store the row.
TStore(r) ==
    /\ pc[r] = "t_store"
    /\ topRow' = [val |-> TopOf(ent[r].val), ver |-> at[r],
                  clean |-> (at[r] = rev /\ chooseRow # None /\ chooseRow.clean),
                  chooseStamp |-> ent[r].stamp, chooseVal |-> ent[r].val,
                  moved |-> FALSE]
    /\ ent' = [ent EXCEPT ![r] = TopEntry(TopOf(ent[r].val))]
    /\ liveT' = AllFalse
    /\ pc' = [pc EXCEPT ![r] = "t_release"]
    /\ UNCHANGED <<inputVars, chooseRow, diskChoose, inflight,
                   at, goal, sub, liveC, seenT, mail, result>>

TRelease(r) ==
    /\ pc[r] = "t_release"
    /\ inflight' = inflight \ {<<"top", at[r]>>}
    /\ mail' = [w \in Readers |->
                    IF pc[w] = "t_wait" /\ at[w] = at[r] THEN ent[r] ELSE mail[w]]
    /\ result' = [result EXCEPT ![r] = ent[r].val]
    /\ ent' = [ent EXCEPT ![r] = None]
    /\ pc' = [pc EXCEPT ![r] = "done"]
    /\ UNCHANGED <<inputVars, chooseRow, topRow, diskChoose, at, goal, sub, liveC, seenT, liveT>>

TWait(r) ==
    /\ pc[r] = "t_wait"
    /\ mail[r] # None
    /\ result' = [result EXCEPT ![r] = mail[r].val]
    /\ mail' = [mail EXCEPT ![r] = None]
    /\ pc' = [pc EXCEPT ![r] = "done"]
    /\ UNCHANGED <<inputVars, storeVars, at, goal, sub, ent, liveC, seenT, liveT>>

Next ==
    \/ \E m \in Modes: WriteMode(m)
    \/ \E i \in Modes, v \in ValueSet: WriteData(i, v)
    \/ EvictChoose
    \/ EvictTop
    \/ \E r \in Readers:
        \/ \E q \in Queries: ReadLive(r, q)
        \/ \E q \in Queries, s \in Revs: ReadSnapshot(r, q, s)
        \/ CLookup(r) \/ CGreen(r) \/ CClaim(r) \/ CRun(r) \/ CRelease(r) \/ CWait(r)
        \/ TLookup(r) \/ TGreen(r) \/ TClaim(r) \/ TStore(r) \/ TRelease(r) \/ TWait(r)

Spec == Init /\ [][Next]_vars

Symmetry == Permutations(Readers)

(***************************************************************************)
(* Invariants.                                                             *)
(***************************************************************************)
PCs == {"idle", "c_lookup", "c_green", "c_claim", "c_claim_cold", "c_run", "c_load",
        "c_release", "c_wait", "t_lookup", "t_green", "t_claim", "t_store",
        "t_release", "t_wait", "done"}

TypeOK ==
    /\ rev \in Revs
    /\ cancelEpoch \in Nat
    /\ modeHist \in [Revs -> [val: Modes, at: Revs]]
    /\ dataHist \in [Modes -> [Revs -> [val: ValueSet, at: Revs]]]
    /\ chooseRow \in ChooseRows \cup {None}
    /\ topRow \in TopRows \cup {None}
    /\ diskChoose \in [val: ValueSet, modeVal: Modes, dataVal: ValueSet] \cup {None}
    /\ inflight \subseteq Queries \X Revs
    /\ pc \in [Readers -> PCs]
    /\ at \in [Readers -> Revs]
    /\ goal \in [Readers -> Queries]
    /\ sub \in [Readers -> {"none", "verify", "body"}]
    /\ ent \in [Readers -> Entries \cup {None}]
    /\ liveC \in [Readers -> BOOLEAN]
    /\ seenT \in [Readers -> TopSeen \cup {None}]
    /\ liveT \in [Readers -> BOOLEAN]
    /\ mail \in [Readers -> Entries \cup {None}]
    /\ result \in [Readers -> Nat \cup {None}]

\* G1 and G4: a call returns what a fresh run at its revision would, at the
\* newest revision and through a snapshot, whatever ran in between.
ReadsMatchFreshRun ==
    \A r \in Readers: pc[r] = "done" => result[r] = Eval(goal[r], at[r])

\* A stored row holds the value for the revision it was last verified at,
\* and a clean row holds the value for the newest revision: what a cache hit
\* without a dependency walk relies on.
RowsAreSound ==
    /\ chooseRow # None =>
        /\ chooseRow.val = ChooseAt(chooseRow.ver)
        /\ chooseRow.clean => chooseRow.val = ChooseAt(rev)
    /\ topRow # None =>
        /\ topRow.val = TopOf(ChooseAt(topRow.ver))
        /\ topRow.clean => topRow.val = TopOf(ChooseAt(rev))

\* The dependencies a row records are the ones a run at its verified
\* revision reads, with the markers they have there.
RecordedDependenciesHold ==
    chooseRow # None =>
        /\ chooseRow.branch = modeHist[chooseRow.ver].val
        /\ chooseRow.modeAt = modeHist[chooseRow.ver].at
        /\ chooseRow.branchAt = dataHist[chooseRow.branch][chooseRow.ver].at

\* A change stamp names one value of choose: every row and every caller
\* that holds a stamp of choose agrees on the value it stands for, and top
\* was computed from the value its recorded stamp stands for.  Comparing
\* stamps is therefore comparing values.
Held(x, stampField, valField) == IF x = None THEN {} ELSE {<<x[stampField], x[valField]>>}
HoldsChooseEntry(r) == pc[r] \in {"c_green", "c_release", "t_store"}
HeldChooseEntries ==
    Held(chooseRow, "stamp", "val")
    \cup Held(topRow, "chooseStamp", "chooseVal")
    \cup UNION {Held(ent[r], "stamp", "val") : r \in {x \in Readers : HoldsChooseEntry(x)}}
    \cup UNION {Held(seenT[r], "chooseStamp", "chooseVal") : r \in Readers}
    \cup UNION {Held(mail[r], "stamp", "val") : r \in {x \in Readers : pc[x] = "c_wait"}}
StampsNameValues ==
    /\ \A a, b \in HeldChooseEntries: a[1] = b[1] => a[2] = b[2]
    /\ topRow # None => topRow.val = TopOf(topRow.chooseVal)

\* Dirty propagation, invariant 1 in GraphStore: a clean row sits on clean
\* rows, so a write to any input beneath it reaches it.
CleanRowsRestOnCleanRows ==
    (topRow # None /\ topRow.clean) => (chooseRow # None /\ chooseRow.clean)

\* G5: at most one caller runs a query at a revision; the others join it,
\* and whoever waits is waiting for a caller that will hand over an entry.
RunsChoose(r) == pc[r] \in {"c_run", "c_load", "c_release"}
RunsTop(r) == sub[r] = "body" \/ pc[r] \in {"t_store", "t_release"}
OneRunPerRevision ==
    /\ \A r1, r2 \in Readers:
        (r1 # r2 /\ at[r1] = at[r2]) =>
            /\ ~(RunsChoose(r1) /\ RunsChoose(r2))
            /\ ~(RunsTop(r1) /\ RunsTop(r2))
    /\ \A s \in Revs:
        /\ <<"choose", s>> \in inflight <=> \E r \in Readers: RunsChoose(r) /\ at[r] = s
        /\ <<"top", s>> \in inflight <=> \E r \in Readers: RunsTop(r) /\ at[r] = s
    /\ \A w \in Readers:
        /\ (pc[w] = "c_wait" /\ mail[w] = None) =>
            \E r \in Readers: r # w /\ RunsChoose(r) /\ at[r] = at[w]
        /\ (pc[w] = "t_wait" /\ mail[w] = None) =>
            \E r \in Readers: r # w /\ RunsTop(r) /\ at[r] = at[w]

\* Markers and verified revisions never run ahead of the newest revision.
MarkersBoundedByRevision ==
    /\ \A s \in Revs:
        /\ modeHist[s].at <= rev
        /\ \A i \in Modes: dataHist[i][s].at <= rev
    /\ chooseRow # None => chooseRow.ver <= rev
    /\ topRow # None => topRow.ver <= rev
    /\ \A r \in Readers: at[r] <= rev

\* G2, checked without evictions and snapshot readers (CascadeMinimal.cfg):
\* the body of top runs again only if choose has had another value since
\* the row of top was computed.  A run of choose that gives the value it
\* had keeps its stamp, and top stays as it is.  With evictions or readers
\* at older revisions the row of choose can be replaced in between and come
\* back under a new stamp, which costs a run of top and changes no result
\* (G13).
TopRunsOnlyAfterChooseMoved ==
    \A r \in Readers: (pc[r] = "t_store" /\ topRow # None) => topRow.moved

\* The revision and the cancellation epoch never go back.
Monotone == [][rev' >= rev /\ cancelEpoch' >= cancelEpoch]_vars

====
