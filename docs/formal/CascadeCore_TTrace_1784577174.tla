---- MODULE CascadeCore_TTrace_1784577174 ----
EXTENDS Sequences, CascadeCore, TLCExt, Toolbox, Naturals, TLC

_expression ==
    LET CascadeCore_TEExpression == INSTANCE CascadeCore_TEExpression
    IN CascadeCore_TEExpression!expression
----

_trace ==
    LET CascadeCore_TETrace == INSTANCE CascadeCore_TETrace
    IN CascadeCore_TETrace!trace
----

_inv ==
    ~(
        TLCGet("level") = Len(_TETrace)
        /\
        chooseChangedAt = (1)
        /\
        rightVal = (0)
        /\
        rev = (1)
        /\
        snapResult = (0)
        /\
        activeDep = ("left")
        /\
        modeChangedAt = (-1)
        /\
        cancelEpoch = (1)
        /\
        rightHist = ((0 :> 0 @@ 1 :> 0 @@ 2 :> 0 @@ 3 :> 0 @@ 4 :> 0 @@ 5 :> 0))
        /\
        leftHist = ((0 :> 0 @@ 1 :> 1 @@ 2 :> 0 @@ 3 :> 0 @@ 4 :> 0 @@ 5 :> 0))
        /\
        modeHist = ((0 :> "left" @@ 1 :> "left" @@ 2 :> "left" @@ 3 :> "left" @@ 4 :> "left" @@ 5 :> "left"))
        /\
        mode = ("left")
        /\
        rightChangedAt = (-1)
        /\
        diskCache = ({})
        /\
        leftChangedAt = (1)
        /\
        snapRev = (1)
        /\
        leftVal = (1)
        /\
        chooseVal = (1)
        /\
        depObservedChangedAt = (1)
        /\
        prevCancelEpoch = (0)
    )
----

_init ==
    /\ modeHist = _TETrace[1].modeHist
    /\ rightVal = _TETrace[1].rightVal
    /\ leftHist = _TETrace[1].leftHist
    /\ cancelEpoch = _TETrace[1].cancelEpoch
    /\ rightHist = _TETrace[1].rightHist
    /\ snapRev = _TETrace[1].snapRev
    /\ mode = _TETrace[1].mode
    /\ activeDep = _TETrace[1].activeDep
    /\ snapResult = _TETrace[1].snapResult
    /\ rev = _TETrace[1].rev
    /\ chooseVal = _TETrace[1].chooseVal
    /\ diskCache = _TETrace[1].diskCache
    /\ modeChangedAt = _TETrace[1].modeChangedAt
    /\ leftVal = _TETrace[1].leftVal
    /\ leftChangedAt = _TETrace[1].leftChangedAt
    /\ chooseChangedAt = _TETrace[1].chooseChangedAt
    /\ prevCancelEpoch = _TETrace[1].prevCancelEpoch
    /\ depObservedChangedAt = _TETrace[1].depObservedChangedAt
    /\ rightChangedAt = _TETrace[1].rightChangedAt
----

_next ==
    /\ \E i,j \in DOMAIN _TETrace:
        /\ \/ /\ j = i + 1
              /\ i = TLCGet("level")
        /\ modeHist  = _TETrace[i].modeHist
        /\ modeHist' = _TETrace[j].modeHist
        /\ rightVal  = _TETrace[i].rightVal
        /\ rightVal' = _TETrace[j].rightVal
        /\ leftHist  = _TETrace[i].leftHist
        /\ leftHist' = _TETrace[j].leftHist
        /\ cancelEpoch  = _TETrace[i].cancelEpoch
        /\ cancelEpoch' = _TETrace[j].cancelEpoch
        /\ rightHist  = _TETrace[i].rightHist
        /\ rightHist' = _TETrace[j].rightHist
        /\ snapRev  = _TETrace[i].snapRev
        /\ snapRev' = _TETrace[j].snapRev
        /\ mode  = _TETrace[i].mode
        /\ mode' = _TETrace[j].mode
        /\ activeDep  = _TETrace[i].activeDep
        /\ activeDep' = _TETrace[j].activeDep
        /\ snapResult  = _TETrace[i].snapResult
        /\ snapResult' = _TETrace[j].snapResult
        /\ rev  = _TETrace[i].rev
        /\ rev' = _TETrace[j].rev
        /\ chooseVal  = _TETrace[i].chooseVal
        /\ chooseVal' = _TETrace[j].chooseVal
        /\ diskCache  = _TETrace[i].diskCache
        /\ diskCache' = _TETrace[j].diskCache
        /\ modeChangedAt  = _TETrace[i].modeChangedAt
        /\ modeChangedAt' = _TETrace[j].modeChangedAt
        /\ leftVal  = _TETrace[i].leftVal
        /\ leftVal' = _TETrace[j].leftVal
        /\ leftChangedAt  = _TETrace[i].leftChangedAt
        /\ leftChangedAt' = _TETrace[j].leftChangedAt
        /\ chooseChangedAt  = _TETrace[i].chooseChangedAt
        /\ chooseChangedAt' = _TETrace[j].chooseChangedAt
        /\ prevCancelEpoch  = _TETrace[i].prevCancelEpoch
        /\ prevCancelEpoch' = _TETrace[j].prevCancelEpoch
        /\ depObservedChangedAt  = _TETrace[i].depObservedChangedAt
        /\ depObservedChangedAt' = _TETrace[j].depObservedChangedAt
        /\ rightChangedAt  = _TETrace[i].rightChangedAt
        /\ rightChangedAt' = _TETrace[j].rightChangedAt

\* Uncomment the ASSUME below to write the states of the error trace
\* to the given file in Json format. Note that you can pass any tuple
\* to `JsonSerialize`. For example, a sub-sequence of _TETrace.
    \* ASSUME
    \*     LET J == INSTANCE Json
    \*         IN J!JsonSerialize("CascadeCore_TTrace_1784577174.json", _TETrace)

=============================================================================

 Note that you can extract this module `CascadeCore_TEExpression`
  to a dedicated file to reuse `expression` (the module in the 
  dedicated `CascadeCore_TEExpression.tla` file takes precedence 
  over the module `CascadeCore_TEExpression` below).

---- MODULE CascadeCore_TEExpression ----
EXTENDS Sequences, CascadeCore, TLCExt, Toolbox, Naturals, TLC

expression == 
    [
        \* To hide variables of the `CascadeCore` spec from the error trace,
        \* remove the variables below.  The trace will be written in the order
        \* of the fields of this record.
        modeHist |-> modeHist
        ,rightVal |-> rightVal
        ,leftHist |-> leftHist
        ,cancelEpoch |-> cancelEpoch
        ,rightHist |-> rightHist
        ,snapRev |-> snapRev
        ,mode |-> mode
        ,activeDep |-> activeDep
        ,snapResult |-> snapResult
        ,rev |-> rev
        ,chooseVal |-> chooseVal
        ,diskCache |-> diskCache
        ,modeChangedAt |-> modeChangedAt
        ,leftVal |-> leftVal
        ,leftChangedAt |-> leftChangedAt
        ,chooseChangedAt |-> chooseChangedAt
        ,prevCancelEpoch |-> prevCancelEpoch
        ,depObservedChangedAt |-> depObservedChangedAt
        ,rightChangedAt |-> rightChangedAt
        
        \* Put additional constant-, state-, and action-level expressions here:
        \* ,_stateNumber |-> _TEPosition
        \* ,_modeHistUnchanged |-> modeHist = modeHist'
        
        \* Format the `modeHist` variable as Json value.
        \* ,_modeHistJson |->
        \*     LET J == INSTANCE Json
        \*     IN J!ToJson(modeHist)
        
        \* Lastly, you may build expressions over arbitrary sets of states by
        \* leveraging the _TETrace operator.  For example, this is how to
        \* count the number of times a spec variable changed up to the current
        \* state in the trace.
        \* ,_modeHistModCount |->
        \*     LET F[s \in DOMAIN _TETrace] ==
        \*         IF s = 1 THEN 0
        \*         ELSE IF _TETrace[s].modeHist # _TETrace[s-1].modeHist
        \*             THEN 1 + F[s-1] ELSE F[s-1]
        \*     IN F[_TEPosition - 1]
    ]

=============================================================================



Parsing and semantic processing can take forever if the trace below is long.
 In this case, it is advised to uncomment the module below to deserialize the
 trace from a generated binary file.

\*
\*---- MODULE CascadeCore_TETrace ----
\*EXTENDS IOUtils, CascadeCore, TLC
\*
\*trace == IODeserialize("CascadeCore_TTrace_1784577174.bin", TRUE)
\*
\*=============================================================================
\*

---- MODULE CascadeCore_TETrace ----
EXTENDS CascadeCore, TLC

trace == 
    <<
    ([chooseChangedAt |-> -1,rightVal |-> 0,rev |-> 0,snapResult |-> NullResult,activeDep |-> "left",modeChangedAt |-> -1,cancelEpoch |-> 0,rightHist |-> (0 :> 0 @@ 1 :> 0 @@ 2 :> 0 @@ 3 :> 0 @@ 4 :> 0 @@ 5 :> 0),leftHist |-> (0 :> 0 @@ 1 :> 0 @@ 2 :> 0 @@ 3 :> 0 @@ 4 :> 0 @@ 5 :> 0),modeHist |-> (0 :> "left" @@ 1 :> "left" @@ 2 :> "left" @@ 3 :> "left" @@ 4 :> "left" @@ 5 :> "left"),mode |-> "left",rightChangedAt |-> -1,diskCache |-> {},leftChangedAt |-> -1,snapRev |-> -1,leftVal |-> 0,chooseVal |-> 0,depObservedChangedAt |-> -1,prevCancelEpoch |-> 0]),
    ([chooseChangedAt |-> -1,rightVal |-> 0,rev |-> 0,snapResult |-> NullResult,activeDep |-> "left",modeChangedAt |-> -1,cancelEpoch |-> 0,rightHist |-> (0 :> 0 @@ 1 :> 0 @@ 2 :> 0 @@ 3 :> 0 @@ 4 :> 0 @@ 5 :> 0),leftHist |-> (0 :> 0 @@ 1 :> 0 @@ 2 :> 0 @@ 3 :> 0 @@ 4 :> 0 @@ 5 :> 0),modeHist |-> (0 :> "left" @@ 1 :> "left" @@ 2 :> "left" @@ 3 :> "left" @@ 4 :> "left" @@ 5 :> "left"),mode |-> "left",rightChangedAt |-> -1,diskCache |-> {},leftChangedAt |-> -1,snapRev |-> 0,leftVal |-> 0,chooseVal |-> 0,depObservedChangedAt |-> -1,prevCancelEpoch |-> 0]),
    ([chooseChangedAt |-> 1,rightVal |-> 0,rev |-> 1,snapResult |-> NullResult,activeDep |-> "left",modeChangedAt |-> -1,cancelEpoch |-> 1,rightHist |-> (0 :> 0 @@ 1 :> 0 @@ 2 :> 0 @@ 3 :> 0 @@ 4 :> 0 @@ 5 :> 0),leftHist |-> (0 :> 0 @@ 1 :> 1 @@ 2 :> 0 @@ 3 :> 0 @@ 4 :> 0 @@ 5 :> 0),modeHist |-> (0 :> "left" @@ 1 :> "left" @@ 2 :> "left" @@ 3 :> "left" @@ 4 :> "left" @@ 5 :> "left"),mode |-> "left",rightChangedAt |-> -1,diskCache |-> {},leftChangedAt |-> 1,snapRev |-> 0,leftVal |-> 1,chooseVal |-> 1,depObservedChangedAt |-> 1,prevCancelEpoch |-> 0]),
    ([chooseChangedAt |-> 1,rightVal |-> 0,rev |-> 1,snapResult |-> 0,activeDep |-> "left",modeChangedAt |-> -1,cancelEpoch |-> 1,rightHist |-> (0 :> 0 @@ 1 :> 0 @@ 2 :> 0 @@ 3 :> 0 @@ 4 :> 0 @@ 5 :> 0),leftHist |-> (0 :> 0 @@ 1 :> 1 @@ 2 :> 0 @@ 3 :> 0 @@ 4 :> 0 @@ 5 :> 0),modeHist |-> (0 :> "left" @@ 1 :> "left" @@ 2 :> "left" @@ 3 :> "left" @@ 4 :> "left" @@ 5 :> "left"),mode |-> "left",rightChangedAt |-> -1,diskCache |-> {},leftChangedAt |-> 1,snapRev |-> 0,leftVal |-> 1,chooseVal |-> 1,depObservedChangedAt |-> 1,prevCancelEpoch |-> 0]),
    ([chooseChangedAt |-> 1,rightVal |-> 0,rev |-> 1,snapResult |-> 0,activeDep |-> "left",modeChangedAt |-> -1,cancelEpoch |-> 1,rightHist |-> (0 :> 0 @@ 1 :> 0 @@ 2 :> 0 @@ 3 :> 0 @@ 4 :> 0 @@ 5 :> 0),leftHist |-> (0 :> 0 @@ 1 :> 1 @@ 2 :> 0 @@ 3 :> 0 @@ 4 :> 0 @@ 5 :> 0),modeHist |-> (0 :> "left" @@ 1 :> "left" @@ 2 :> "left" @@ 3 :> "left" @@ 4 :> "left" @@ 5 :> "left"),mode |-> "left",rightChangedAt |-> -1,diskCache |-> {},leftChangedAt |-> 1,snapRev |-> 1,leftVal |-> 1,chooseVal |-> 1,depObservedChangedAt |-> 1,prevCancelEpoch |-> 0])
    >>
----


=============================================================================

---- CONFIG CascadeCore_TTrace_1784577174 ----
CONSTANTS
    MaxRev = 5
    ValueSet = { 0 , 1 , 2 }
    NullResult = NullResult
    NullResult = NullResult

INVARIANT
    _inv

CHECK_DEADLOCK
    \* CHECK_DEADLOCK off because of PROPERTY or INVARIANT above.
    FALSE

INIT
    _init

NEXT
    _next

CONSTANT
    _TETrace <- _trace

ALIAS
    _expression
=============================================================================
\* Generated on Mon Jul 20 14:52:55 CDT 2026