# InteropDB Refactor Plan

## Goal

Replace the current interop flow with a design that is:

- definitionally consistent given trusted input snapshots
- atomically persisted
- easy to unit test without timing-sensitive devstack orchestration
- explicit about side effects like engine rewinds

The core idea is:

- the caller provides trusted observations of chain state
- a pure state engine decides what the interop state should become
- a thin storage/execution layer applies those changes atomically and runs any required resets

This is not really "just a database". It is closer to:

- a **state machine** that owns interop invariants
- backed by a single **atomic store**

So the naming I recommend is:

- `InteropStateEngine` for the pure transition logic
- `InteropStore` for the atomic persisted state
- `InteropController` for the outer loop that gathers inputs and performs side effects

`InteropDB` is an acceptable umbrella term in conversation, but in code I would keep the distinction between the engine and the store.

## Trust Boundary

The new design should **not** try to independently prove the correctness of the OP-node snapshot inputs.

We will trust the supplied observations for:

- L2 head at timestamp
- corresponding L1 head
- chain-local reset completion

The engine is responsible for enforcing **internal consistency** of the interop view once those observations are supplied.

That means:

- if the caller lies, the engine can still be fooled
- if the caller supplies consistent observations, the engine should behave deterministically and safely

This is the right tradeoff because the testing problem is currently in the orchestration layer, not in expressing the interop invariants.

## High-Level Shape

Split the current interop package into 3 layers.

## Progression Rule

For simplicity and auditability, the state machine should obey a strict
single-step progression rule:

- stay at the same timestamp
- advance by exactly one timestamp
- rewind by exactly one timestamp

It should never jump forward or backward by more than one timestamp in a
single engine step.

That means:

- no "scan back to the newest valid prefix, then jump there" behavior inside
  the engine
- rewinds happen one timestamp at a time
- repeated rewinds are represented as repeated engine steps

This is intentionally more conservative than the previous design because it
gives the system a much simpler state transition graph and makes reasoning
about invariants easier for review and audit.

## Activation Timestamp

The plan should make activation behavior explicit, because it is the one place
where there is no previously accepted snapshot.

Rules:

- the controller is configured with a fixed `activationTimestamp`
- before activation, the engine must not accept or deny any frontier
- if `Accepted == nil`, the only legal frontier timestamp is
  `activationTimestamp`
- the first successful commit creates `Accepted.Timestamp == activationTimestamp`
- if the engine rewinds from `activationTimestamp`, the result is `Accepted ==
  nil`
- if `Accepted == nil`, deny decisions at or after `activationTimestamp` must be
  pruned as part of returning to the pre-activation state

This gives the state machine a clean initial state:

- pre-activation: `Accepted == nil`, `LastValidatedTS == nil`
- post-activation: accepted snapshots progress one timestamp at a time from
  `activationTimestamp`

It also makes the `activationTimestamp == 0` edge case explicit instead of
leaving it to ad hoc repair behavior.

### 1. Snapshot Source

An interface that hides all live chain queries.

Important constraint:

- the engine should not ask for accepted and frontier snapshots through separate
  unrelated live calls if those observations are intended to describe the same
  round
- the source should provide one coherent observation bundle for the engine step

That avoids reintroducing the same TOCTOU class of bug through the new design.

So instead of a very chatty source, I recommend one round-oriented API:

```go
type SnapshotSource interface {
    ObserveRound(ctx context.Context, acceptedTS *uint64, frontierTS uint64) (RoundObservation, error)
}
```

The concrete implementation may call:

- `LocalSafeBlockAtTimestamp`
- `OptimisticAt`
- sync-status APIs

Tests can provide synthetic snapshots directly.

`RoundObservation` should contain both:

- the accepted timestamp observation being revalidated
- the frontier observation being considered for the next step

so the engine sees one coherent view of the world per iteration.

A concrete first-pass shape should be something like:

```go
type RoundObservation struct {
    AcceptedTS uint64
    Accepted   SnapshotAvailability[AcceptedSnapshot]
    FrontierTS uint64
    Frontier   SnapshotAvailability[FrontierSnapshot]
}

type SnapshotAvailability[T any] struct {
    Present bool
    Reason  AvailabilityReason
    Value   T
}

type AvailabilityReason uint8

const (
    AvailabilityPresent AvailabilityReason = iota
    AvailabilityPreActivation
    AvailabilityNotReady
    AvailabilityConflict
)
```

This makes the observation self-describing:

- missing accepted snapshot because we are pre-activation
- missing frontier because the next timestamp is not ready yet
- missing snapshot because the source detected a conflict

### 2. Pure State Engine

This layer owns the interop invariants.

```go
type StepInput struct {
    Observation  RoundObservation
    Verification VerificationResult
}

type StepResult struct {
    NewState InteropState
    Effects  []Effect
    Outcome  Outcome
}
```

The engine should not talk to:

- bbolt
- op-node
- logsDB
- chain containers

It should only:

- compare snapshots
- decide whether to wait, commit, rewind, or prune
- emit explicit effects

It also should not perform verification itself. Verification should already
have been reduced to a plain `VerificationResult` input before `Step(...)`
runs.

One more important rule:

- the engine should be deterministic and idempotent for the same `(state,
  observation)` pair

That makes retries and crash recovery much easier to reason about.

`Outcome` should also be explicit, for example:

```go
type Outcome uint8

const (
    OutcomeNoOp Outcome = iota
    OutcomeWait
    OutcomeAdvance
    OutcomeRewind
    OutcomeConflict
)
```

That gives the controller a stable semantic contract instead of inferring
behavior from incidental effect combinations.

### 3. Atomic Store + Effect Runner

The store persists the entire interop state atomically in one place.

The controller loop becomes:

1. load current state
2. gather snapshot observations
3. run pure engine step
4. atomically persist `NewState`
5. execute emitted side effects in order

That gives us a single place where:

- accepted snapshot
- frontier deny decisions
- rewind boundary
- validated/read boundary

are all updated together.

### 4. Evidence Resolution

Verification should not walk a database graph directly.

Instead, the controller should resolve a plain **frontier evidence bundle** for
the exact frontier snapshot being checked, and pass that bundle into
verification.

That means the logical flow is:

1. collect the frontier snapshot
2. resolve all evidence needed for that exact frontier
3. verify the snapshot against that evidence bundle
4. hand the resulting valid/invalid decision to the engine

This is the main way to keep verification unit-testable while still allowing a
cache under the hood.

## Data Model

The state we want to make first-class is:

```go
type AcceptedSnapshot struct {
    Timestamp   uint64
    L1Inclusion eth.BlockID
    L1Heads     map[eth.ChainID]eth.BlockID
    L2Heads     map[eth.ChainID]eth.BlockID
}

type FrontierSnapshot struct {
    Timestamp   uint64
    L1Inclusion eth.BlockID
    L1Heads     map[eth.ChainID]eth.BlockID
    L2Heads     map[eth.ChainID]eth.BlockID
}

type DeniedDecision struct {
    Timestamp    uint64
    DeniedFrontier FrontierSnapshot
    InvalidHeads map[eth.ChainID]eth.BlockID
}

type InteropState struct {
    Accepted         *AcceptedSnapshot
    DeniedByTS       map[uint64][]DeniedDecision
    LastValidatedTS  *uint64
    PendingEffects   []PendingEffect
}
```

`AcceptedSnapshot` and `FrontierSnapshot` are structurally identical on purpose.
This is a semantic distinction, not a storage optimization:

- `AcceptedSnapshot` means "persisted state we currently trust"
- `FrontierSnapshot` means "candidate state we are evaluating"

If the duplication becomes noisy in implementation, we can collapse them to a
shared `Snapshot` type later, but the plan keeps both names because they make
the transition rules easier to read and audit.

The key refactor is:

- `VerifiedDB` and the denylist stop being separate first-class truth stores
- they become one atomic persisted interop state

We can still physically back that state with bbolt, but logically it is one store.

The important non-state data structure is:

```go
type FrontierEvidence struct {
    Timestamp uint64
    Blocks    map[eth.ChainID]BlockEvidence
}

type BlockEvidence struct {
    Block              eth.BlockID
    ExecutingMessages  []ExecutingMessage
    InitiatingEvidence map[MessageKey]InitiatingMessageEvidence
}
```

This evidence is not authoritative persisted state. It is an input object for
verification.

The verifier should return plain data too, for example:

```go
type VerificationResult struct {
    Timestamp    uint64
    Status       VerificationStatus
    InvalidHeads map[eth.ChainID]eth.BlockID
}

type VerificationStatus uint8

const (
    VerificationValid VerificationStatus = iota
    VerificationInvalid
    VerificationNotReady
    VerificationConflict
)
```

This is intentionally richer than a simple "invalid heads or not" result.
The controller/engine should be able to distinguish:

- fully valid frontier
- invalid frontier with explicit invalid heads
- not-ready / incomplete evidence
- hard conflict or corrupted evidence/cache state

Invariant:

- if `Status == VerificationInvalid`, then `InvalidHeads` must be non-empty
- if `Status != VerificationInvalid`, then `InvalidHeads` should be empty

## Invariants To Make First-Class

These are the invariants the new engine should own directly.

### Accepted Snapshot Invariants

- there is at most one accepted snapshot for the latest timestamp
- accepted timestamps are sequential
- accepted snapshots are immutable once superseded, except through explicit rewind
- `L1Inclusion == max(L1Heads)`
- accepted timestamp movement per step is in `{-1, 0, +1}`
- if `Accepted != nil`, then `LastValidatedTS != nil` and `*LastValidatedTS == Accepted.Timestamp`
- if `Accepted == nil`, the next legal frontier timestamp is `activationTimestamp`

### Frontier / Retry Invariants

- frontier timestamp must equal `accepted.Timestamp + 1` or the activation timestamp
- a frontier snapshot must be self-consistent before it can be committed
- if the previously accepted snapshot no longer matches observations, the engine must rewind before making progress
- if only the frontier changed, the engine must not silently reuse stale deny decisions for that frontier
- observations must be self-describing even when partial:
  - accepted snapshot may be absent when `Accepted == nil`
  - frontier snapshot may be absent or marked unavailable when the next timestamp is not yet ready

### Deny Invariants

- deny decisions are scoped to a specific frontier timestamp
- deny decisions are only valid relative to the exact frontier snapshot that produced them
- deny decisions should compare by exact snapshot equality, not loose timestamp/height matching
- `DeniedByTS[t]` may contain multiple decisions over time, but only decisions whose
  `DeniedFrontier` exactly matches the current frontier at `t` may remain live
- if accepted state rewinds past a deny decision timestamp, that deny decision must be pruned
- if the current frontier at the same timestamp differs from the denied frontier, that deny decision must be pruned

### Reset Invariants

- every rewind/reset target must be explicit in the emitted effects
- the controller must not claim progress until the reset effect completes synchronously
- state mutation and effect planning must happen before side effects execute
- effects must be safe to retry after crash or controller restart
- the engine must not emit duplicate rewind/reset effects if an equivalent pending effect already exists
- before stepping the engine, the controller must drain or reconcile any retry-safe pending effects

## Effects

The pure engine should emit typed effects instead of directly mutating live systems.

```go
type Effect interface {
    isEffect()
}

type RewindAcceptedState struct {
    ToTimestamp uint64
}

type ResetChainToAccepted struct {
    ChainID    eth.ChainID
    Timestamp  uint64
    L2Head     eth.BlockID
}

type PruneDeniedDecisions struct {
    AfterTimestamp uint64
}

type PruneFrontierDeniedDecisions struct {
    Timestamp uint64
}

type PendingEffect struct {
    ID     string
    Effect Effect
}
```

The controller can then map those effects to:

- engine rewinds
- logsDB rewinds
- store updates

without hiding the decision logic in side-effecting code.

Under the single-step progression rule, `RewindAcceptedState` may only target
the immediately previous timestamp.

Each effect should also have a stable identity so the controller can safely
retry after crash or restart.

The `ID` should be deterministic from the logical action, for example:

- effect type
- target timestamp
- target chain
- target head hash

That makes deduplication and retry semantics much easier to reason about.

For deny pruning, the engine should prefer replacement semantics over unbounded
append growth:

- when frontier `t` changes, stale denied decisions for `t` should be removed
- at most the currently matching denied frontier(s) for `t` should remain live

## Evidence Cache

What is currently thought of as `logsDB` should become an `EvidenceCache`.

It should be treated as:

- a rebuildable cache
- keyed by exact block identity
- not part of the authoritative interop state

Suggested interface:

```go
type EvidenceCache interface {
    Get(chainID eth.ChainID, block eth.BlockID) (BlockEvidence, bool, error)
    Put(chainID eth.ChainID, block eth.BlockID, ev BlockEvidence) error
    ClearChain(chainID eth.ChainID) error
    ClearAfter(chainID eth.ChainID, blockNum uint64) error
}
```

Important rule:

- cache hits must be verified against the requested exact `eth.BlockID`
- any mismatch is a hard cache conflict
- on conflict, the controller should clear or trim the affected cache and retry

So correctness comes from:

- trusted frontier snapshot input
- exact block identity matching
- explicit evidence bundle construction

not from trusting the cache blindly.

This means the verifier should consume `FrontierEvidence`, not read from the
cache directly.

The controller should treat `VerificationConflict` as a hard evidence/cache
problem:

- clear or trim the affected cache
- do not advance state
- retry with freshly resolved evidence

## Storage Plan

The store should commit a full `InteropState` transactionally.

Two implementation options:

### Option A: Single bbolt DB / bucket set

Keep using bbolt, but collapse persistence into one store package with one transaction boundary.

Pros:

- smallest migration from current code
- easy to atomically update accepted snapshot + deny decisions together

Cons:

- still a persistence-oriented API unless we are disciplined

### Option B: In-memory engine state + append/replace persistence record

Persist one canonical encoded state blob and load it fully on start.

Pros:

- simplest semantics
- easiest to reason about atomically

Cons:

- less incremental

I would start with **Option A**, but with a strict API that behaves like a single-state store, not like multiple independent tables.

## Suggested Package Layout

Something like:

```text
op-supernode/supernode/activity/interop/state/
    engine.go
    types.go
    invariants.go
    effects.go
    engine_test.go

op-supernode/supernode/activity/interop/store/
    store.go
    bolt_store.go
    store_test.go

op-supernode/supernode/activity/interop/controller/
    controller.go
    snapshot_source.go
    effect_runner.go
```

Or, if we want less package overhead at first:

```text
op-supernode/supernode/activity/interop/engine/
op-supernode/supernode/activity/interop/store/
```

I would not keep adding more logic directly into the current `interop.go`.

## Concrete Refactor Phases

### Phase 1: Freeze the State Model

Define:

- `AcceptedSnapshot`
- `FrontierSnapshot`
- `DeniedDecision`
- `InteropState`
- `Effect`
- `Outcome`

No behavior changes yet.

Deliverable:

- types and serialization definitions
- golden/unit tests around encoding and equality

### Phase 2: Build the Pure Engine

Implement:

```go
func Step(state InteropState, input StepInput) (StepResult, error)
```

The engine should handle:

- accepted snapshot still valid -> continue
- accepted snapshot stale -> emit a one-timestamp rewind/prune/reset step
- frontier inconsistent -> wait
- frontier valid + invalid heads -> add denied decision from `VerificationResult`
- frontier valid + no invalid heads -> advance accepted snapshot

Deliverable:

- table tests
- fuzz/property tests
- no chain-container dependencies

### Phase 3: Atomic Store

Implement one store transaction boundary:

- load state
- persist new state
- record pending effects and their completion

Deliverable:

- no more independent `VerifiedDB` + denylist mutation logic at the controller level
- one store API with atomic update semantics
- explicit crash-recovery semantics for pending effects

### Phase 4: Controller Shell

Rebuild the outer interop loop to:

- collect observations from `SnapshotSource`
- resolve evidence for the frontier snapshot
- run verification to produce `VerificationResult`
- call `Step`
- persist `NewState`
- execute effects synchronously

Deliverable:

- old `progressInterop` shrinks dramatically
- reset behavior is explicit and easier to test

### Phase 5: Compatibility / Migration

Adapt existing read-side APIs:

- `VerifiedBlockAtL1`
- `LatestVerifiedL2Block`
- superauthority hooks

These should read from the new accepted snapshot state instead of bespoke DB logic.

### Phase 6: Test Strategy

The new test pyramid should be:

- many pure engine unit tests
- store atomicity tests
- a few controller integration tests
- only a small number of acceptance tests

The acceptance tests should only prove:

- snapshots are gathered correctly from live nodes
- resets actually execute
- the system converges end to end

They should not be the primary place where invariants are validated.

## Verify During Implementation

When implementing the plan, explicitly verify these assumptions instead of
quietly inheriting them from the current code:

- `ObserveRound(...)` can actually provide one coherent round view from the
  current chain-container APIs
- `LatestVerifiedL2Block` and `VerifiedBlockAtL1` can be served cleanly from the
  new accepted-state model without bespoke fallback logic
- the controller can drain pending rewind/reset effects synchronously without
  deadlocking the current reset callback path
- `VerificationNotReady` and `VerificationConflict` map cleanly to distinct
  controller behavior
- the evidence cache can be keyed by exact `eth.BlockID` without relying on the
  current logsDB layout
- the one-step rewind rule remains operationally acceptable under repeated
  reorgs

## What To Unit Test Aggressively

The pure engine should have table-driven tests for:

- commit valid frontier
- reject inconsistent frontier
- rewind stale accepted snapshot by exactly one timestamp
- prune deny decisions after rewind
- prune deny decisions when frontier timestamp is retried with a different frontier
- reset effect emission for chains affected by pruned deny decisions
- no-op behavior when observations match state exactly

And fuzz tests for:

- monotonic timestamp progression
- repeated frontier changes at the same timestamp
- repeated one-step rewinds then re-advance
- deny prune idempotence

## Recommended Design Constraint

The controller should not be allowed to "partially apply" an engine decision.

That means:

- all state mutation for a step should be represented in one `NewState`
- all side effects should be emitted explicitly
- if side effects fail, the next controller iteration should re-enter from a durable, well-defined state
- if side effects were persisted as pending but not completed, the controller should retry them idempotently before asking the engine to step again

This is the main reason to treat the design as a state machine plus store, not as a handful of loosely related DB helpers.

Similarly, the verifier should not be allowed to implicitly pull more state
from cache/storage during a verification step. It should receive the frontier
evidence bundle as an explicit input.

Similarly, `ObserveRound(...)` should return one coherent observation object
that fully describes what is and is not available for this step, rather than
forcing the engine/controller to infer readiness from missing side-channel
calls.

## Proposed First Slice

The first implementation slice should be:

1. define `InteropState`, `AcceptedSnapshot`, `FrontierSnapshot`, `DeniedDecision`, `RoundObservation`
2. define `FrontierEvidence` and the `EvidenceCache` interface
3. define effect types and pending-effect persistence model
4. implement pure `Step`
5. write a large table-test suite around `Step`
6. only then adapt the current interop loop to feed `Step`

That gives us the maximum testing value before we touch devstack or acceptance orchestration again.

## Open Questions

- Should denied decisions persist the full frontier snapshot, or a compact digest/reference?
  - I would start with the full snapshot for clarity.

- Should the store keep historical accepted snapshots or only the latest accepted snapshot plus rollback boundary?
  - I would keep enough history to rewind by timestamp deterministically.

- Should reset effects be applied before or after persisting new state?
  - I would persist the intended new state first, along with pending effects, then execute reset effects, and make the next iteration able to retry or reconcile partial external progress.

- Do we want the engine to own logsDB rewind planning too, or leave logsDB as a controller concern?
  - I would keep the evidence cache as a controller concern. The engine should
    emit rewind boundaries, and the controller should map those to cache
    trimming/clearing.

## Recommendation

Proceed with a state-machine refactor, not another incremental patch to the current interop loop.

The right abstraction is:

- **pure engine**
- **atomic store**
- **thin controller**

That is the shortest path to:

- definitionally enforced invariants
- real unit-testability
- fewer timing-sensitive acceptance tests
