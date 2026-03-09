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

### 1. Snapshot Source

An interface that hides all live chain queries:

```go
type SnapshotSource interface {
    AcceptedSnapshot(ctx context.Context, ts uint64) (AcceptedSnapshot, error)
    FrontierSnapshot(ctx context.Context, ts uint64) (FrontierSnapshot, error)
}
```

The concrete implementation may call:

- `LocalSafeBlockAtTimestamp`
- `OptimisticAt`
- sync-status APIs

Tests can provide synthetic snapshots directly.

### 2. Pure State Engine

This layer owns the interop invariants.

```go
type EngineState struct {
    Accepted      *AcceptedSnapshot
    DenyEntries   []DeniedDecision
}

type StepInput struct {
    AcceptedObservation *AcceptedSnapshot
    FrontierObservation *FrontierSnapshot
}

type StepResult struct {
    NewState EngineState
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
    Frontier     FrontierSnapshot
    InvalidHeads map[eth.ChainID]eth.BlockID
}

type InteropState struct {
    Accepted         *AcceptedSnapshot
    DeniedByTS       map[uint64][]DeniedDecision
    LastValidatedTS  *uint64
}
```

The key refactor is:

- `VerifiedDB` and the denylist stop being separate first-class truth stores
- they become one atomic persisted interop state

We can still physically back that state with bbolt, but logically it is one store.

## Invariants To Make First-Class

These are the invariants the new engine should own directly.

### Accepted Snapshot Invariants

- there is at most one accepted snapshot for the latest timestamp
- accepted timestamps are sequential
- accepted snapshots are immutable once superseded, except through explicit rewind
- `L1Inclusion == max(L1Heads)`

### Frontier / Retry Invariants

- frontier timestamp must equal `accepted.Timestamp + 1` or the activation timestamp
- a frontier snapshot must be self-consistent before it can be committed
- if the previously accepted snapshot no longer matches observations, the engine must rewind before making progress
- if only the frontier changed, the engine must not silently reuse stale deny decisions for that frontier

### Deny Invariants

- deny decisions are scoped to a specific frontier timestamp
- deny decisions are only valid relative to the frontier snapshot that produced them
- if accepted state rewinds past a deny decision timestamp, that deny decision must be pruned
- if the current frontier at the same timestamp differs from the denied frontier, that deny decision must be pruned

### Reset Invariants

- every rewind/reset target must be explicit in the emitted effects
- the controller must not claim progress until the reset effect completes synchronously
- state mutation and effect planning must happen before side effects execute

## Effects

The pure engine should emit typed effects instead of directly mutating live systems.

```go
type Effect interface {
    isEffect()
}

type RewindAcceptedState struct {
    KeepTimestamp uint64
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
```

The controller can then map those effects to:

- engine rewinds
- logsDB rewinds
- store updates

without hiding the decision logic in side-effecting code.

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
- accepted snapshot stale -> emit rewind/prune/reset effects
- frontier inconsistent -> wait
- frontier valid + invalid heads -> add denied decision
- frontier valid + no invalid heads -> advance accepted snapshot

Deliverable:

- table tests
- fuzz/property tests
- no chain-container dependencies

### Phase 3: Atomic Store

Implement one store transaction boundary:

- load state
- persist new state
- mark applied/pruned deny decisions

Deliverable:

- no more independent `VerifiedDB` + denylist mutation logic at the controller level
- one store API with atomic update semantics

### Phase 4: Controller Shell

Rebuild the outer interop loop to:

- collect observations from `SnapshotSource`
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

## What To Unit Test Aggressively

The pure engine should have table-driven tests for:

- commit valid frontier
- reject inconsistent frontier
- rewind stale accepted snapshot
- prune deny decisions after rewind
- prune deny decisions when frontier timestamp is retried with a different frontier
- reset effect emission for chains affected by pruned deny decisions
- no-op behavior when observations match state exactly

And fuzz tests for:

- monotonic timestamp progression
- repeated frontier changes at the same timestamp
- rewind then re-advance
- deny prune idempotence

## Recommended Design Constraint

The controller should not be allowed to "partially apply" an engine decision.

That means:

- all state mutation for a step should be represented in one `NewState`
- all side effects should be emitted explicitly
- if side effects fail, the next controller iteration should re-enter from a durable, well-defined state

This is the main reason to treat the design as a state machine plus store, not as a handful of loosely related DB helpers.

## Proposed First Slice

The first implementation slice should be:

1. define `InteropState`, `AcceptedSnapshot`, `FrontierSnapshot`, `DeniedDecision`
2. implement pure `Step`
3. write a large table-test suite around `Step`
4. only then adapt the current interop loop to feed `Step`

That gives us the maximum testing value before we touch devstack or acceptance orchestration again.

## Open Questions

- Should denied decisions persist the full frontier snapshot, or a compact digest/reference?
  - I would start with the full snapshot for clarity.

- Should the store keep historical accepted snapshots or only the latest accepted snapshot plus rollback boundary?
  - I would keep enough history to rewind by timestamp deterministically.

- Should reset effects be applied before or after persisting new state?
  - I would persist the intended new state first, then execute reset effects, and make the next iteration able to reconcile partial external progress.

- Do we want the engine to own logsDB rewind planning too, or leave logsDB as a controller concern?
  - I would have the engine emit the rewind boundary and let the controller map that to logsDB operations.

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
