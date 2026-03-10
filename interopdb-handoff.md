# InteropDB Refactor Handoff

## Current State

The interop runtime has already been migrated off the old imperative flow.

The live runtime now uses:

- a pure state machine in [engine](/home/main/op/optimism/strong-consistency-supernode/op-supernode/supernode/activity/interop/engine)
- an atomic state store in [store](/home/main/op/optimism/strong-consistency-supernode/op-supernode/supernode/activity/interop/store)
- a controller in [controller](/home/main/op/optimism/strong-consistency-supernode/op-supernode/supernode/activity/interop/controller)
- runtime adapters in [observe.go](/home/main/op/optimism/strong-consistency-supernode/op-supernode/supernode/activity/interop/observe.go), [runtime_controller.go](/home/main/op/optimism/strong-consistency-supernode/op-supernode/supernode/activity/interop/runtime_controller.go), and [interop.go](/home/main/op/optimism/strong-consistency-supernode/op-supernode/supernode/activity/interop/interop.go)

The old imperative runtime path is gone.
The old `VerifiedDB` and migration path are gone.

## Key Invariants Now Implemented

### State Machine

- authoritative interop state is persisted atomically
- state progresses one timestamp at a time
- each step may:
  - stay at the same timestamp
  - advance by one timestamp
  - rewind by one timestamp

### Deny State

- there is exactly one denied decision per timestamp
- one denied decision may contain multiple invalid heads across chains
- deny state lives authoritatively in `InteropState.DeniedByTS`
- the chain-local denylist is only a projection for op-node compatibility

### L1 Consistency

- the controller now runs a shared same-chain check across:
  - accepted `L1Heads`
  - frontier `L1Heads`
- the current implementation is a by-number checker using the shared L1 client
- this is the synchrony-assumption version, not the eventual ancestry-walk version

## Main Code Structure

### Pure Engine

Path:

- [engine/types.go](/home/main/op/optimism/strong-consistency-supernode/op-supernode/supernode/activity/interop/engine/types.go)
- [engine/step.go](/home/main/op/optimism/strong-consistency-supernode/op-supernode/supernode/activity/interop/engine/step.go)
- [engine/validate.go](/home/main/op/optimism/strong-consistency-supernode/op-supernode/supernode/activity/interop/engine/validate.go)

Responsibilities:

- validate `InteropState`
- compare accepted/frontier observations
- decide `Outcome`
- emit typed `Effect`s

### Atomic Store

Path:

- [store/store.go](/home/main/op/optimism/strong-consistency-supernode/op-supernode/supernode/activity/interop/store/store.go)
- [store/encoding.go](/home/main/op/optimism/strong-consistency-supernode/op-supernode/supernode/activity/interop/store/encoding.go)

Responsibilities:

- load/store full `InteropState`
- persist pending effects atomically with state

### Controller

Path:

- [controller/controller.go](/home/main/op/optimism/strong-consistency-supernode/op-supernode/supernode/activity/interop/controller/controller.go)

Responsibilities:

1. load state
2. drain pending effects
3. observe one coherent round
4. enforce L1 same-chain consistency on accepted + frontier heads
5. resolve frontier evidence
6. run verification
7. step the engine
8. persist new state
9. run emitted effects

### Runtime Adapters

Path:

- [observe.go](/home/main/op/optimism/strong-consistency-supernode/op-supernode/supernode/activity/interop/observe.go)
- [runtime_controller.go](/home/main/op/optimism/strong-consistency-supernode/op-supernode/supernode/activity/interop/runtime_controller.go)
- [checker.go](/home/main/op/optimism/strong-consistency-supernode/op-supernode/supernode/activity/interop/checker.go)

Responsibilities:

- adapt chain-container observations into `RoundObservation`
- fetch frontier evidence from live chains
- run legacy verification functions against explicit evidence
- map engine effects into:
  - chain invalidation
  - chain resets
  - deny pruning/clearing

## Tests That Were Green At Handoff

Unit/runtime:

```bash
go test ./op-supernode/... -count=1
```

Supernode acceptance:

```bash
go test ./op-acceptance-tests/tests/supernode/... -count=1 -timeout=20m
```

The acceptance sweep included:

- [tests/supernode/interop](/home/main/op/optimism/strong-consistency-supernode/op-acceptance-tests/tests/supernode/interop)
- [tests/supernode/interop/activation](/home/main/op/optimism/strong-consistency-supernode/op-acceptance-tests/tests/supernode/interop/activation)
- [tests/supernode/interop/follow_l2](/home/main/op/optimism/strong-consistency-supernode/op-acceptance-tests/tests/supernode/interop/follow_l2)
- [tests/supernode/interop/reorg](/home/main/op/optimism/strong-consistency-supernode/op-acceptance-tests/tests/supernode/interop/reorg)
- [tests/supernode/interop/same_timestamp_invalid](/home/main/op/optimism/strong-consistency-supernode/op-acceptance-tests/tests/supernode/interop/same_timestamp_invalid)

## Remaining Review Findings

### 1. Accepted L1 Inconsistency Handling

Current behavior:

- the controller same-chain check marks the round as `AvailabilityConflict`
- the engine turns accepted conflict into `OutcomeConflict`

Files:

- [controller/controller.go](/home/main/op/optimism/strong-consistency-supernode/op-supernode/supernode/activity/interop/controller/controller.go)
- [engine/step.go](/home/main/op/optimism/strong-consistency-supernode/op-supernode/supernode/activity/interop/engine/step.go)

Open design question:

- if accepted + frontier heads fail the same-chain check, should that mean:
  - `wait/conflict`
  - or `rewind`

Current best interpretation:

- if the accepted snapshot itself is no longer the accepted world, this should likely become a rewind trigger
- if the frontier alone is inconsistent, this should remain wait/conflict

This distinction is not implemented yet.

### 2. `logsDB` Is Still Slightly Too Trusted

Current risk:

- `loadLogsFromEvidence` skips reload when the cache has a block at the same or later height
- it does not prove that the cached same-height block hash matches the requested frontier block
- later verification can then misinterpret stale cache state as an invalid block instead of a cache conflict

Files:

- [logdb.go](/home/main/op/optimism/strong-consistency-supernode/op-supernode/supernode/activity/interop/logdb.go)
- [algo.go](/home/main/op/optimism/strong-consistency-supernode/op-supernode/supernode/activity/interop/algo.go)
- [runtime_controller.go](/home/main/op/optimism/strong-consistency-supernode/op-supernode/supernode/activity/interop/runtime_controller.go)

Recommended next step:

- push exact-block cache validation further toward the evidence resolver / cache layer
- treat stale cache mismatches as cache conflicts and rebuilds, not as invalid frontier blocks or fatal runtime errors

## Recommended Next Work

1. Split accepted-chain inconsistency from frontier inconsistency in the controller path.
2. Decide and implement:
   - accepted inconsistency => rewind
   - frontier inconsistency => wait/conflict
3. Harden `logsDB` so same-height hash mismatches trigger cache invalidation/rebuild.
4. Add tests specifically for:
   - accepted L1 inconsistency causing the intended outcome
   - stale same-height logs cache behavior

## Current Branch

Active branch:

- `karlfloersch/strong-consistency-supernode`

Latest work in this slice:

- `dc5b3be648` `interop: tighten deny state and l1 consistency`

## Important Intent

The current design goal is:

- one authoritative interop state
- explicit evidence input
- deterministic step logic
- no hidden correctness dependence on cache/database graph behavior

Any follow-up changes should preserve that direction.
