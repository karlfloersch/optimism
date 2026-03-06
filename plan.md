# Strong Consistency Plan for Supernode Interop

## Summary

Add a synchronous L1 consistency gate to `op-supernode` interop so we never commit a new `VerifiedResult` unless its L1 view is still canonical.

The recommended implementation is:

1. Use the existing shared supernode L1 client as the canonical L1 oracle.
2. Check stored verified state for L1 reorgs before progressing.
3. Check the next frontier's per-chain L1 sources against canonical L1 before loading logs / verifying messages.
4. Rewind interop-local state together when L1 inconsistency is detected:
   - `verifiedDB`
   - per-chain `logsDB`
   - per-chain denylist
5. Do not rewind chain engines as part of this L1-consistency repair path.

This keeps the interop DB consistent by construction, without adding a new async repair loop as the primary mechanism.

This document is a working implementation plan, not a rigid spec. We should keep the core invariants fixed and allow the exact function boundaries or helper shapes to change if the code reveals a simpler path during implementation.

## Double-Check of Issue / Handoff

The issue is directionally right, but two parts should change in the implementation plan:

1. We do not need a new L1 header plumbing story first.
   The supernode already creates a shared L1 client in `op-supernode/supernode/supernode.go` and passes that same client into all chain containers. `sources.L1Client` already exposes `L1BlockRefByNumber`, and `eth.L1BlockRef` already includes `ParentHash`.

2. Rewinding only `verifiedDB` is not enough.
   `loadLogs()` can reuse already-sealed `logsDB` blocks if they are still present. If we rewind verified timestamps after an L1 reorg but leave later `logsDB` contents in place, the next verification round can read stale blocks/logs from the old fork. The rewind step needs to move `logsDB` back to the same last-consistent timestamp, or clear it if no consistent timestamp remains.

3. The repair path should stay local to interop state.
   We do not want to route L1-consistency repair through `InvalidateBlock()` / engine rewind machinery. The op-nodes already handle their own L1 reorgs. The interop fix is to rewind accepted cross-chain evidence and wait until the chains become consistent again.

Because we already have a canonical L1 oracle, v1 does not need a general "are these two arbitrary non-canonical blocks on the same fork?" API. It is enough to check whether each relevant L1 block still matches the canonical block at that block number.

## Recommended Design

### Canonicality Model

Use `L1BlockRefByNumber` as the source of truth for canonical L1.

For any stored or frontier `eth.BlockID{Hash, Number}`:

- Fetch canonical `eth.L1BlockRef` at `Number`
- Compare `canonical.Hash` to `block.Hash`
- If they differ, the block is non-canonical

This is stronger and simpler than pairwise ancestry checks:

- If two blocks are both canonical at their respective numbers, they are on the same canonical chain.
- If a stored `L1Inclusion` is canonical, it is automatically consistent with later canonical frontier blocks.
- `sources.L1Client.L1BlockRefByNumber` is already intentionally not cached by number across reorgs, which is exactly what we want here.

`ParentHash` is still useful if we later want a deeper ancestry helper, but it is not required for the first strong-consistency patch.

### Round-Local L1 Snapshot

To make the consistency logic easy to test, gather L1 facts for a round into a small in-memory snapshot and run the core consistency logic against that snapshot rather than against live RPC calls.

A good normalized shape is:

```go
type L1Snapshot struct {
    RefsByHash            map[common.Hash]eth.L1BlockRef
    CanonicalHashByNumber map[uint64]common.Hash
}
```

Interpretation:

- `RefsByHash` is the set of L1 refs known for the current round
- `CanonicalHashByNumber` is the canonical-chain index for the numbers we care about

This supports both checker modes:

- v1 by-number canonical check
- later ancestry-walk check with a hash-indexed parent walk

For the first patch, it is acceptable to populate only the canonical-by-number subset that is actually needed. The snapshot can grow if and when ancestry-walk mode is added.

### Gate Placement

Add two synchronous gates:

1. `progressInterop()` start-of-round reorg gate
   - Check whether the latest stored `VerifiedResult.L1Inclusion` is still canonical.
   - If not, scan backward one timestamp at a time to the latest stored result whose `L1Inclusion` is canonical.
   - Once that boundary is found, apply one coordinated rewind of interop-local state.
   - Return early and let the next loop iteration restart from the repaired state.

2. Frontier canonicality gate
   - After `checkChainsReady(ts)` but before `loadLogs(ts)`, collect the per-chain optimistic L1 sources for that timestamp.
   - Require each of those L1 blocks to still be canonical at its number.
   - If any chain reports a stale/non-canonical L1 block, return early and wait for the chains to catch up.

### Freeze the Frontier Once

Today the code asks chains for optimistic L1 data inside `l1Inclusion()`.
That should change.

Within a single round:

- Collect frontier L2 heads once
- Collect frontier per-chain L1 sources once
- Run the frontier consistency gate against that exact set
- Pass that exact set through to `l1Inclusion()` / `verifyInteropMessages()`

This avoids extra `OptimisticAt()` calls later in the round and reduces intra-round TOCTOU behavior.

### Imperative Shell, Pure Consistency Kernel

Keep `progressInterop()` as the effectful orchestrator, but pull the consistency logic into small pure functions that can be hammered in tests.

Recommended split:

- impure collection:
  - read verified history from `verifiedDB`
  - read frontier L2/L1 observations from chains
  - build round-local `L1Snapshot`
- pure consistency decisions:
  - `DecideRepairBoundary(...)`
  - `CheckFrontierConsistency(...)`
  - `MaxL1Inclusion(...)`
- impure application:
  - rewind `verifiedDB`
  - rewind or clear `logsDB`
  - rewind or clear the denylist
  - load logs
  - commit verified result

This keeps the difficult state transitions visible in the real control flow, while making the decision logic deterministic and easy to test.

### Minimal-Change Strategy

To minimize both code churn and test surface area, the first patch should reuse the existing interop persistence and rewind style as much as possible.

Specifically:

- use the shared L1 client that already exists in the supernode
- add a pre-step in `progressInterop()` rather than redesigning the whole loop
- scan backward through verified timestamps one at a time
- apply one direct rewind once the retained boundary is found
- reuse `VerifiedDB.RewindAfter(...)`
- reuse `LogsDB.Rewind(...)` / `Clear(...)`
- add denylist rewind primitives analogous to the existing DB rewind style
- do not call `InvalidateBlock()` or `RewindEngine()` for L1-consistency repair

This keeps the new behavior concentrated in interop, instead of expanding the patch into engine or derivation reset behavior.

## Implementation Plan

## Phases

### Phase 1: Foundations

Goal:

- establish the checker abstraction and the round-local L1 snapshot shape
- make the core consistency logic easy to test before wiring it into the full interop loop

Scope:

- add the narrow L1 source interface to interop
- plumb the shared L1 client into interop
- add the `L1Snapshot` collection helper
- add the pure decision helpers:
  - `DecideRepairBoundary(...)`
  - `CheckFrontierConsistency(...)`
  - `MaxL1Inclusion(...)`
- add unit tests for the pure helpers and snapshot building

Expected outcome:

- no major behavior change yet, but the structure for the strong-consistency logic exists and is easy to hammer in tests

### Phase 2: Stored-State Repair Gate

Goal:

- make previously committed interop state self-consistent with canonical L1 before any new round proceeds

Scope:

- add the start-of-round repair check in `progressInterop()`
- check the latest stored `VerifiedResult.L1Inclusion` against canonical L1
- scan backward one timestamp at a time to the last consistent verified result
- rewind `verifiedDB`, `logsDB`, and denylist together
- clear interop state entirely if no consistent verified result remains
- reset `currentL1` on repair
- do not rewind chain engines as part of this path

Expected outcome:

- interop never continues building on top of known stale verified state

### Phase 3: Frozen Frontier Gate

Goal:

- ensure the next round uses one coherent frontier and refuses to progress on stale L1 views

Scope:

- freeze `blocksAtTimestamp` once per round
- collect per-chain optimistic L1 sources once per round
- build the round-local L1 snapshot for the frontier numbers
- run `CheckFrontierConsistency(...)` before `loadLogs()`
- return `Wait` / no progress if any frontier chain is stale or inconsistent
- compute `l1Inclusion()` from the frozen frontier instead of refetching
- do not advance `currentL1` on a consistency stall

Expected outcome:

- interop commits only from a coherent frontier and never mixes observations from multiple rounds

### Phase 4: Hardening and Follow-Through

Goal:

- tighten edge behavior, document assumptions, and leave the structure ready for a stronger checker later

Scope:

- add read-path tests and document the `VerifiedBlockAtL1()` assumptions
- add metrics/logging for stalls and rewinds if useful
- clean up names and helper boundaries that become obvious during implementation
- leave the `L1ConsistencyChecker` abstraction ready for a later ancestry-walk implementation

Expected outcome:

- the first strong-consistency patch is complete and the path to a future ancestry-walk checker is straightforward

### 1. Plumb the L1 source into interop

Files:

- `op-supernode/supernode/activity/interop/interop.go`
- `op-supernode/supernode/supernode.go`
- `op-supernode/supernode/activity/interop/interop_test.go`

Changes:

- Add a narrow L1 source interface in the interop package:
  - `L1BlockRefByNumber(context.Context, uint64) (eth.L1BlockRef, error)`
- Add this field to `Interop`
- Extend `interop.New(...)` to accept the shared L1 source
- Pass `s.l1Client` from `supernode.New(...)`
- Update interop tests/harness to inject a mock L1 source

Recommendation:

- Keep the interface narrow and local to interop.
- Do not add a new method to `ChainContainer` for this first patch.

### 2. Add L1 consistency helpers

Files:

- `op-supernode/supernode/activity/interop/interop.go`
- optionally a new helper file like `op-supernode/supernode/activity/interop/l1_consistency.go`

Helpers:

- `canonicalL1At(number uint64) (eth.L1BlockRef, error)`
- `isCanonicalL1Block(block eth.BlockID) (bool, eth.L1BlockRef, error)`
- `collectL1Snapshot(...)`
- `DecideRepairBoundary(...)`
- `CheckFrontierConsistency(...)`

Behavior:

- Treat L1 RPC failures as transient errors
- Treat hash mismatch at the same number as inconsistency, not fatal corruption

### 3. Add a repair path for stored verified state

Files:

- `op-supernode/supernode/activity/interop/interop.go`
- `op-supernode/supernode/activity/interop/verified_db.go`
- `op-supernode/supernode/activity/interop/logdb.go`

Add a helper with behavior roughly like:

- Read latest verified timestamp/result
- If none exists, continue normally
- If latest result is canonical, continue normally
- Otherwise scan backward one timestamp at a time until finding the last canonical result
- Rewind `verifiedDB` to that boundary
- Rewind every chain `logsDB` to the matching `VerifiedResult.L2Heads[chainID]`
- Rewind every chain denylist to the matching `VerifiedResult.L2Heads[chainID].Number`
- If no canonical result remains, clear all `logsDB`s, clear all denylist state, and rewind/clear `verifiedDB`
- Reset `currentL1`
- Return early without attempting new interop progress in the same round

Important detail:

- Rewind `logsDB` using the `L2Heads` from the last consistent verified result, not via block invalidation machinery.
- Rewind denylist entries per chain with the rule: keep `height <= keptHead.Number`, delete `height > keptHead.Number`.
- This is interop-local state repair, not a chain-container invalidation / engine rewind event.

Pure decision shape:

- input:
  - verified results plus whether each result's `L1Inclusion` is consistent with the round-local L1 snapshot
- output:
  - `NoRepair`
  - `RewindTo(timestamp)`
  - `ClearAll`

### 4. Add a frontier canonicality gate

Files:

- `op-supernode/supernode/activity/interop/interop.go`
- `op-supernode/supernode/activity/interop/algo.go`

Refactor the round shape to:

1. Determine next timestamp
2. Repair stored state if latest verified L1 is non-canonical
3. `checkChainsReady(ts)`
4. Collect frontier per-chain optimistic L1 sources once
5. Verify each frontier L1 block is canonical
6. `loadLogs(ts)`
7. `verifyInteropMessages(...)`
8. cycle verification
9. commit

If frontier L1 canonicality fails:

- Return `Result{}` / no progress
- Do not invalidate L2 blocks
- Let the next round retry after chains catch up

Pure decision shape:

- input:
  - prior verified anchor if present
  - per-chain frontier observations
  - round-local L1 snapshot
- output:
  - `Proceed` with frozen `L1Inclusion`
  - `Wait`

### 5. Stop recomputing L1 inclusion from fresh RPC reads

Files:

- `op-supernode/supernode/activity/interop/algo.go`
- `op-supernode/supernode/activity/interop/interop.go`

Refactor `l1Inclusion()` so it works from pre-collected frontier L1 block IDs rather than calling `OptimisticAt()` again.

That means:

- The max-L1-number calculation remains the same
- The chosen `L1Inclusion` is derived from the already-checked frontier
- We avoid mixing different frontier snapshots within one verification round

### 6. Preserve current failure semantics

For the first patch:

- L1 inconsistency in stored verified state: rewind and return early
- L1 inconsistency in current frontier: return early and retry later
- L1 RPC error: return error, let the main loop back off

Do not:

- Add async self-healing as the primary mechanism
- Denylist blocks purely because their L1 source is temporarily stale/non-canonical
- Rewind engines as part of the L1-consistency repair path
- Introduce a new cross-component lock unless the implementation proves it is necessary

## Test Plan

### Keystone integration suite

Start with a small multi-chain keystone suite that exercises the full repaired control flow with heterogeneous chains.

1. `TestInterop_L1Reorg_RewindsVerifiedLogsAndDenylist_ToLastConsistentPrefix`
   - at least two chains
   - different block times and different retained L2 block numbers at the same interop timestamp
   - latest verified suffix becomes stale
   - interop scans backward one timestamp at a time
   - interop rewinds `verifiedDB`, per-chain `logsDB`, and per-chain denylist to the retained prefix
   - no engine rewind is triggered by this repair path

2. `TestInterop_UsesFrozenFrontierForEntireRound`
   - the round freezes the candidate frontier once
   - later RPC answers changing do not alter the round result
   - stale frontier causes wait/no-progress instead of a commit

3. `TestInterop_L1Reorg_ClearsStateWhenNoConsistentPrefixRemains`
   - no retained verified prefix exists
   - `verifiedDB`, `logsDB`, and denylist are cleared
   - the next round restarts from activation behavior

These tests should be the soundness anchor for the whole patch.

### Pure consistency tests

Add direct table tests for the pure decision functions with no mocks:

1. `DecideRepairBoundary`
   - latest result consistent => `NoRepair`
   - latest inconsistent, previous consistent => `RewindTo(previousTS)`
   - multiple inconsistent tail results => `RewindTo(lastGoodTS)`
   - no consistent result remains => `ClearAll`

2. `CheckFrontierConsistency`
   - all frontier L1 refs canonical => `Proceed`
   - one stale frontier L1 ref => `Wait`
   - empty or malformed frontier input => deterministic failure mode

3. `MaxL1Inclusion`
   - returns max-number block from frozen frontier set
   - preserves exact chosen block hash for the max-number entry

These should be easy to fuzz later because they are pure and only depend on input data.

### L1 snapshot / checker tests

Add focused tests around:

- canonical match at same number
- canonical mismatch at same number
- transient L1 RPC failure
- snapshot contains only the required canonical numbers for v1
- future-facing: ancestry walk can be added against the same snapshot shape

Use `op-service/testutils.MockL1Source`.

### Interop progression tests

Add or extend tests for:

1. Latest verified result reorged
   - stored latest `L1Inclusion` no longer matches canonical
   - interop rewinds to previous consistent timestamp
   - `currentL1` is reset

2. Latest verified result reorged all the way past activation
   - no canonical verified result remains
   - verified DB is cleared
   - logs DBs are cleared
   - denylist state is cleared

3. Frontier chain is stale on old L1 fork
   - `checkChainsReady()` succeeds
   - frontier canonicality gate fails
   - interop returns no progress
   - no block invalidation is triggered

4. Canonical frontier succeeds
   - interop proceeds and commits normally
   - committed `L1Inclusion` still uses max L1 number from the checked frontier

5. Rewind repairs both DB layers
   - after repair, next round does not reuse stale `logsDB` blocks beyond the rewind point

6. Rewind repairs denylist by retained per-chain head
   - entries at heights `<= keptHead.Number` remain
   - entries at heights `> keptHead.Number` are deleted
   - at least one test should cover different retained block numbers across chains

7. Frozen frontier is used for the entire round
   - `checkChainsReady()` and frontier collection happen once
   - later steps do not refetch a different L2/L1 pair for the same timestamp

8. `CurrentL1` does not advance on consistency stall
   - frontier gate returns `Wait`
   - `currentL1` remains unchanged

### Wiring tests

Add a small constructor/wiring test to ensure `supernode.New(...)` passes the shared L1 client into interop.

### Read-path tests

For the first patch, keep `VerifiedBlockAtL1()` number-based and document the assumption that the caller provides a canonical/finalized L1 anchor.

Add tests that lock in the intended behavior:

- canonical finalized L1 anchor returns the latest verified block at or before that L1 number
- if no verified result exists at or before that number, return empty
- do not add a new hash-mismatch behavior here unless we explicitly decide to change caller semantics

## Suggested Patch Order

1. Plumb L1 source into interop and update tests/harness
2. Add helper functions for canonicality checks
3. Add denylist rewind primitives
4. Add stored-state repair / rewind path
5. Add frontier collection + frontier canonicality gate
6. Refactor `l1Inclusion()` to use the pre-collected frontier
7. Add tests for regressions and edge cases

This order keeps the patch reviewable and lets us land the hard correctness logic before any cleanup.

## Non-Goals for the First Patch

- Async auditing / self-healing loop
- General ancestry checks between arbitrary non-canonical L1 blocks
- New chain-container APIs for L1 header access
- Performance optimization beyond what the shared L1 client cache already provides

## Follow-Ups

After the first correctness patch lands, follow-ups that may be worth adding:

- metrics/logging for L1-consistency stalls and rewinds
- a read-only audit mode that periodically verifies DB consistency
- a deeper ancestry helper if future work needs to compare two non-canonical fork candidates directly

## Acceptance Gates

The first patch is only done when all of the following are true:

1. A reorged stored `L1Inclusion` causes interop to rewind to the last consistent verified timestamp or clear state if none remains.
2. The repair path scans backward one timestamp at a time but applies one coordinated rewind to the chosen boundary.
3. The repair path rewinds `verifiedDB`, `logsDB`, and denylist together.
4. L1-consistency repair does not trigger engine rewind.
5. A stale/non-canonical frontier causes `progressInterop()` to make no progress and commit nothing.
6. A consistency stall does not advance `currentL1`.
7. `l1Inclusion()` is computed from the frozen frontier gathered for that round, not from fresh later RPC reads.
8. The keystone integration suite passes for heterogeneous chains with different block times / block numbers.
9. The pure consistency functions have direct table tests covering the main repair/proceed/wait/clear branches.
