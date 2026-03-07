# Keystone Test Plan

## Goal

Add one production-shaped integration suite that proves the new strong-consistency behavior in the shared-supernode devstack:

1. previously accepted interop state is repaired when the underlying chain view changes
2. new frontier data waits instead of committing when the chains are not yet consistent
3. once the chains converge again, interop resumes and progresses normally

This should live in `op-acceptance-tests/tests/supernode/...`, not only in unit tests, so it exercises:

- sysgo backend
- shared `op-supernode`
- real L1 and L2 nodes
- real batchers
- real CL/EL reset behavior

The plan here is intentionally incremental. We can tighten or simplify it as we learn more from the first implementation.

## What Already Exists

### Shared supernode acceptance harness

The existing supernode acceptance tests already use the right backend:

- `op-acceptance-tests/tests/supernode/interop/init_test.go`
- `op-devstack/presets/twol2.go`
- `op-devstack/sysgo/system.go`

The relevant preset is:

- `presets.WithTwoL2SupernodeInterop(delaySeconds)`
- `presets.NewTwoL2SupernodeInterop(t, delaySeconds)`

That preset already gives us:

- two L2 chains backed by one shared `op-supernode`
- a real sysgo L1
- a deterministic `TestSequencer`
- `ControlPlane`
- `PauseInterop` / `ResumeInterop` test control on the supernode
- batcher start/stop control

### Existing supernode-side scenarios

The current supernode acceptance tests already cover useful pieces:

- `op-acceptance-tests/tests/supernode/interop/head_progression_test.go`
  - pause/resume interop
  - batcher stop/start to force local-safe lag
  - assertions on `CrossSafe`, `Finalized`, and EL labels

- `op-acceptance-tests/tests/supernode/interop/reorg/invalid_message_reorg_test.go`
  - invalid interop message
  - L2 reset / replacement block behavior
  - observing a block-number-preserving reorg on the EL

Those are good templates, but they do not yet cover the new accepted-state repair logic after a genuine L1-side divergence.

### Existing L1 reorg patterns in devstack acceptance tests

There are already acceptance tests in the repo that trigger a real L1 reorg under sysgo:

- `op-acceptance-tests/tests/interop/reorgs/l2_reorgs_after_l1_reorg_test.go`
- `op-acceptance-tests/tests/sync/follow_l2/sync_test.go`

The key pattern is:

1. stop L1 fake-PoS with `ControlPlane.FakePoSState(..., stack.Stop)`
2. use `TestSequencer.SequenceBlock` on the L1 chain, optionally off a non-head parent
3. restart fake-PoS so the alternative branch becomes canonical
4. wait for:
   - `L1EL.ReorgTriggered(...)`
   - `L2EL.ReorgTriggered(...)`

That means a real L1 reorg is feasible in the devstack-based supernode tests. We do not need a separate lower-level harness just to create the divergence.

## Recommended Keystone Suite

Use a small suite, not one giant test file with every behavior mixed together.

### 1. Primary keystone

Suggested name:

- `TestSupernodeStrongConsistency_L1Reorg_RepairsWaitsAndRecovers`

This is the main end-to-end story.

It should prove, in one test:

1. interop validates through timestamp `T`
2. an L1 reorg invalidates the previously accepted world at or before `T`
3. the supernode repairs accepted state
4. while the post-reorg frontier is not yet consistent, interop does not advance
5. once the chains recover, interop validates again and continues forward

### 2. Secondary frontier-only guardrail

Suggested name:

- `TestSupernodeStrongConsistency_FrontierLag_WaitsWithoutRepair`

This is the simpler synchronous-gate test.

It should prove:

1. accepted state remains good
2. one chain lags in local-safe
3. the next timestamp is not validated
4. no repair occurs
5. once the lagging chain catches up, validation resumes

This is mostly the existing batcher-stop story, but framed as a strong-consistency test with explicit assertions on "wait, do not repair".

### 3. Regression guard on invalid-message path

Suggested name:

- `TestSupernodeStrongConsistency_InvalidMessagePathStillReplacesBlock`

This should stay smaller. It is mostly a regression check around:

- `op-acceptance-tests/tests/supernode/interop/reorg/invalid_message_reorg_test.go`

The point is to ensure the new repair/wait logic did not break the old invalid-message reset path.

## Recommended Primary Keystone Flow

This is the recommended shape for `TestSupernodeStrongConsistency_L1Reorg_RepairsWaitsAndRecovers`.

### Phase A: Build a known accepted prefix

Use:

- `sys := presets.NewTwoL2SupernodeInterop(t, 0)`
- `sys.Supernode.EnsureInteropPaused(sys.L2ACL, sys.L2BCL, pauseOffset)`

Recommended flow:

1. let both chains make normal progress
2. pause interop ahead of the current accepted frontier
3. wait until timestamp `acceptedTS = pausedTS - 1` is validated
4. snapshot:
   - `SuperRootAtTimestamp(acceptedTS)` must exist
   - both chains' `SafeL2`
   - both chains' `LocalSafeL2`
   - both chains' `L2EL` safe labels

Why pause first:

- it gives a stable accepted prefix
- it makes later "did repair drop accepted state?" assertions much easier
- the new repair logic runs before the pause check, so pause does not prevent repair

### Phase B: Trigger a genuine L1 reorg

Use the existing devstack pattern from the non-supernode acceptance tests:

1. get the L1 CL:
   - `cl := sys.L1Network.Escape().L1CLNode(match.FirstL1CL)`
2. stop fake-PoS:
   - `sys.ControlPlane.FakePoSState(cl.ID(), stack.Stop)`
3. choose a divergence block before or at the accepted world
4. use the L1 `TestSequencer` control API to build an alternative L1 block from the divergence parent
5. restart fake-PoS:
   - `sys.ControlPlane.FakePoSState(cl.ID(), stack.Start)`
6. wait for:
   - `sys.L1EL.ReorgTriggered(divergence, attempts)`
   - at least one L2 EL reorg on the affected chain(s)

The safest divergence choice is:

- choose an L1 block number that is known to be in use by the accepted prefix
- for example, use the smaller of the chains' current `SafeL2.L1Origin.Number`

This is enough to invalidate the accepted interop world without needing private supernode internals.

### Phase C: Assert accepted-state repair

After the reorg lands, the first new strong-consistency invariant to check is:

- the previously accepted timestamp is no longer treated as validated if its accepted world changed

Recommended assertion:

1. poll `SuperRootAtTimestamp(acceptedTS)`
2. expect it to become unavailable (`Data == nil`) after repair

This is the clearest externally visible proof that the accepted verified prefix was rewound.

Also assert at least one chain rewinds in cross-safe / EL safe terms:

- `L2CL.RewindedFn(types.CrossSafe, ...)`
- or direct block-hash inequality on the old safe height

### Phase D: Assert frontier wait

Once accepted state has been repaired, do not immediately resume to success.

Instead, assert that the next timestamp does not get validated while the chains are still inconsistent after the reorg.

Recommended observable:

- `SuperRootAtTimestamp(nextTS)` stays unavailable for some interval

Where:

- `nextTS` is `acceptedTS + blockTime`

Also assert that cross-safe does not advance during this interval:

- `L2ACL.NotAdvancedFn(types.CrossSafe, attempts)`
- `L2BCL.NotAdvancedFn(types.CrossSafe, attempts)`

This is the external signature of the synchronous frontier gate:

- no new validation
- no false progress
- just waiting for a coherent world

### Phase E: Assert recovery

Finally, once the L2s re-derive on the new canonical L1 chain:

1. wait for local-safe to recover and move past the repaired point
2. resume interop if the test is still paused
3. assert:
   - `SuperRootAtTimestamp(acceptedTS)` becomes available again if the timestamp is re-derived and revalidated
   - or, more simply, `SuperRootAtTimestamp(nextTS)` becomes available
4. assert `CrossSafe` resumes advancing on both chains

This closes the full story:

- accepted prefix existed
- accepted prefix was invalidated
- supernode repaired it
- supernode waited during inconsistency
- supernode resumed once the world converged

## Secondary Guardrail Flow

For `TestSupernodeStrongConsistency_FrontierLag_WaitsWithoutRepair`:

1. let the system validate through a baseline timestamp
2. stop `L2BatcherB`
3. let chain A continue moving
4. assert:
   - chain B `UnsafeL2` advances
   - chain B `LocalSafeL2` stays pinned
   - `SuperRootAtTimestamp(aheadTS)` stays unavailable
   - previously validated timestamp remains available
5. restart `L2BatcherB`
6. assert `aheadTS` eventually becomes validated

This is very close to the existing chain-lag test, but the important framing change is:

- lack of progress here is a correct frontier wait
- it is not a repair event

## Why This Suite Is Better Than A Single Huge Test

The primary keystone should prove the full repair/wait/recover story.

The secondary tests should isolate:

- frontier wait without repair
- invalid-message reset path still works

That split makes failures legible:

- if the primary test fails, we know the end-to-end story is broken
- if the frontier-only guardrail fails, the synchronous gate is wrong
- if the invalid-message regression fails, we broke the old reset behavior

## Gaps To Expect During Implementation

### 1. Picking the reorg depth

We should expect to tune the divergence depth a bit.

The test needs the L1 reorg to invalidate accepted supernode state, not just the latest unsafe tail. The existing interop reorg tests in `op-acceptance-tests/tests/interop/reorgs/` are the best template for choosing this.

### 2. Public observability is limited

The supernode query API only exposes:

- `SuperRootAtTimestamp`

So the acceptance test will need to infer repair from:

- disappearance of a previously available super-root
- cross-safe rewinds / stalls
- eventual recovery

That is fine for the keystone. Detailed rewind-boundary correctness should continue to live in the seam/unit tests we already added.

### 3. Real L1 reorg timing may be slightly noisy

The devstack pattern is real enough that we should expect some timing slack:

- stop fake-PoS
- sequence alternate L1 block
- restart fake-PoS
- wait for L1 and L2 reorg observables

This is realistic, but it means the test should lean on `Eventually` and existing DSL wait helpers instead of fixed sleeps wherever possible.

## Recommended File Layout

Add a new package under:

- `op-acceptance-tests/tests/supernode/interop/consistency/`

Suggested files:

- `init_test.go`
  - `TestMain`
  - use `presets.WithTwoL2SupernodeInterop(0)`
  - probably also `presets.WithTimeTravel()` if it is stable with the reorg flow

- `reorg_repair_recovery_test.go`
  - the primary keystone

- `frontier_wait_test.go`
  - the frontier-only guardrail

Why a new package:

- it isolates the heavier stateful scenarios from the existing supernode interop package
- it avoids polluting other tests with aggressive reorgs and long waits
- it mirrors the existing `supernode/interop/reorg/` split

## Recommendation

The right first keystone is:

- `TestSupernodeStrongConsistency_L1Reorg_RepairsWaitsAndRecovers`

Use the supernode devstack preset, but borrow the L1 reorg technique from the existing non-supernode acceptance tests.

That gives us:

- the production-shaped supernode topology we care about
- a real L1 reorg, not just a synthetic local reset
- explicit visibility into repair, wait, and recovery

If the primary keystone turns out to be too noisy at first, the fallback is:

1. land the frontier-only guardrail in `op-acceptance-tests`
2. keep the accepted-state repair story in seam/unit tests temporarily
3. then iterate toward the full reorg keystone once the scenario is stable

My recommendation is to try the full keystone first. The devstack control surfaces already look sufficient.
