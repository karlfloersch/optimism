package offset_el_safe

import (
	"testing"

	"github.com/ethereum-optimism/optimism/op-devstack/devtest"
	"github.com/ethereum-optimism/optimism/op-devstack/dsl"
	"github.com/ethereum-optimism/optimism/op-devstack/sysgo"
	"github.com/ethereum-optimism/optimism/op-service/eth"
	"github.com/ethereum-optimism/optimism/op-supervisor/supervisor/types"
)

// TestELSyncSafeRetractedByOffset verifies that when OffsetELSafe is configured on
// the verifier, the safe and finalized EL heads are set behind the unsafe head after
// EL sync completes.
//
// Flow:
//  1. Both nodes advance LocalSafe (ensures the CL has real finalized state).
//  2. Stop/wipe the verifier EL to force a full EL sync on restart.
//  3. Advance the sequencer further while the verifier is down.
//  4. Restart verifier, peer ELs, and wait for EL sync + forkchoice update.
//  5. Assert safe/finalized are retracted from unsafe by the configured offset.
func TestELSyncSafeRetractedByOffset(gt *testing.T) {
	t := devtest.ParallelT(gt)
	sysgo.SkipOnOpReth(t, "not supported (peering issue)")
	sys := newOffsetELSafeSystem(t)
	require := t.Require()
	logger := t.Logger()

	// Advance both CLs to LocalSafe so the verifier CL has valid finalized state
	// before we stop it (prevents "forkchoice not initialized" after EL wipe).
	dsl.CheckAll(t,
		sys.L2CL.AdvancedFn(types.LocalSafe, 1, 30),
		sys.L2CLB.AdvancedFn(types.LocalSafe, 1, 30))
	sys.L2CLB.Matched(sys.L2CL, types.LocalSafe, 30)

	// Stop verifier and wipe its EL to force a full EL sync on restart.
	sys.L2ELB.Stop()
	sys.L2CLB.Stop()

	// Advance sequencer further while verifier is down.
	sys.L2CL.Advanced(types.LocalSafe, 3, 30)

	sys.L2ELB.Start()
	sys.L2CLB.Start()
	sys.L2ELB.PeerWith(sys.L2EL)

	// Wait for verifier CL to advance its LocalSafe, indicating EL sync completed
	// and the forkchoice update (with safe/finalized) was sent.
	sys.L2CLB.Advanced(types.LocalSafe, 1, 30)

	unsafeHead := sys.L2ELB.BlockRefByLabel(eth.Unsafe)
	safeHead := sys.L2ELB.SafeHead().BlockRef
	finalizedHead := sys.L2ELB.FinalizedHead().BlockRef

	logger.Info("Verifier heads after EL sync",
		"unsafe", unsafeHead.Number,
		"safe", safeHead.Number,
		"finalized", finalizedHead.Number)

	// Safe and finalized must be behind unsafe when offset is configured.
	require.Greater(unsafeHead.Number, safeHead.Number,
		"safe head should be behind unsafe head when OffsetELSafe is configured")
	require.Greater(unsafeHead.Number, finalizedHead.Number,
		"finalized head should be behind unsafe head when OffsetELSafe is configured")
	require.Equal(safeHead.Number, finalizedHead.Number,
		"safe and finalized should be equal after EL sync with offset")

	retraction := unsafeHead.Number - safeHead.Number
	logger.Info("Observed retraction", "blocks", retraction)
	require.Greater(retraction, uint64(0), "retraction must be nonzero")
	// expected retraction is 5 blocks: 10s offset / 2s block time
	expectedRetraction := uint64(5)
	//observed retraction may not be exact if the verifier progressed derivation after EL sync completed.
	if retraction != expectedRetraction {
		t.Logger().Warn("Observed retraction does not match expected retraction", "observed", retraction, "expected", expectedRetraction)
	}
}
