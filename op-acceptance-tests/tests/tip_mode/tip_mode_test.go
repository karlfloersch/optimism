package tip_mode

import (
	"testing"

	"github.com/ethereum-optimism/optimism/op-devstack/devtest"
	"github.com/ethereum-optimism/optimism/op-devstack/dsl"
	"github.com/ethereum-optimism/optimism/op-devstack/presets"
	"github.com/ethereum-optimism/optimism/op-supervisor/supervisor/types"
)

// TestTipModeBasicSync verifies that a tip mode verifier can sync safe and finalized heads
// from the sequencer without running L1 derivation.
func TestTipModeBasicSync(gt *testing.T) {
	t := devtest.SerialT(gt)
	sys := presets.NewTipMode(t)
	require := t.Require()
	logger := t.Logger()

	logger.Info("Starting tip mode basic sync test")

	// The sequencer should be producing blocks
	initialSafeSeq := sys.L2CL.SafeL2BlockRef().Number
	logger.Info("Initial sequencer state", "safe", initialSafeSeq)

	// Wait for sequencer to advance safe head by at least 5 blocks
	targetDelta := uint64(5)
	dsl.CheckAll(t,
		sys.L2CL.AdvancedFn(types.LocalSafe, targetDelta, 30),
	)

	newSafeSeq := sys.L2CL.SafeL2BlockRef().Number
	logger.Info("Sequencer advanced", "old_safe", initialSafeSeq, "new_safe", newSafeSeq)
	require.GreaterOrEqual(newSafeSeq, initialSafeSeq+targetDelta, "sequencer should have advanced safe head")

	// The tip mode verifier should sync to match the sequencer's safe head
	// Give it some time to poll and sync
	logger.Info("Waiting for tip mode verifier to sync")
	sys.L2CLB.Matched(sys.L2CL, types.LocalSafe, 30)

	verifierSafe := sys.L2CLB.SafeL2BlockRef()
	logger.Info("Tip mode verifier synced", "safe", verifierSafe.Number, "hash", verifierSafe.Hash)

	// Verify the safe heads match
	require.Equal(newSafeSeq, verifierSafe.Number, "tip mode verifier safe head should match sequencer")
	require.Equal(sys.L2CL.SafeL2BlockRef().Hash, verifierSafe.Hash, "tip mode verifier safe hash should match sequencer")
}

// TestTipModeFinalizedSync verifies that tip mode correctly syncs finalized heads.
func TestTipModeFinalizedSync(gt *testing.T) {
	t := devtest.SerialT(gt)
	sys := presets.NewTipMode(t)
	require := t.Require()
	logger := t.Logger()

	logger.Info("Starting tip mode finalized sync test")

	// Wait for L1 to produce enough blocks for finalization
	// L1 needs head > finalizedDistance (20) for any blocks to be finalized
	logger.Info("Waiting for L1 to produce sufficient blocks for finalization...")
	for i := 0; i < 30; i++ {
		l1Head := sys.L1Network.WaitForBlock()
		logger.Info("L1 block produced", "number", l1Head.Number)
		if l1Head.Number >= 23 {
			logger.Info("L1 has sufficient blocks for finalization")
			break
		}
	}

	// Wait for both nodes to advance finalized heads
	initialFinSeq := sys.L2CL.HeadBlockRef(types.Finalized).Number
	logger.Info("Initial sequencer finalized", "finalized", initialFinSeq)

	// Wait for sequencer to advance finalized by at least 3 blocks
	targetDelta := uint64(3)
	sys.L2CL.Advanced(types.Finalized, targetDelta, 50)

	newFinSeq := sys.L2CL.HeadBlockRef(types.Finalized).Number
	logger.Info("Sequencer finalized advanced", "old_fin", initialFinSeq, "new_fin", newFinSeq)
	require.GreaterOrEqual(newFinSeq, initialFinSeq+targetDelta, "sequencer should have advanced finalized head")

	// The tip mode verifier should sync finalized head
	logger.Info("Waiting for tip mode verifier to sync finalized")
	sys.L2CLB.Matched(sys.L2CL, types.Finalized, 30)

	verifierFin := sys.L2CLB.HeadBlockRef(types.Finalized)
	logger.Info("Tip mode verifier finalized synced", "finalized", verifierFin.Number, "hash", verifierFin.Hash)

	// Verify the finalized heads match
	require.Equal(newFinSeq, verifierFin.Number, "tip mode verifier finalized head should match sequencer")
	require.Equal(sys.L2CL.HeadBlockRef(types.Finalized).Hash, verifierFin.Hash, "tip mode verifier finalized hash should match sequencer")
}

// TestTipModeUnsafeViaP2P verifies that tip mode nodes still receive unsafe blocks via P2P gossip.
func TestTipModeUnsafeViaP2P(gt *testing.T) {
	t := devtest.SerialT(gt)
	sys := presets.NewTipMode(t)
	require := t.Require()
	logger := t.Logger()

	logger.Info("Starting tip mode unsafe P2P test")

	// Verify P2P connection between nodes
	sys.L2CLB.IsP2PConnected(sys.L2CL)
	logger.Info("Tip mode verifier is P2P connected to sequencer")

	// First, wait for the sequencer to produce some blocks and for safe heads to sync
	initialSafeSeq := sys.L2CL.SafeL2BlockRef().Number
	logger.Info("Initial sequencer safe head", "safe", initialSafeSeq)

	// Wait for sequencer to advance safe head by at least 5 blocks
	targetDelta := uint64(5)
	dsl.CheckAll(t,
		sys.L2CL.AdvancedFn(types.LocalSafe, targetDelta, 30),
	)

	newSafeSeq := sys.L2CL.SafeL2BlockRef().Number
	logger.Info("Sequencer advanced safe head", "old_safe", initialSafeSeq, "new_safe", newSafeSeq)

	// Wait for verifier to sync safe head via tip mode RPC
	logger.Info("Waiting for verifier to sync safe head")
	sys.L2CLB.Matched(sys.L2CL, types.LocalSafe, 60)
	logger.Info("Verifier safe head synced")

	// Now verify that unsafe blocks also sync via P2P (alongside safe head progression)
	logger.Info("Waiting for verifier unsafe head to sync via P2P")
	sys.L2CLB.Matched(sys.L2CL, types.LocalUnsafe, 60)

	verifierUnsafe := sys.L2CLB.HeadBlockRef(types.LocalUnsafe)
	seqUnsafe := sys.L2CL.HeadBlockRef(types.LocalUnsafe)
	logger.Info("P2P sync complete", "sequencer", seqUnsafe.Number, "verifier", verifierUnsafe.Number)

	// Verify unsafe heads match (P2P sync is working)
	require.Equal(seqUnsafe.Number, verifierUnsafe.Number, "tip mode verifier unsafe head should match sequencer via P2P")
	require.Equal(seqUnsafe.Hash, verifierUnsafe.Hash, "tip mode verifier unsafe hash should match sequencer")
}

// TestTipModeContinuousSync verifies that tip mode continues to sync as the sequencer progresses.
func TestTipModeContinuousSync(gt *testing.T) {
	t := devtest.SerialT(gt)
	sys := presets.NewTipMode(t)
	require := t.Require()
	logger := t.Logger()

	logger.Info("Starting tip mode continuous sync test")

	// Perform multiple rounds of sync to verify continuous operation
	for i := 0; i < 3; i++ {
		logger.Info("Continuous sync round", "round", i+1)

		// Wait for sequencer to advance
		sys.L2CL.Advanced(types.LocalSafe, 2, 30)

		// Verify tip mode verifier keeps up
		sys.L2CLB.Matched(sys.L2CL, types.LocalSafe, 30)

		seqSafe := sys.L2CL.SafeL2BlockRef()
		verSafe := sys.L2CLB.SafeL2BlockRef()
		logger.Info("Sync round complete", "round", i+1, "seq_safe", seqSafe.Number, "ver_safe", verSafe.Number)

		require.Equal(seqSafe.Hash, verSafe.Hash, "tip mode verifier should stay synced with sequencer")
	}

	logger.Info("Continuous sync test completed successfully")
}
