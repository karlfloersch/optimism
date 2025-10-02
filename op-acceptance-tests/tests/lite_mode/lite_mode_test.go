package lite_mode

import (
	"testing"

	"github.com/ethereum-optimism/optimism/op-devstack/devtest"
	"github.com/ethereum-optimism/optimism/op-devstack/dsl"
	"github.com/ethereum-optimism/optimism/op-devstack/presets"
	"github.com/ethereum-optimism/optimism/op-supervisor/supervisor/types"
)

// TestLiteModeBasicSync verifies that a lite mode verifier can sync safe and finalized heads
// from the sequencer without running L1 derivation.
func TestLiteModeBasicSync(gt *testing.T) {
	t := devtest.SerialT(gt)
	sys := presets.NewLiteMode(t)
	require := t.Require()
	logger := t.Logger()

	logger.Info("Starting lite mode basic sync test")

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

	// The lite mode verifier should sync to match the sequencer's safe head
	// Give it some time to poll and sync
	logger.Info("Waiting for lite mode verifier to sync")
	sys.L2CLB.Matched(sys.L2CL, types.LocalSafe, 30)

	verifierSafe := sys.L2CLB.SafeL2BlockRef()
	logger.Info("Lite mode verifier synced", "safe", verifierSafe.Number, "hash", verifierSafe.Hash)

	// Verify the safe heads match
	require.Equal(newSafeSeq, verifierSafe.Number, "lite mode verifier safe head should match sequencer")
	require.Equal(sys.L2CL.SafeL2BlockRef().Hash, verifierSafe.Hash, "lite mode verifier safe hash should match sequencer")
}

// TestLiteModeFinalizedSync verifies that lite mode correctly syncs finalized heads.
func TestLiteModeFinalizedSync(gt *testing.T) {
	t := devtest.SerialT(gt)
	sys := presets.NewLiteMode(t)
	require := t.Require()
	logger := t.Logger()

	logger.Info("Starting lite mode finalized sync test")

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

	// The lite mode verifier should sync finalized head
	logger.Info("Waiting for lite mode verifier to sync finalized")
	sys.L2CLB.Matched(sys.L2CL, types.Finalized, 30)

	verifierFin := sys.L2CLB.HeadBlockRef(types.Finalized)
	logger.Info("Lite mode verifier finalized synced", "finalized", verifierFin.Number, "hash", verifierFin.Hash)

	// Verify the finalized heads match
	require.Equal(newFinSeq, verifierFin.Number, "lite mode verifier finalized head should match sequencer")
	require.Equal(sys.L2CL.HeadBlockRef(types.Finalized).Hash, verifierFin.Hash, "lite mode verifier finalized hash should match sequencer")
}

// TestLiteModeUnsafeViaP2P verifies that lite mode nodes still receive unsafe blocks via P2P gossip.
func TestLiteModeUnsafeViaP2P(gt *testing.T) {
	t := devtest.SerialT(gt)
	sys := presets.NewLiteMode(t)
	require := t.Require()
	logger := t.Logger()

	logger.Info("Starting lite mode unsafe P2P test")

	// Verify P2P connection between nodes
	sys.L2CLB.IsP2PConnected(sys.L2CL)
	logger.Info("Lite mode verifier is P2P connected to sequencer")

	// The sequencer should produce new unsafe blocks
	initialUnsafeSeq := sys.L2CL.HeadBlockRef(types.LocalUnsafe).Number
	logger.Info("Initial sequencer unsafe", "unsafe", initialUnsafeSeq)

	// Wait for sequencer to produce more unsafe blocks
	targetDelta := uint64(3)
	sys.L2CL.Advanced(types.LocalUnsafe, targetDelta, 30)

	newUnsafeSeq := sys.L2CL.HeadBlockRef(types.LocalUnsafe).Number
	logger.Info("Sequencer produced unsafe blocks", "old_unsafe", initialUnsafeSeq, "new_unsafe", newUnsafeSeq)
	require.GreaterOrEqual(newUnsafeSeq, initialUnsafeSeq+targetDelta, "sequencer should have produced unsafe blocks")

	// The lite mode verifier should receive unsafe blocks via P2P
	logger.Info("Waiting for lite mode verifier to receive unsafe blocks via P2P")
	sys.L2CLB.Matched(sys.L2CL, types.LocalUnsafe, 60)

	verifierUnsafe := sys.L2CLB.HeadBlockRef(types.LocalUnsafe)
	logger.Info("Lite mode verifier received unsafe blocks", "unsafe", verifierUnsafe.Number, "hash", verifierUnsafe.Hash)

	// Verify unsafe heads match (P2P sync is working)
	require.Equal(newUnsafeSeq, verifierUnsafe.Number, "lite mode verifier unsafe head should match sequencer via P2P")
	require.Equal(sys.L2CL.HeadBlockRef(types.LocalUnsafe).Hash, verifierUnsafe.Hash, "lite mode verifier unsafe hash should match sequencer")
}

// TestLiteModeContinuousSync verifies that lite mode continues to sync as the sequencer progresses.
func TestLiteModeContinuousSync(gt *testing.T) {
	t := devtest.SerialT(gt)
	sys := presets.NewLiteMode(t)
	require := t.Require()
	logger := t.Logger()

	logger.Info("Starting lite mode continuous sync test")

	// Perform multiple rounds of sync to verify continuous operation
	for i := 0; i < 3; i++ {
		logger.Info("Continuous sync round", "round", i+1)

		// Wait for sequencer to advance
		sys.L2CL.Advanced(types.LocalSafe, 2, 30)

		// Verify lite mode verifier keeps up
		sys.L2CLB.Matched(sys.L2CL, types.LocalSafe, 30)

		seqSafe := sys.L2CL.SafeL2BlockRef()
		verSafe := sys.L2CLB.SafeL2BlockRef()
		logger.Info("Sync round complete", "round", i+1, "seq_safe", seqSafe.Number, "ver_safe", verSafe.Number)

		require.Equal(seqSafe.Hash, verSafe.Hash, "lite mode verifier should stay synced with sequencer")
	}

	logger.Info("Continuous sync test completed successfully")
}
