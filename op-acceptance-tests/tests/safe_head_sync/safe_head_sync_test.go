package safe_head_sync

import (
	"testing"

	"github.com/ethereum-optimism/optimism/op-devstack/devtest"
	"github.com/ethereum-optimism/optimism/op-devstack/dsl"
	"github.com/ethereum-optimism/optimism/op-devstack/presets"
	stypes "github.com/ethereum-optimism/optimism/op-supervisor/supervisor/types"
)

// TestSafeHeadSync tests the core safe head gossip functionality
// Verifies that a prover node can gossip its safe heads to a follower node
// and that the follower node can receive and apply those safe heads without derivation
func TestSafeHeadSync(gt *testing.T) {
	t := devtest.SerialT(gt)
	sys := presets.NewFollowerMode(t)
	logger := t.Logger()

	require := t.Require()

	logger.Info("Starting safe head sync test",
		"prover", sys.ProverCL.Escape().ID(),
		"follower", sys.FollowerCL.Escape().ID())

	// Phase 1: Both nodes advance unsafe heads (via existing P2P gossip)
	// This verifies the basic P2P connection is working
	logger.Info("Phase 1: Verifying unsafe head sync")
	dsl.CheckAll(t,
		sys.ProverCL.AdvancedFn(stypes.LocalUnsafe, 10, 30),
		sys.FollowerCL.AdvancedFn(stypes.LocalUnsafe, 10, 30),
	)
	logger.Info("Phase 1: Unsafe head sync verified")

	// Phase 2: Prover advances safe head (through normal derivation)
	// The prover should gossip these safe heads to the follower
	logger.Info("Phase 2: Prover advancing safe head")
	sys.ProverCL.Advanced(stypes.CrossSafe, 5, 30)

	proverSafeHead := sys.ProverCL.SafeL2BlockRef()
	logger.Info("Prover safe head advanced",
		"number", proverSafeHead.Number,
		"hash", proverSafeHead.Hash)

	// Phase 3: Follower safe head should match prover via safe head gossip
	// This is the core test: follower receives gossiped safe heads and applies them
	logger.Info("Phase 3: Waiting for follower safe head to match prover")

	// The follower should match the prover's safe head through gossip
	// rather than through its own derivation (which should be disabled)
	sys.FollowerCL.Matched(sys.ProverCL, stypes.CrossSafe, 30)

	followerSafeHead := sys.FollowerCL.SafeL2BlockRef()
	logger.Info("Follower safe head matched prover",
		"number", followerSafeHead.Number,
		"hash", followerSafeHead.Hash)

	require.Equal(proverSafeHead.Hash, followerSafeHead.Hash,
		"Follower safe head should match prover safe head")
	require.Equal(proverSafeHead.Number, followerSafeHead.Number,
		"Follower safe head number should match prover")

	logger.Info("Safe head sync test completed successfully")
}

// TestFollowerModeDerivationDisabled verifies that follower mode
// disables the derivation pipeline and relies purely on gossip
func TestFollowerModeDerivationDisabled(gt *testing.T) {
	t := devtest.SerialT(gt)
	sys := presets.NewFollowerMode(t)
	logger := t.Logger()

	logger.Info("Testing that follower derivation is disabled")

	// Both nodes advance unsafe heads normally
	dsl.CheckAll(t,
		sys.ProverCL.AdvancedFn(stypes.LocalUnsafe, 5, 30),
		sys.FollowerCL.AdvancedFn(stypes.LocalUnsafe, 5, 30),
	)

	// Stop the prover temporarily to prevent safe head gossip
	logger.Info("Stopping prover to test follower independence")
	sys.ProverCL.Stop()

	// Follower should NOT advance its safe head without gossip
	// This verifies derivation is truly disabled
	sys.FollowerCL.NotAdvanced(stypes.CrossSafe, 15)

	followerSafeHead := sys.FollowerCL.SafeL2BlockRef()
	logger.Info("Follower safe head remained static without gossip",
		"number", followerSafeHead.Number)

	logger.Info("Follower derivation disability test completed")
}
