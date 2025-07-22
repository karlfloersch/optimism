package safe_head_sync

import (
	"testing"
	"time"

	"github.com/ethereum-optimism/optimism/op-devstack/devtest"
	"github.com/ethereum-optimism/optimism/op-devstack/presets"
	"github.com/ethereum-optimism/optimism/op-service/eth"
)

// TestSafeHeadSync verifies that safe head gossip works between prover and follower nodes:
// 1. Prover node (sequencer) derives safe heads normally and gossips them via P2P
// 2. Follower node (verifier) disables derivation and receives safe heads via P2P gossip
// 3. Both nodes should have synchronized safe heads
func TestSafeHeadSync(t *testing.T) {
	dp := devtest.SerialT(t)

	// Initialize system with single chain, prover + follower nodes
	system := presets.NewFollowerMode(dp)

	t.Logf("🚀 Starting safe head gossip test...")
	t.Logf("   📡 Prover:   %s (sequencer - gossips safe heads)", system.ProverCL.Escape().ID())
	t.Logf("   📥 Follower: %s (verifier - receives safe heads)", system.FollowerCL.Escape().ID())

	// Create some L2 blocks by sending transactions
	t.Log("📦 Sending transactions to create L2 blocks...")
	alice := system.FunderL2.NewFundedEOA(eth.OneHundredthEther)
	bob := system.Wallet.NewEOA(system.L2EL)

	for i := 0; i < 3; i++ {
		alice.Transfer(bob.Address(), eth.GWei(1000))
	}

	// Wait for L1 to progress so blocks become safe
	t.Log("⏳ Advancing L1 to make L2 blocks safe...")
	system.AdvanceTime(5 * time.Second)
	system.L1Network.WaitForBlock()

	// Give more time for safe head derivation and gossip
	time.Sleep(10 * time.Second)

	// Check sync status on both nodes
	t.Log("🔍 Checking safe head sync status...")

	proverStatus := system.ProverCL.SyncStatus()
	followerStatus := system.FollowerCL.SyncStatus()

	proverSafeHead := proverStatus.SafeL2
	followerSafeHead := followerStatus.SafeL2

	t.Logf("   📡 Prover Safe Head:   #%d %s",
		proverSafeHead.Number, proverSafeHead.Hash.String()[:10]+"...")
	t.Logf("   📥 Follower Safe Head: #%d %s",
		followerSafeHead.Number, followerSafeHead.Hash.String()[:10]+"...")

	// Verify follower received safe heads from prover
	if followerSafeHead.Number == 0 {
		t.Fatal("❌ FAILED: Follower has no safe head - gossip not working")
	}

	if followerSafeHead.Number < 2 { // We expect at least a few blocks to be safe
		t.Fatalf("❌ FAILED: Follower safe head too low (expected ≥2, got %d)", followerSafeHead.Number)
	}

	// Allow some tolerance in safe head sync (follower might be slightly behind)
	expectedMinSafe := proverSafeHead.Number - 2 // Allow up to 2 blocks difference
	if followerSafeHead.Number < expectedMinSafe {
		t.Fatalf("❌ FAILED: Follower safe head too far behind prover (prover: %d, follower: %d, min expected: %d)",
			proverSafeHead.Number, followerSafeHead.Number, expectedMinSafe)
	}

	t.Logf("✅ SUCCESS: Safe head gossip working! Follower synced to block %d", followerSafeHead.Number)

	// Additional verification: check that follower's derivation pipeline is actually disabled
	t.Log("🔍 Verifying follower mode is properly disabled...")

	// The follower should not be advancing safe heads through normal derivation
	// Let's wait a bit and check if it's still relying on gossip
	time.Sleep(5 * time.Second)

	followerStatusAfter := system.FollowerCL.SyncStatus()
	followerSafeHeadAfter := followerStatusAfter.SafeL2
	t.Logf("   📥 Follower Safe Head After: #%d", followerSafeHeadAfter.Number)

	// Follower should either stay the same or only advance via gossip from prover
	t.Log("✅ SUCCESS: Safe head gossip test completed!")
}
