package reorg

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/ethereum-optimism/optimism/op-devstack/devtest"
	"github.com/ethereum-optimism/optimism/op-devstack/presets"
	"github.com/ethereum-optimism/optimism/op-devstack/stack"
	"github.com/ethereum-optimism/optimism/op-devstack/stack/match"
	"github.com/ethereum/go-ethereum/common/hexutil"
)

// TestSupernodeStrongConsistency_L1Reorg_RepairsAndRecovers proves the new
// strong-consistency flow against the shared-supernode devstack:
// 1. a timestamp is accepted and exposed through SuperRootAtTimestamp
// 2. an L1 reorg invalidates that accepted world
// 3. the accepted timestamp temporarily disappears while interop repairs
// 4. the timestamp is revalidated on the new canonical L1
// 5. once interop is resumed, the next timestamp validates normally
func TestSupernodeStrongConsistency_L1Reorg_RepairsAndRecovers(gt *testing.T) {
	t := devtest.SerialT(gt)
	sys := presets.NewTwoL2SupernodeInterop(t, 0)

	// Let L1/L2 progress beyond the very early genesis-era blocks before we
	// freeze interop. Reorging the accepted world from a later L1 dependency is
	// both more realistic and works better with the fake-PoS test sequencer.
	sys.L1EL.WaitForBlockNumber(12)

	pausedTS := sys.Supernode.EnsureInteropPaused(sys.L2ACL, sys.L2BCL, 10)
	require.Greater(t, pausedTS, uint64(0), "paused timestamp must be past genesis")

	acceptedTS := pausedTS - 1
	acceptedResp := sys.Supernode.SuperRootAtTimestamp(acceptedTS)
	require.NotNil(t, acceptedResp.Data, "accepted timestamp should be validated before the reorg")
	require.NotEmpty(t, acceptedResp.OptimisticAtTimestamp, "accepted timestamp should expose per-chain optimistic outputs")

	reorgTarget := acceptedResp.Data.VerifiedRequiredL1
	require.Greater(t, reorgTarget.Number, uint64(0), "accepted timestamp should depend on a non-genesis required L1 block")
	divergence := sys.L1EL.BlockRefByNumber(reorgTarget.Number)

	t.Logger().Info("captured accepted world",
		"accepted_ts", acceptedTS,
		"frontier_ts", pausedTS,
		"verified_required_l1", acceptedResp.Data.VerifiedRequiredL1,
		"reorg_target", reorgTarget,
	)

	l1CL := sys.L1Network.Escape().L1CLNode(match.FirstL1CL)
	sys.ControlPlane.FakePoSState(l1CL.ID(), stack.Stop)
	t.Cleanup(func() {
		sys.ControlPlane.FakePoSState(l1CL.ID(), stack.Start)
	})

	// Rewind the L1 EL to the parent of the accepted dependency, then let
	// FakePoS rebuild forward on the new canonical branch.
	require.NoError(t, sys.L1EL.EthClient().RPC().CallContext(
		t.Ctx(),
		nil,
		"debug_setHead",
		hexutil.Uint64(divergence.Number-1),
	))
	sys.ControlPlane.FakePoSState(l1CL.ID(), stack.Start)
	sys.L1EL.ReorgTriggered(divergence, 10)

	// The accepted timestamp should become unavailable once the supernode repairs
	// its accepted prefix against the new canonical L1 chain.
	require.Eventually(t, func() bool {
		resp := sys.Supernode.SuperRootAtTimestamp(acceptedTS)
		return resp.Data == nil
	}, 2*time.Minute, time.Second, "accepted timestamp should disappear after the L1 reorg")

	// Because interop is still paused at pausedTS, it should be able to re-verify
	// the repaired prefix but stop before validating the next timestamp.
	require.Eventually(t, func() bool {
		resp := sys.Supernode.SuperRootAtTimestamp(acceptedTS)
		return resp.Data != nil
	}, 2*time.Minute, time.Second, "accepted timestamp should be revalidated on the new canonical L1")

	frontierResp := sys.Supernode.SuperRootAtTimestamp(pausedTS)
	require.Nil(t, frontierResp.Data, "paused frontier timestamp should remain unavailable before resume")

	sys.Supernode.ResumeInterop()

	require.Eventually(t, func() bool {
		resp := sys.Supernode.SuperRootAtTimestamp(pausedTS)
		return resp.Data != nil
	}, 2*time.Minute, time.Second, "frontier timestamp should validate once interop resumes")
}
