package strongconsistency

import (
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/ethereum-optimism/optimism/op-devstack/devtest"
	"github.com/ethereum-optimism/optimism/op-devstack/presets"
	"github.com/ethereum-optimism/optimism/op-devstack/stack"
	"github.com/ethereum-optimism/optimism/op-devstack/stack/match"
	"github.com/ethereum-optimism/optimism/op-service/eth"
	"github.com/ethereum-optimism/optimism/op-service/txplan"
	"github.com/ethereum/go-ethereum/common/hexutil"
)

// TestSupernodeStrongConsistency_L1Reorg_RepairsAndRecovers proves the new
// strong-consistency flow against the shared-supernode devstack:
// 1. a timestamp is accepted and exposed through SuperRootAtTimestamp
// 2. an L1 reorg invalidates that accepted world
// 3. the accepted timestamp temporarily disappears while interop repairs
// 4. the timestamp is revalidated against a different accepted L1 world
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
	preReorgRequiredL1 := acceptedResp.Data.VerifiedRequiredL1

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

	repairedResp := sys.Supernode.SuperRootAtTimestamp(acceptedTS)
	require.NotNil(t, repairedResp.Data, "accepted timestamp should be available after repair")
	require.NotEqual(t, preReorgRequiredL1, repairedResp.Data.VerifiedRequiredL1, "repair should revalidate the timestamp against a different accepted L1 world")

	frontierResp := sys.Supernode.SuperRootAtTimestamp(pausedTS)
	require.Nil(t, frontierResp.Data, "paused frontier timestamp should remain unavailable before resume")

	sys.Supernode.ResumeInterop()

	require.Eventually(t, func() bool {
		resp := sys.Supernode.SuperRootAtTimestamp(pausedTS)
		return resp.Data != nil
	}, 2*time.Minute, time.Second, "frontier timestamp should validate once interop resumes")
}

// TestSupernodeStrongConsistency_ExploresStaleFrontierDenyReplacement is an
// exploratory scaffold for the remaining stale-frontier-deny question.
//
// Target scenario, in English:
//
//  1. Pause interop so we have a stable accepted prefix at timestamp T and an
//     unverified frontier at T+1.
//  2. On chain A, create the initiating message that chain B depends on.
//  3. On chain B, create an executing message that is invalid under the current
//     cross-chain world F1.
//  4. Resume interop and let B process the invalid execution:
//     - interop deny-lists B's block at T+1
//     - B rewinds
//     - B builds a replacement block R at the same height
//  5. Pause interop again before T+1 is revalidated.
//  6. Reorg or otherwise perturb the *dependent* chain A so that the initiating
//     side of the dependency changes from F1 to F2, while the accepted prefix
//     at T remains valid.
//  7. Under F2, B's executing message should now be valid.
//  8. The strong-consistency property we want is:
//     - the stale deny decision from F1 is removed
//     - any replacement block R on B that was built because of that stale deny
//     is eventually unwound
//     - B can rebuild T+1 from the new frontier world F2
//
// The current version of this test is still exploratory: it gives us the
// observability needed to understand the frontier world, but it does not yet
// fully isolate the cross-chain F1 -> F2 dependency transition described above.
func TestSupernodeStrongConsistency_ExploresStaleFrontierDenyReplacement(gt *testing.T) {
	t := devtest.SerialT(gt)
	sys := presets.NewTwoL2SupernodeInterop(t, 0)

	ctx := t.Ctx()

	pausedTS := sys.Supernode.EnsureInteropPaused(sys.L2ACL, sys.L2BCL, 10)
	require.Greater(t, pausedTS, uint64(0))
	acceptedTS := pausedTS - 1
	acceptedResp := sys.Supernode.SuperRootAtTimestamp(acceptedTS)
	require.NotNil(t, acceptedResp.Data, "accepted prefix must exist before the scenario starts")

	alice := sys.FunderA.NewFundedEOA(eth.OneEther)
	bob := sys.FunderB.NewFundedEOA(eth.OneEther)
	eventLoggerA := alice.DeployEventLogger()

	sys.L2B.CatchUpTo(sys.L2A)
	sys.L2A.CatchUpTo(sys.L2B)

	rng := rand.New(rand.NewSource(12345))
	initMsg := alice.SendRandomInitMessage(rng, eventLoggerA, 2, 10)
	sys.L2B.WaitForBlock()
	execMsg := bob.SendInvalidExecMessage(initMsg)

	invalidBlockNumber := execMsg.Receipt.BlockNumber.Uint64()
	invalidBlockHash := execMsg.BlockHash()
	targetTS := sys.L2B.TimestampForBlockNum(invalidBlockNumber)
	require.GreaterOrEqual(t, targetTS, pausedTS, "the denied frontier timestamp should still be beyond the accepted prefix")

	require.Eventually(t, func() bool {
		return sys.L2BCL.SyncStatus().LocalSafeL2.Number >= invalidBlockNumber
	}, 60*time.Second, time.Second, "invalid block should become locally safe")

	sys.Supernode.ResumeInterop()

	var replacement eth.BlockRef
	require.Eventually(t, func() bool {
		current, err := sys.L2ELB.Escape().EthClient().BlockRefByNumber(ctx, invalidBlockNumber)
		if err != nil {
			return false
		}
		if current.Hash == invalidBlockHash {
			return false
		}
		replacement = current
		return true
	}, 60*time.Second, time.Second, "replacement block should appear after invalidation")

	// Re-pause the same timestamp so we can perturb its frontier world before it
	// gets revalidated.
	sys.Supernode.PauseInterop(targetTS)
	require.Eventually(t, func() bool {
		resp := sys.Supernode.SuperRootAtTimestamp(targetTS)
		return resp.Data == nil
	}, 30*time.Second, time.Second, "target timestamp should remain unvalidated while paused")

	var frontierBefore *stack.InteropDebugSnapshot
	require.Eventually(t, func() bool {
		state := sys.Supernode.InteropDebugState()
		if state.Frontier == nil || state.Frontier.Timestamp != targetTS {
			return false
		}
		frontierBefore = state.Frontier
		_, ok := frontierBefore.L2Heads[sys.L2B.ChainID()]
		return ok
	}, 30*time.Second, time.Second, "frontier timestamp should expose debug snapshot outputs")

	frontierB, ok := frontierBefore.L1Heads[sys.L2B.ChainID()]
	require.True(t, ok, "chain B frontier L1 head must be present")
	frontierBL2, ok := frontierBefore.L2Heads[sys.L2B.ChainID()]
	require.True(t, ok, "chain B frontier L2 head must be present")
	t.Logf("frontier before reorg: ts=%d replacementHeight=%d replacementHash=%s frontierBlock=%s requiredL1=%s acceptedRequiredL1=%s",
		targetTS,
		invalidBlockNumber,
		replacement.Hash,
		frontierBL2.Hash,
		frontierB,
		acceptedResp.Data.VerifiedRequiredL1,
	)

	// We want to perturb the frontier world without invalidating the accepted
	// prefix, so choose a divergence point at the frontier dependency and require
	// it to be strictly newer than the accepted dependency.
	require.Greater(t, frontierB.Number, acceptedResp.Data.VerifiedRequiredL1.Number, "frontier dependency must be ahead of the accepted dependency for this scenario")
	divergence := sys.L1EL.BlockRefByNumber(frontierB.Number)

	l1CL := sys.L1Network.Escape().L1CLNode(match.FirstL1CL)
	sys.ControlPlane.FakePoSState(l1CL.ID(), stack.Stop)
	t.Cleanup(func() {
		sys.ControlPlane.FakePoSState(l1CL.ID(), stack.Start)
	})

	require.NoError(t, sys.L1EL.EthClient().RPC().CallContext(
		ctx,
		nil,
		"debug_setHead",
		hexutil.Uint64(divergence.Number-1),
	))
	sys.ControlPlane.FakePoSState(l1CL.ID(), stack.Start)
	sys.L1EL.ReorgTriggered(divergence, 10)

	var frontierAfter *stack.InteropDebugSnapshot
	require.Eventually(t, func() bool {
		state := sys.Supernode.InteropDebugState()
		if state.Frontier == nil || state.Frontier.Timestamp != targetTS {
			return false
		}
		frontierAfter = state.Frontier
		newL1, ok := frontierAfter.L1Heads[sys.L2B.ChainID()]
		if !ok {
			return false
		}
		newL2, ok := frontierAfter.L2Heads[sys.L2B.ChainID()]
		if !ok {
			return false
		}
		return newL1 != frontierB || newL2 != frontierBL2
	}, 2*time.Minute, time.Second, "frontier world for the same timestamp should change after the L1 reorg")

	frontierAfterL1 := frontierAfter.L1Heads[sys.L2B.ChainID()]
	frontierAfterL2 := frontierAfter.L2Heads[sys.L2B.ChainID()]

	t.Logf("frontier after reorg: ts=%d replacementHeight=%d replacementHash=%s frontierBlock=%s requiredL1=%s",
		targetTS,
		invalidBlockNumber,
		replacement.Hash,
		frontierAfterL2.Hash,
		frontierAfterL1,
	)

	// This is the behavior we ultimately want to tighten: once the frontier
	// world has changed, the chain should not remain on the replacement block
	// created under the stale frontier deny decision.
	require.Eventually(t, func() bool {
		current, err := sys.L2ELB.Escape().EthClient().BlockRefByNumber(ctx, invalidBlockNumber)
		if err != nil {
			return false
		}
		return current.Hash != replacement.Hash
	}, 2*time.Minute, time.Second, "replacement block should be rewound out once its frontier deny world becomes stale")
}

// TestSupernodeStrongConsistency_SameTimestampMissingInitReplacesExec proves
// the F1 half of the cross-chain stale-deny story with deterministic same-
// timestamp blocks:
//   - chain A builds T+1 without the init
//   - chain B builds T+1 with an exec that depends on that missing init
//   - interop denies B and B installs a replacement block at the same height
//
// This test is intentionally smaller than the full stale-deny scenario. Its job
// is to prove that the deterministic same-timestamp setup can actually create
// the "replacement under F1" state that the larger test needs.
func TestSupernodeStrongConsistency_SameTimestampMissingInitReplacesExec(gt *testing.T) {
	t := devtest.SerialT(gt)
	sys := presets.NewTwoL2SupernodeInterop(t, 0).ForSameTimestampTesting(t)
	rng := rand.New(rand.NewSource(11111))

	pairA := sys.PrepareInitA(rng, 0)
	sys.IncludeAndValidate(
		nil,
		[]*txplan.PlannedTx{pairA.SubmitExecTo(sys.Bob)},
		false, true, // only B should be replaced
	)
}

// TestSupernodeStrongConsistency_SameTimestampMissingInitPauseAfterReset proves
// the test-control window we need for the stale frontier deny case:
//   - chain B is replaced under F1
//   - interop pauses before retrying the same target timestamp
//   - the target timestamp stays unvalidated while the replacement is installed
func TestSupernodeStrongConsistency_SameTimestampMissingInitPauseAfterReset(gt *testing.T) {
	t := devtest.SerialT(gt)
	sys := presets.NewTwoL2SupernodeInterop(t, 0).ForSameTimestampTesting(t)
	rng := rand.New(rand.NewSource(22222))
	ctx := t.Ctx()

	pairA := sys.PrepareInitA(rng, 0)
	_, blockB := sys.IncludeAtNextTimestamp(
		nil,
		[]*txplan.PlannedTx{pairA.SubmitExecTo(sys.Bob)},
	)

	blockTS := blockB.Time
	retryTS := blockTS - 1
	execBlockNumber := blockB.Number
	execBlockHash := blockB.Hash

	t.Logf("constructed F1 candidate: blockTS=%d retryTS=%d execBlockNumber=%d execBlockHash=%s", blockTS, retryTS, execBlockNumber, execBlockHash)

	sys.Supernode.PauseInteropAfterNextReset(retryTS)
	sys.Supernode.ResumeInterop()
	t.Logf("armed pause-after-reset for retryTS=%d and resumed interop", retryTS)

	var replacement eth.BlockRef
	require.Eventually(t, func() bool {
		current, err := sys.L2ELB.Escape().EthClient().BlockRefByNumber(ctx, execBlockNumber)
		if err != nil {
			t.Logf("replacement wait: failed to read block %d: %v", execBlockNumber, err)
			return false
		}
		resp := sys.Supernode.SuperRootAtTimestamp(retryTS)
		state := sys.Supernode.InteropDebugState()
		t.Logf("replacement wait: currentHash=%s originalHash=%s validated=%t debugNextTS=%d frontierNil=%t",
			current.Hash, execBlockHash, resp.Data != nil, state.NextTS, state.Frontier == nil)
		if current.Hash == execBlockHash {
			return false
		}
		replacement = current
		t.Logf("replacement observed: height=%d replacementHash=%s", execBlockNumber, replacement.Hash)
		return true
	}, 60*time.Second, time.Second, "B should install a replacement block under F1 before retrying the target timestamp")

	require.Never(t, func() bool {
		resp := sys.Supernode.SuperRootAtTimestamp(retryTS)
		state := sys.Supernode.InteropDebugState()
		t.Logf("pause check: validated=%t debugNextTS=%d frontierNil=%t", resp.Data != nil, state.NextTS, state.Frontier == nil)
		return resp.Data != nil
	}, 10*time.Second, time.Second, "target timestamp should remain unvalidated while interop is paused after reset")

	state := sys.Supernode.InteropDebugState()
	require.NotNil(t, state.Frontier, "debug state should expose the retried frontier snapshot")
	require.Equal(t, retryTS, state.NextTS, "interop should still be retrying the same target timestamp")
	require.Equal(t, retryTS, state.Frontier.Timestamp, "frontier snapshot should still be for the unvalidated target timestamp")
	t.Logf("post-reset paused state: nextTS=%d frontierL2Heads=%v frontierL1Heads=%v", state.NextTS, state.Frontier.L2Heads, state.Frontier.L1Heads)

	current, err := sys.L2ELB.Escape().EthClient().BlockRefByNumber(ctx, execBlockNumber)
	require.NoError(t, err)
	require.Equal(t, replacement.Hash, current.Hash, "replacement block should still be installed while the retry timestamp is paused")
	t.Logf("confirmed replacement remains installed while paused: blockNumber=%d hash=%s", execBlockNumber, current.Hash)
}

// TestSupernodeStrongConsistency_CrossChainStaleDenyScaffold is the concrete
// acceptance plan for the remaining stale-frontier-deny question.
//
// The dangerous case is cross-chain:
//   - chain A owns the initiating message
//   - chain B owns the executing message
//   - B is denied under frontier world F1 because the A-side dependency is not
//     present in the world that interop is currently evaluating
//   - B builds replacement block R at the same height
//   - later A changes to frontier world F2, while the accepted prefix at T
//     remains valid
//   - under F2, B's exec should now be valid
//
// The cleanest version of the real test should combine two ingredients:
//  1. Use the deterministic same-timestamp primitive to create F1:
//     - chain A builds T+1 without the init
//     - chain B builds T+1 with the exec
//     - interop denies B and B installs replacement block R
//  2. Use the normal batcher/L1/op-node path to move chain A from F1 to F2 for
//     that same timestamp, so B's exec should become valid without private EL
//     surgery.
//
// The debug hook is still useful here: it lets us prove that the accepted
// prefix stays fixed while the frontier snapshot for T+1 changes from F1 to F2,
// which is the exact stale-deny condition we care about.
func TestSupernodeStrongConsistency_CrossChainStaleDenyScaffold(gt *testing.T) {
	t := devtest.SerialT(gt)
	t.Skip("WIP: replacing the batcher-lag setup with a deterministic same-timestamp F1 -> F2 scenario")

	sys := presets.NewTwoL2SupernodeInterop(t, 0)
	rng := rand.New(rand.NewSource(424242))
	ctx := t.Ctx()

	alice := sys.FunderA.NewFundedEOA(eth.OneEther)
	bob := sys.FunderB.NewFundedEOA(eth.OneEther)
	eventLoggerA := alice.DeployEventLogger()

	sys.L2B.CatchUpTo(sys.L2A)
	sys.L2A.CatchUpTo(sys.L2B)

	pausedTS := sys.Supernode.EnsureInteropPaused(sys.L2ACL, sys.L2BCL, 10)
	acceptedTS := pausedTS - 1
	acceptedResp := sys.Supernode.SuperRootAtTimestamp(acceptedTS)
	require.NotNil(t, acceptedResp.Data, "accepted prefix must exist before the stale-deny scenario starts")
	t.Logf("accepted prefix fixed: acceptedTS=%d pausedTS=%d", acceptedTS, pausedTS)

	// Freeze chain A's local-safe progression while still allowing unsafe blocks
	// to be produced on A. This should give us frontier world F1:
	// - A's init exists on A
	// - but A's local-safe view has not caught up yet
	// - so B's exec should be invalid under the frontier that interop sees.
	sys.L2BatcherA.Stop()

	initMsg := alice.SendRandomInitMessage(rng, eventLoggerA, 2, 10)
	execMsg := bob.SendExecMessage(initMsg)

	initBlockNumber := initMsg.BlockNumber().Uint64()
	execBlockNumber := execMsg.BlockNumber().Uint64()
	execBlockHash := execMsg.BlockHash()
	targetTS := sys.L2B.TimestampForBlockNum(execBlockNumber)
	t.Logf("submitted init/exec: initBlock=%d execBlock=%d targetTS=%d execHash=%s", initBlockNumber, execBlockNumber, targetTS, execBlockHash)

	require.GreaterOrEqual(t, targetTS, pausedTS, "exec timestamp should still be beyond the accepted prefix")

	require.Eventually(t, func() bool {
		statusA := sys.L2ACL.SyncStatus()
		statusB := sys.L2BCL.SyncStatus()
		return statusA.LocalSafeL2.Number < initBlockNumber &&
			statusB.LocalSafeL2.Number >= execBlockNumber
	}, 60*time.Second, time.Second, "A should lag init in local-safe while B includes the exec in local-safe")
	t.Log("established frontier world F1: A local-safe lags init while B local-safe includes exec")

	// Resume interop under F1 and wait for B to replace the exec block.
	sys.Supernode.ResumeInterop()

	var replacement eth.BlockRef
	require.Eventually(t, func() bool {
		current, err := sys.L2ELB.Escape().EthClient().BlockRefByNumber(ctx, execBlockNumber)
		if err != nil {
			return false
		}
		if current.Hash == execBlockHash {
			return false
		}
		replacement = current
		return true
	}, 60*time.Second, time.Second, "B should install a replacement block under frontier world F1")
	t.Logf("replacement installed under F1: height=%d replacement=%s", execBlockNumber, replacement.Hash)

	// Re-pause before T+1 is revalidated so we can let chain A catch up and
	// observe the frontier transition from F1 to F2.
	sys.Supernode.PauseInterop(targetTS)
	require.Eventually(t, func() bool {
		resp := sys.Supernode.SuperRootAtTimestamp(targetTS)
		return resp.Data == nil
	}, 30*time.Second, time.Second, "target timestamp should remain unvalidated while re-paused")
	t.Logf("re-paused interop at targetTS=%d before revalidation", targetTS)

	var frontierBefore *stack.InteropDebugSnapshot
	require.Eventually(t, func() bool {
		state := sys.Supernode.InteropDebugState()
		if state.Frontier == nil || state.Frontier.Timestamp != targetTS {
			return false
		}
		frontierBefore = state.Frontier
		_, ok := frontierBefore.L1Heads[sys.L2A.ChainID()]
		return ok
	}, 30*time.Second, time.Second, "debug state should expose the F1 frontier for targetTS")
	t.Logf("captured F1 frontier for targetTS=%d on chainA: l1=%s l2=%s", targetTS, frontierBefore.L1Heads[sys.L2A.ChainID()], frontierBefore.L2Heads[sys.L2A.ChainID()])

	sys.L2BatcherA.Start()
	t.Log("resumed batcher A to move from F1 to F2")

	require.Eventually(t, func() bool {
		return sys.L2ACL.SyncStatus().LocalSafeL2.Number >= initBlockNumber
	}, 2*time.Minute, time.Second, "chain A local-safe should catch up once batcher A resumes")
	t.Logf("chain A local-safe caught up to initBlock=%d", initBlockNumber)

	var frontierAfter *stack.InteropDebugSnapshot
	require.Eventually(t, func() bool {
		state := sys.Supernode.InteropDebugState()
		if state.Frontier == nil || state.Frontier.Timestamp != targetTS {
			return false
		}
		frontierAfter = state.Frontier
		return frontierAfter.L1Heads[sys.L2A.ChainID()] != frontierBefore.L1Heads[sys.L2A.ChainID()] ||
			frontierAfter.L2Heads[sys.L2A.ChainID()] != frontierBefore.L2Heads[sys.L2A.ChainID()]
	}, 2*time.Minute, time.Second, "same targetTS should move from F1 to F2 once A catches up")
	t.Logf("frontier moved to F2 for targetTS=%d on chainA: l1=%s l2=%s", targetTS, frontierAfter.L1Heads[sys.L2A.ChainID()], frontierAfter.L2Heads[sys.L2A.ChainID()])

	sys.Supernode.ResumeInterop()
	t.Log("resumed interop to process the F2 frontier")

	require.Eventually(t, func() bool {
		current, err := sys.L2ELB.Escape().EthClient().BlockRefByNumber(ctx, execBlockNumber)
		if err != nil {
			return false
		}
		return current.Hash != replacement.Hash
	}, 2*time.Minute, time.Second, "B should leave the replacement block once the F1 deny world becomes stale under F2")

	t.Logf("frontier transitioned for target ts=%d; B left replacement height=%d old=%s new_frontier_A_l1=%s new_frontier_A_l2=%s",
		targetTS,
		execBlockNumber,
		replacement.Hash,
		frontierAfter.L1Heads[sys.L2A.ChainID()],
		frontierAfter.L2Heads[sys.L2A.ChainID()],
	)
}
