package sysgo

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/log"
	"github.com/stretchr/testify/require"

	"github.com/ethereum-optimism/optimism/op-devstack/devtest"
	// use sysgo variant of the two-chain preset to avoid generics mismatch
	"github.com/ethereum-optimism/optimism/op-devstack/shim"
	"github.com/ethereum-optimism/optimism/op-devstack/stack"
	"github.com/ethereum-optimism/optimism/op-devstack/stack/match"
	"github.com/ethereum-optimism/optimism/op-service/eth"

	"github.com/ethereum-optimism/optimism/op-service/testlog"
)

func TestJustSitThere(gt *testing.T) {
	//////////////////////////////////////////////////////////////////////
	// variables to control test behavior
	const testName = "JustSitThere"
	const finalityCheckHeight = uint64(10)

	//////////////////////////////////////////////////////////////////////
	// set up a minimal system with SV2 embedding an op-node

	// test setup
	t := devtest.SerialT(gt)
	gt.Logf("%s: Starting system setup", testName)
	logger := testlog.Logger(gt, log.LevelInfo)
	onFail, onSkipNow := exiters(gt)
	p := devtest.NewP(context.Background(), logger, onFail, onSkipNow)
	gt.Cleanup(p.Close)
	ctx, cancel := context.WithTimeout(t.Ctx(), 600*time.Second)
	defer cancel()

	// stack setup
	var ids DefaultMinimalSystemIDs
	opt := stack.Combine[*Orchestrator](
		DefaultMinimalSystemNoCL(&ids),
		WithInterop2ActivationOffsetForSV2(4),
		WithSupervisorV2OnFirstChain(),
		// Start a batcher against the embedded op-node (via CL registered by SV2) as part of orchestrator lifecycle
		stack.Finally[*Orchestrator](func(orch *Orchestrator) {
			nets := orch.l2Nets.Values()
			if len(nets) == 0 {
				return
			}
			net := nets[0]
			cid := net.id.ChainID()
			optB := WithBatcher(stack.NewL2BatcherID("main", cid), stack.NewL1ELNodeID("l1", DefaultL1ID), stack.NewL2CLNodeID("embedded", cid), stack.NewL2ELNodeID("sequencer", cid))
			optB.AfterDeploy(orch)
		}),
	)
	orch := NewOrchestrator(p, stack.Combine[*Orchestrator]())
	stack.ApplyOptionLifecycle(opt, orch)
	system := shim.NewSystem(t)
	orch.Hydrate(system)
	gt.Logf("%s: System setup complete", testName)

	// Get EL client
	l2Net := system.L2Networks()[0]
	el := l2Net.L2ELNode(match.FirstL2EL)

	// wait for the system to be ready
	gt.Logf("%s: Waiting for SV2 to be ready", testName)
	sv2URL := os.Getenv("SV2_DENYLIST_URL")
	t.Require().NotEmpty(sv2URL)
	{
		ctx2, cancel2 := context.WithTimeout(t.Ctx(), 60*time.Second)
		defer cancel2()
		t.Require().NoError(WaitSV2Ready(ctx2, sv2URL))
	}
	gt.Logf("%s: SV2 is ready", testName)
	// test preparation complete
	//////////////////////////////////////////////////////////////////////

	//////////////////////////////////////////////////////////////////////
	// finally: assert test conditions

	// assert cross safe (finalized) is still advancing
	require.Eventually(t, func() bool {
		ref, err := el.EthClient().BlockRefByLabel(ctx, eth.Finalized)
		if err != nil {
			return false
		}
		return ref.Number >= finalityCheckHeight
	}, 60*time.Second, 300*time.Millisecond)
	//////////////////////////////////////////////////////////////////////
}

func TestManualRollback(gt *testing.T) {
	//////////////////////////////////////////////////////////////////////
	// variables to control test behavior
	const testName = "ManualRollback"
	const targetHeight = uint64(8)
	const rollbackHeight = uint64(3)
	const finalityCheckHeight = uint64(10)

	//////////////////////////////////////////////////////////////////////
	// set up a minimal system with SV2 embedding an op-node

	// test setup
	t := devtest.SerialT(gt)
	gt.Logf("%s: Starting system setup", testName)
	logger := testlog.Logger(gt, log.LevelInfo)
	onFail, onSkipNow := exiters(gt)
	p := devtest.NewP(context.Background(), logger, onFail, onSkipNow)
	gt.Cleanup(p.Close)
	ctx, cancel := context.WithTimeout(t.Ctx(), 60*time.Second)
	defer cancel()

	// stack setup
	var ids DefaultMinimalSystemIDs
	opt := stack.Combine[*Orchestrator](
		DefaultMinimalSystemNoCL(&ids),
		WithInterop2ActivationOffsetForSV2(4),
		WithSupervisorV2OnFirstChain(),
		// Start a batcher against the embedded op-node (via CL registered by SV2) as part of orchestrator lifecycle
		stack.Finally[*Orchestrator](func(orch *Orchestrator) {
			nets := orch.l2Nets.Values()
			if len(nets) == 0 {
				return
			}
			net := nets[0]
			cid := net.id.ChainID()
			optB := WithBatcher(stack.NewL2BatcherID("main", cid), stack.NewL1ELNodeID("l1", DefaultL1ID), stack.NewL2CLNodeID("embedded", cid), stack.NewL2ELNodeID("sequencer", cid))
			optB.AfterDeploy(orch)
		}),
	)
	orch := NewOrchestrator(p, stack.Combine[*Orchestrator]())
	stack.ApplyOptionLifecycle(opt, orch)
	system := shim.NewSystem(t)
	orch.Hydrate(system)
	gt.Logf("%s: System setup complete", testName)

	// Get EL client
	l2Net := system.L2Networks()[0]
	el := l2Net.L2ELNode(match.FirstL2EL)

	// wait for the system to be ready
	gt.Logf("%s: Waiting for SV2 to be ready", testName)
	sv2URL := os.Getenv("SV2_DENYLIST_URL")
	t.Require().NotEmpty(sv2URL)
	{
		ctx2, cancel2 := context.WithTimeout(t.Ctx(), 60*time.Second)
		defer cancel2()
		t.Require().NoError(WaitSV2Ready(ctx2, sv2URL))
	}
	gt.Logf("%s: SV2 is ready", testName)
	// test preparation complete
	//////////////////////////////////////////////////////////////////////

	//////////////////////////////////////////////////////////////////////
	// wait for target blocks
	gt.Logf("%s: Waiting for %d safe blocks to be produced", testName, targetHeight)
	require.Eventually(t, func() bool {
		ref, err := el.EthClient().BlockRefByLabel(ctx, eth.Safe)
		if err != nil {
			return false
		}
		return ref.Number >= targetHeight
	}, 60*time.Second, 300*time.Millisecond)
	gt.Logf("%s: Chain has reached %d safe blocks", testName, targetHeight)

	//////////////////////////////////////////////////////////////////////
	// collect the hash of the block at target height
	gt.Logf("SimpleTest: Collecting hash of block at height %d", targetHeight)
	var originalBlock struct {
		Hash string `json:"hash"`
	}
	targetHeightHex := fmt.Sprintf("0x%x", targetHeight)
	err := el.L2EthClient().RPC().CallContext(ctx, &originalBlock, "eth_getBlockByNumber", targetHeightHex, false)
	t.Require().NoError(err)
	t.Require().NotEmpty(originalBlock.Hash)
	gt.Logf("SimpleTest: Original block %d hash: %s", targetHeight, originalBlock.Hash)
	//////////////////////////////////////////////////////////////////////

	//////////////////////////////////////////////////////////////////////
	// trigger a rollback
	gt.Logf("%s: Triggering rollback to block %d", testName, rollbackHeight)
	chainID := l2Net.RollupConfig().L2ChainID.Uint64()
	reqBody, _ := json.Marshal(map[string]uint64{"to_block_number": rollbackHeight})
	resp, err := http.Post(fmt.Sprintf("%s/admin/rollback?chainId=%d", sv2URL, chainID), "application/json", bytes.NewReader(reqBody))
	t.Require().NoError(err)
	if resp != nil {
		defer resp.Body.Close()
		t.Require().Equal(http.StatusNoContent, resp.StatusCode)
	}
	gt.Logf("%s: Rollback triggered successfully", testName)
	//////////////////////////////////////////////////////////////////////

	//////////////////////////////////////////////////////////////////////
	// wait for target block again
	gt.Logf("%s: Waiting for chain to advance to safe block %d after rollback", testName, targetHeight)
	require.Eventually(t, func() bool {
		ref, err := el.EthClient().BlockRefByLabel(ctx, eth.Safe)
		if err != nil {
			return false
		}
		return ref.Number >= targetHeight
	}, 60*time.Second, 300*time.Millisecond)
	gt.Logf("SimpleTest: Chain has advanced to %d safe blocks again after rollback", targetHeight)
	//////////////////////////////////////////////////////////////////////

	//////////////////////////////////////////////////////////////////////
	// collect the hash of the block at target height again
	gt.Logf("%s: Collecting hash of block at height %d after rollback", testName, targetHeight)
	var newBlock struct {
		Hash string `json:"hash"`
	}
	err = el.L2EthClient().RPC().CallContext(ctx, &newBlock, "eth_getBlockByNumber", targetHeightHex, false)
	t.Require().NoError(err)
	t.Require().NotEmpty(newBlock.Hash)
	gt.Logf("%s: New block %d hash: %s", testName, targetHeight, newBlock.Hash)
	//////////////////////////////////////////////////////////////////////

	//////////////////////////////////////////////////////////////////////
	// finally: assert test conditions

	// assert the block is the same before and after rollback
	t.Require().Equal(originalBlock.Hash, newBlock.Hash, fmt.Sprintf("block hash at height %d should be the same after rollback", targetHeight))

	// assert cross safe (finalized) is still advancing
	require.Eventually(t, func() bool {
		ref, err := el.EthClient().BlockRefByLabel(ctx, eth.Finalized)
		if err != nil {
			return false
		}
		return ref.Number >= finalityCheckHeight
	}, 60*time.Second, 300*time.Millisecond)
	//////////////////////////////////////////////////////////////////////
}
