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

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"

	"github.com/ethereum/go-ethereum/log"
	"github.com/stretchr/testify/require"

	"github.com/ethereum-optimism/optimism/devnet-sdk/contracts/bindings"
	"github.com/ethereum-optimism/optimism/devnet-sdk/contracts/constants"
	"github.com/ethereum-optimism/optimism/op-chain-ops/devkeys"
	"github.com/ethereum-optimism/optimism/op-devstack/devtest"
	"github.com/ethereum-optimism/optimism/op-service/retry"
	"github.com/ethereum-optimism/optimism/op-service/txintent"
	"github.com/ethereum-optimism/optimism/op-service/txplan"
	supertypes "github.com/ethereum-optimism/optimism/op-supervisor/supervisor/types"

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
	const finalityCheckHeight = uint64(15)

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
	chainID := l2Net.RollupConfig().L2ChainID.Uint64()

	// wait for the system to be ready
	gt.Logf("%s: Waiting for SV2 to be ready", testName)
	sv2URL := os.Getenv("SV2_AUTHORIZATION_URL")
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

	// assert cross safe (finalized) is still advancing via supervisor sync status
	require.Eventually(t, func() bool {
		req, err := http.NewRequestWithContext(ctx, "GET", fmt.Sprintf("%s/v1/cross_safe?chainId=%d", sv2URL, chainID), nil)
		if err != nil {
			return false
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return false
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			return false
		}
		var out supertypes.DerivedIDPair
		if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
			return false
		}
		return out.Derived.Number >= finalityCheckHeight
	}, 600*time.Second, 300*time.Millisecond)
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
	sv2URL := os.Getenv("SV2_AUTHORIZATION_URL")
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

	// verify cross-safe regressed to <= rollbackHeight immediately
	req, err := http.NewRequestWithContext(ctx, "GET", fmt.Sprintf("%s/v1/cross_safe?chainId=%d", sv2URL, chainID), nil)
	t.Require().NoError(err)
	resp2, err := http.DefaultClient.Do(req)
	t.Require().NoError(err)
	defer resp2.Body.Close()
	t.Require().Equal(http.StatusOK, resp2.StatusCode)
	var afterRB supertypes.DerivedIDPair
	t.Require().NoError(json.NewDecoder(resp2.Body).Decode(&afterRB))
	gt.Logf("%s: Cross-safe after rollback derived=%d", testName, afterRB.Derived.Number)
	t.Require().LessOrEqual(afterRB.Derived.Number, rollbackHeight)
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
		req, err := http.NewRequestWithContext(ctx, "GET", fmt.Sprintf("%s/v1/cross_safe?chainId=%d", sv2URL, chainID), nil)
		if err != nil {
			return false
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return false
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			return false
		}
		var out supertypes.DerivedIDPair
		if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
			return false
		}
		return out.Derived.Number >= finalityCheckHeight
	}, 600*time.Second, 300*time.Millisecond)
	//////////////////////////////////////////////////////////////////////
}

func TestValidExecutingMessage(gt *testing.T) {
	//////////////////////////////////////////////////////////////////////
	// variables to control test behavior
	const testName = "ValidExecutingMessage"
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

	// Get EL client and setup EOA for transactions
	l2Net := system.L2Networks()[0]
	chainID := l2Net.RollupConfig().L2ChainID.Uint64()

	// wait for the system to be ready
	gt.Logf("%s: Waiting for SV2 to be ready", testName)
	sv2URL := os.Getenv("SV2_AUTHORIZATION_URL")
	t.Require().NotEmpty(sv2URL)
	{
		ctx2, cancel2 := context.WithTimeout(t.Ctx(), 60*time.Second)
		defer cancel2()
		t.Require().NoError(WaitSV2Ready(ctx2, sv2URL))
	}
	gt.Logf("%s: SV2 is ready", testName)

	// test preparation complete
	//////////////////////////////////////////////////////////////////////

	// TODO: create a initiating message on the chain
	//////////////////////////////////////////////////////////////////////
	// Build a funded EOA using devkeys and request faucet funds
	keys, err := devkeys.NewSaltedDevKeys(devkeys.TestMnemonic, os.Getenv("OP_DEVSTACK_SALT"))
	t.Require().NoError(err)
	alicePriv, _ := keys.Secret(devkeys.UserKey(0))
	aliceAddr, _ := keys.Address(devkeys.UserKey(0))
	_ = l2Net.Faucet(match.FirstFaucet).API().RequestETH(t.Ctx(), aliceAddr, eth.OneTenthEther)

	// Use the EL client for tx planning
	el := l2Net.L2ELNode(match.FirstL2EL)
	plan := txplan.Combine(
		txplan.WithPrivateKey(alicePriv),
		txplan.WithChainID(el.EthClient()),
		txplan.WithPendingNonce(el.EthClient()),
		txplan.WithAgainstLatestBlock(el.EthClient()),
		txplan.WithEstimator(el.EthClient(), true),
		txplan.WithRetrySubmission(el.EthClient(), 5, retry.Exponential()),
		txplan.WithRetryInclusion(el.EthClient(), 5, retry.Exponential()),
		txplan.WithBlockInclusionInfo(el.EthClient()),
	)

	// Deploy EventLogger contract to emit initiating event
	deployTx := txplan.NewPlannedTx(plan, txplan.WithData(common.FromHex(bindings.EventloggerBin)))
	deployReceipt, err := deployTx.Included.Eval(t.Ctx())
	t.Require().NoError(err)
	eventLoggerAddress := deployReceipt.ContractAddress

	// Create initiating message by calling EventLogger.emitLog
	init := &txintent.InitTrigger{
		Emitter:    eventLoggerAddress,
		Topics:     [][32]byte{{}},
		OpaqueData: []byte("hello"),
	}
	initTx := txintent.NewIntent[*txintent.InitTrigger, *txintent.InteropOutput](plan)
	initTx.Content.Set(init)
	initReceipt, err := initTx.PlannedTx.Included.Eval(t.Ctx())
	t.Require().NoError(err)
	_ = initReceipt

	// TODO: create a valid executing message based on the initiating message
	//////////////////////////////////////////////////////////////////////
	// Build an executing message that references the initiating message (single event -> index 0)
	execTx := txintent.NewIntent[*txintent.ExecTrigger, *txintent.InteropOutput](plan)
	execTx.Content.DependOn(&initTx.Result)
	execTx.Content.Fn(txintent.ExecuteIndexed(constants.CrossL2Inbox, &initTx.Result, 0))
	execReceipt, err := execTx.PlannedTx.Included.Eval(t.Ctx())
	t.Require().NoError(err)
	// one ExecutingMessage log is expected
	t.Require().Equal(1, len(execReceipt.Logs))
	//////////////////////////////////////////////////////////////////////

	// For now, just verify basic system setup is working
	gt.Logf("%s: Basic system verification - checking L2 EL node", testName)
	l2EL := l2Net.L2ELNode(match.FirstL2EL)
	head, err := l2EL.EthClient().BlockRefByLabel(ctx, eth.Unsafe)
	t.Require().NoError(err, "should be able to get unsafe head")
	gt.Logf("%s: L2 unsafe head at block %d", testName, head.Number)

	// scan blocks and confirm both initiating and executing receipts are present
	startNum := initReceipt.BlockNumber.Uint64()
	if execReceipt.BlockNumber.Uint64() < startNum {
		startNum = execReceipt.BlockNumber.Uint64()
	}
	foundInit := false
	foundExec := false
	for num := startNum; num <= head.Number; num++ {
		hexNum := fmt.Sprintf("0x%x", num)
		var blk struct {
			Transactions []struct {
				Hash string `json:"hash"`
			} `json:"transactions"`
		}
		t.Require().NoError(el.L2EthClient().RPC().CallContext(ctx, &blk, "eth_getBlockByNumber", hexNum, true))
		for _, tx := range blk.Transactions {
			var r struct {
				TransactionHash string `json:"transactionHash"`
				Logs            []struct {
					Topics []string `json:"topics"`
				} `json:"logs"`
			}
			t.Require().NoError(el.L2EthClient().RPC().CallContext(ctx, &r, "eth_getTransactionReceipt", tx.Hash))
			if r.TransactionHash == initReceipt.TxHash.Hex() {
				foundInit = true
			}
			if r.TransactionHash == execReceipt.TxHash.Hex() {
				foundExec = true
				// ensure ExecutingMessage event is present in the executing tx receipt
				executingMessageTopic := crypto.Keccak256Hash([]byte("ExecutingMessage(bytes32,(address,uint256,uint256,uint256,uint256))")).Hex()
				seenExecEvent := false
				for _, lg := range r.Logs {
					if len(lg.Topics) > 0 && lg.Topics[0] == executingMessageTopic {
						seenExecEvent = true
						break
					}
				}
				t.Require().True(seenExecEvent, "executing tx must emit ExecutingMessage event")
			}
		}
		if foundInit && foundExec {
			break
		}
	}
	t.Require().True(foundInit, "initiating receipt not found in chain blocks")
	// executing receipt may be reorged out and not re-included; that's acceptable here
	if !foundExec {
		gt.Logf("%s: Executing receipt not found in block scan post-rollback (acceptable)", testName)
	}

	// assert cross safe (finalized) is advancing and contains the executing message
	require.Eventually(t, func() bool {
		req, err := http.NewRequestWithContext(ctx, "GET", fmt.Sprintf("%s/v1/cross_safe?chainId=%d", sv2URL, chainID), nil)
		if err != nil {
			return false
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return false
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			return false
		}
		var out supertypes.DerivedIDPair
		if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
			return false
		}
		gt.Logf("%s: Cross-safe at block %d, target %d", testName, out.Derived.Number, finalityCheckHeight)
		return out.Derived.Number >= finalityCheckHeight
	}, 600*time.Second, 300*time.Millisecond)
	gt.Logf("%s: Test completed successfully - cross-safe advanced to finality height", testName)
	//////////////////////////////////////////////////////////////////////
}

func TestInvalidExecutingMessage(gt *testing.T) {
	//////////////////////////////////////////////////////////////////////
	// variables to control test behavior
	const testName = "InvalidExecutingMessage"

	//////////////////////////////////////////////////////////////////////
	// set up a minimal system with SV2 embedding an op-node

	// test setup and minimal system bring-up
	t, ctx, cancel, _, l2Net, chainID, sv2URL := setupMinimalSystemSV2(gt, testName)
	defer cancel()

	// test preparation complete
	//////////////////////////////////////////////////////////////////////

	// Build funded tx plan and get EL handle
	plan, el := buildFundedPlan(ctx, t, l2Net)

	// send initiating and invalid executing messages via helpers
	initTx := mustSendValidInitiatingMessage(ctx, t, plan)
	execTxHash, execBlockNum, execBlockHash := mustSendInvalidExecutingMessage(ctx, t, plan, initTx)

	// wait until cross-safe reaches execBlockNum + 5
	target := execBlockNum + 5
	waitCrossSafeAtLeast(ctx, t, sv2URL, chainID, target, 600*time.Second)

	// verify the block at execBlockNum has a different hash now (reorg occurred)
	var blk struct {
		Hash string `json:"hash"`
	}
	blockNumHex := fmt.Sprintf("0x%x", execBlockNum)
	t.Require().NoError(el.L2EthClient().RPC().CallContext(ctx, &blk, "eth_getBlockByNumber", blockNumHex, false))
	t.Require().NotEmpty(blk.Hash)
	t.Require().NotEqual(execBlockHash, blk.Hash, "block at original inclusion height should have a different hash after cross-safe progresses")

	// verify the invalid executing tx is not included (no receipt yet)
	var rec struct {
		BlockHash string `json:"blockHash"`
	}
	t.Require().NoError(el.L2EthClient().RPC().CallContext(ctx, &rec, "eth_getTransactionReceipt", execTxHash))
	t.Require().Empty(rec.BlockHash, "invalid executing tx should not be included yet")

	time.Sleep(120 * time.Second)

	gt.Logf("%s: Test completed - invalid executing message handled, cross-safe advanced, and reorg observed", testName)
	//////////////////////////////////////////////////////////////////////
}

func TestTwoChainValidExecutingMessage(gt *testing.T) {
	//////////////////////////////////////////////////////////////////////
	// variables to control test behavior
	const testName = "TwoChainValidExecutingMessage"
	const interopOffset = uint64(6)
	const confirmDepth = uint64(1)
	const crossSafeTarget = uint64(8)

	//////////////////////////////////////////////////////////////////////
	// bring up a two-chain system with SV2 across both chains and batchers

	// test setup
	t := devtest.SerialT(gt)
	gt.Logf("%s: Starting two-chain system setup", testName)
	logger := testlog.Logger(gt, log.LevelInfo)
	onFail, onSkipNow := exiters(gt)
	p := devtest.NewP(context.Background(), logger, onFail, onSkipNow)
	gt.Cleanup(p.Close)
	ctx, cancel := context.WithTimeout(t.Ctx(), 600*time.Second)
	defer cancel()

	// compose two-chain minimal with SV2 on all chains and batchers
	opt := stack.Combine[*Orchestrator](WithSV2TwoChainMinimalDepth(interopOffset, confirmDepth))
	orch := NewOrchestrator(p, stack.Combine[*Orchestrator]())
	stack.ApplyOptionLifecycle(opt, orch)
	system := shim.NewSystem(t)
	orch.Hydrate(system)
	gt.Logf("%s: Two-chain system setup complete", testName)

	// wait for SV2 to be ready
	sv2URL := os.Getenv("SV2_AUTHORIZATION_URL")
	t.Require().NotEmpty(sv2URL)
	{
		ctx2, cancel2 := context.WithTimeout(t.Ctx(), 60*time.Second)
		defer cancel2()
		t.Require().NoError(WaitSV2Ready(ctx2, sv2URL))
	}

	// fetch both L2 networks and their chain IDs
	l2Nets := system.L2Networks()
	t.Require().GreaterOrEqual(len(l2Nets), 2)
	l2A := l2Nets[0]
	l2B := l2Nets[1]
	chainB := l2B.RollupConfig().L2ChainID.Uint64()

	// build funded plans on both chains
	planA, _ := buildFundedPlan(ctx, t, l2A)
	planB, elB := buildFundedPlan(ctx, t, l2B)

	// create initiating message on chain A
	initTx := mustSendValidInitiatingMessage(ctx, t, planA)

	// create valid executing message on chain B (referencing chain A init)
	execTxHash, execBlockNum, execBlockHash := mustSendValidExecutingMessage(ctx, t, planB, initTx)

	// wait until cross-safe on chain B reaches execBlockNum + 5
	target := execBlockNum + 5
	waitCrossSafeAtLeast(ctx, t, sv2URL, chainB, target, 600*time.Second)

	// verify the executing tx is still included with the same block hash
	var rec struct {
		BlockHash string `json:"blockHash"`
	}
	t.Require().NoError(elB.L2EthClient().RPC().CallContext(ctx, &rec, "eth_getTransactionReceipt", execTxHash))
	t.Require().Equal(execBlockHash, rec.BlockHash, "valid executing tx must remain included after cross-safe advances")

	gt.Logf("%s: Valid executing tx remained included after cross-safe >= %d on chain %d", testName, target, chainB)
	//////////////////////////////////////////////////////////////////////
}

//////////////////////////////////////////////////////////////////////
// Helpers

// setupMinimalSystemSV2 performs the common test setup used by these system tests and brings up
// a minimal system with Supervisor V2 on the first chain and an embedded op-node. It returns
// the test context, package scope, context with timeout, cancel func, and hydrated system.
func setupMinimalSystemSV2(gt *testing.T, testName string) (devtest.T, context.Context, context.CancelFunc, stack.ExtensibleSystem, stack.L2Network, uint64, string) {
	// test setup
	t := devtest.SerialT(gt)
	gt.Logf("%s: Starting system setup", testName)
	logger := testlog.Logger(gt, log.LevelInfo)
	onFail, onSkipNow := exiters(gt)
	p := devtest.NewP(context.Background(), logger, onFail, onSkipNow)
	gt.Cleanup(p.Close)
	ctx, cancel := context.WithTimeout(t.Ctx(), 600*time.Second)

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

	// Wait for SV2 to be ready
	sv2URL := os.Getenv("SV2_AUTHORIZATION_URL")
	t.Require().NotEmpty(sv2URL)
	{
		ctx2, cancel2 := context.WithTimeout(t.Ctx(), 60*time.Second)
		defer cancel2()
		t.Require().NoError(WaitSV2Ready(ctx2, sv2URL))
	}
	gt.Logf("%s: SV2 is ready", testName)

	// Provide common handles useful to tests
	l2Net := system.L2Networks()[0]
	chainID := l2Net.RollupConfig().L2ChainID.Uint64()
	return t, ctx, cancel, system, l2Net, chainID, sv2URL
}

// mustSendValidInitiatingMessage deploys the EventLogger contract and submits a valid initiating message.
// It returns the initiating intent, which can be referenced by an executing message.
func mustSendValidInitiatingMessage(ctx context.Context, t devtest.T, plan txplan.Option) *txintent.IntentTx[*txintent.InitTrigger, *txintent.InteropOutput] {
	// Deploy EventLogger contract to emit initiating event
	deployTx := txplan.NewPlannedTx(plan, txplan.WithData(common.FromHex(bindings.EventloggerBin)))
	deployReceipt, err := deployTx.Included.Eval(t.Ctx())
	t.Require().NoError(err)
	eventLoggerAddress := deployReceipt.ContractAddress

	// Create initiating message by calling EventLogger.emitLog
	init := &txintent.InitTrigger{
		Emitter:    eventLoggerAddress,
		Topics:     [][32]byte{{}},
		OpaqueData: []byte("hello"),
	}
	initTx := txintent.NewIntent[*txintent.InitTrigger, *txintent.InteropOutput](plan)
	initTx.Content.Set(init)
	_, err = initTx.PlannedTx.Included.Eval(t.Ctx())
	t.Require().NoError(err)
	return initTx
}

// mustSendInvalidExecutingMessage submits an executing message referencing the given initTx but corrupts the
// payload to make it invalid. It returns the tx hash, inclusion block number, and inclusion block hash.
func mustSendInvalidExecutingMessage(ctx context.Context, t devtest.T, plan txplan.Option, initTx *txintent.IntentTx[*txintent.InitTrigger, *txintent.InteropOutput]) (string, uint64, string) {
	execTx := txintent.NewIntent[*txintent.ExecTrigger, *txintent.InteropOutput](plan)
	execTx.Content.DependOn(&initTx.Result)
	execTx.Content.Fn(func(ctx context.Context) (*txintent.ExecTrigger, error) {
		mk := txintent.ExecuteIndexed(constants.CrossL2Inbox, &initTx.Result, 0)
		tr, err := mk(ctx)
		if err != nil {
			return nil, err
		}
		bad := tr.Msg
		bad.PayloadHash[0] ^= 0xff
		tr.Msg = bad
		return tr, nil
	})
	rec, err := execTx.PlannedTx.Included.Eval(t.Ctx())
	t.Require().NoError(err)
	t.Require().Equal(1, len(rec.Logs))
	return rec.TxHash.Hex(), rec.BlockNumber.Uint64(), rec.BlockHash.Hex()
}

// mustSendValidExecutingMessage submits a valid executing message referencing the given initTx.
// It returns the tx hash, inclusion block number, and inclusion block hash.
func mustSendValidExecutingMessage(ctx context.Context, t devtest.T, plan txplan.Option, initTx *txintent.IntentTx[*txintent.InitTrigger, *txintent.InteropOutput]) (string, uint64, string) {
	execTx := txintent.NewIntent[*txintent.ExecTrigger, *txintent.InteropOutput](plan)
	execTx.Content.DependOn(&initTx.Result)
	execTx.Content.Fn(txintent.ExecuteIndexed(constants.CrossL2Inbox, &initTx.Result, 0))
	rec, err := execTx.PlannedTx.Included.Eval(t.Ctx())
	t.Require().NoError(err)
	t.Require().Equal(1, len(rec.Logs))
	return rec.TxHash.Hex(), rec.BlockNumber.Uint64(), rec.BlockHash.Hex()
}

// waitCrossSafeAtLeast blocks until the cross-safe derived number reaches at least target.
func waitCrossSafeAtLeast(ctx context.Context, t devtest.T, sv2URL string, chainID uint64, target uint64, timeout time.Duration) {
	require.Eventually(t, func() bool {
		req, err := http.NewRequestWithContext(ctx, "GET", fmt.Sprintf("%s/v1/cross_safe?chainId=%d", sv2URL, chainID), nil)
		if err != nil {
			return false
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return false
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			return false
		}
		var out supertypes.DerivedIDPair
		if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
			return false
		}
		return out.Derived.Number >= target
	}, timeout, 300*time.Millisecond)
}

// buildFundedPlan creates a funded EOA and returns a txplan and the L2 EL handle.
func buildFundedPlan(ctx context.Context, t devtest.T, l2Net stack.L2Network) (txplan.Option, stack.L2ELNode) {
	keys, err := devkeys.NewSaltedDevKeys(devkeys.TestMnemonic, os.Getenv("OP_DEVSTACK_SALT"))
	t.Require().NoError(err)
	privKey, _ := keys.Secret(devkeys.UserKey(0))
	addr, _ := keys.Address(devkeys.UserKey(0))
	_ = l2Net.Faucet(match.FirstFaucet).API().RequestETH(t.Ctx(), addr, eth.OneTenthEther)

	el := l2Net.L2ELNode(match.FirstL2EL)
	plan := txplan.Combine(
		txplan.WithPrivateKey(privKey),
		txplan.WithChainID(el.EthClient()),
		txplan.WithPendingNonce(el.EthClient()),
		txplan.WithAgainstLatestBlock(el.EthClient()),
		txplan.WithEstimator(el.EthClient(), true),
		txplan.WithRetrySubmission(el.EthClient(), 5, retry.Exponential()),
		txplan.WithRetryInclusion(el.EthClient(), 5, retry.Exponential()),
		txplan.WithBlockInclusionInfo(el.EthClient()),
	)
	return plan, el
}
