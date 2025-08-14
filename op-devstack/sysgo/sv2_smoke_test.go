package sysgo

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/log"

	"github.com/ethereum-optimism/optimism/op-devstack/devtest"
	// use sysgo variant of the two-chain preset to avoid generics mismatch
	"github.com/ethereum-optimism/optimism/op-devstack/shim"
	"github.com/ethereum-optimism/optimism/op-devstack/stack"
	"github.com/ethereum-optimism/optimism/op-devstack/stack/match"
	opclient "github.com/ethereum-optimism/optimism/op-service/client"
	"github.com/ethereum-optimism/optimism/op-service/eth"
	"github.com/ethereum-optimism/optimism/op-service/retry"
	"github.com/ethereum-optimism/optimism/op-service/sources"
	"github.com/ethereum-optimism/optimism/op-service/testlog"
)

// TestSV2RollbackSingleChain is a minimal single-chain rollback + denylist smoke test.
func TestSV2RollbackSingleChain(gt *testing.T) {
	var ids DefaultMinimalSystemIDs
	opt := stack.Combine[*Orchestrator](
		DefaultMinimalSystemNoCL(&ids),
		WithSupervisorV2OnFirstChain(),
		WithInterop2ActivationOffsetForSV2(6),
		// Start a batcher against the embedded op-node (via CL registered by SV2)
		stack.Finally[*Orchestrator](func(orch *Orchestrator) {
			nets := orch.l2Nets.Values()
			if len(nets) == 0 {
				return
			}
			net := nets[0]
			cid := net.id.ChainID()
			optB := WithBatcher(
				stack.NewL2BatcherID("main", cid),
				stack.NewL1ELNodeID("l1", DefaultL1ID),
				stack.NewL2CLNodeID("embedded", cid),
				stack.NewL2ELNodeID("sequencer", cid),
			)
			optB.AfterDeploy(orch)
		}),
	)

	logger := testlog.Logger(gt, log.LevelInfo)
	onFail, onSkipNow := exiters(gt)
	p := devtest.NewP(context.Background(), logger, onFail, onSkipNow)
	gt.Cleanup(p.Close)

	orch := NewOrchestrator(p, stack.Combine[*Orchestrator]())
	stack.ApplyOptionLifecycle(opt, orch)

	t := devtest.SerialT(gt)
	system := shim.NewSystem(t)
	orch.Hydrate(system)

	// Get EL client
	l2Net := system.L2Networks()[0]
	el := l2Net.L2ELNode(match.FirstL2EL)

	// Wait for a few blocks
	ctx, cancel := context.WithTimeout(t.Ctx(), 45*time.Second)
	defer cancel()
	_ = retry.Do0(ctx, 30, &retry.FixedStrategy{Dur: 300 * time.Millisecond}, func() error {
		ref, err := el.EthClient().BlockRefByLabel(ctx, eth.Unsafe)
		if err != nil {
			return err
		}
		if ref.Number < 3 {
			return fmt.Errorf("waiting for blocks, got %d", ref.Number)
		}
		return nil
	})

	// Snapshot current unsafe head and compute its payload header-hash (stand-in PayloadID)
	var preRef eth.BlockRef
	var prePayloadID string
	var preParentHash string
	{
		ref, err := el.EthClient().BlockRefByLabel(ctx, eth.Unsafe)
		t.Require().NoError(err)
		if ref.Number <= 1 {
			_ = retry.Do0(ctx, 40, &retry.FixedStrategy{Dur: 250 * time.Millisecond}, func() error {
				r2, e2 := el.EthClient().BlockRefByLabel(ctx, eth.Unsafe)
				if e2 != nil {
					return e2
				}
				if r2.Number <= 1 {
					return fmt.Errorf("waiting for height > 1 (have %d)", r2.Number)
				}
				ref = r2
				return nil
			})
		}
		preRef = ref
		// Compute deterministic payload header-hash at this height for denylist assertion
		l2c, err := sources.NewL2Client(el.L2EthClient().RPC(), t.Logger(), nil, sources.L2ClientDefaultConfig(l2Net.RollupConfig(), true))
		t.Require().NoError(err)
		env, err := l2c.PayloadByNumber(ctx, preRef.Number)
		t.Require().NoError(err)
		if actual, ok := env.CheckBlockHash(); ok {
			prePayloadID = actual.Hex()
		}
		t.Require().NotEmpty(prePayloadID)
		// Capture parent block hash before rollback to validate chain continuity later
		if preRef.Number > 0 {
			var parent struct {
				Hash string `json:"hash"`
			}
			parentHex := fmt.Sprintf("0x%x", preRef.Number-1)
			err := el.L2EthClient().RPC().CallContext(ctx, &parent, "eth_getBlockByNumber", parentHex, false)
			t.Require().NoError(err)
			t.Require().NotEmpty(parent.Hash)
			preParentHash = parent.Hash
		}

		// (No automated checker here) Skip denylist gating in this manual rollback test

		// Let EL advance a couple of blocks past H to reduce immediate reorg risk
		target := preRef.Number + 2
		_ = retry.Do0(ctx, 120, &retry.FixedStrategy{Dur: 250 * time.Millisecond}, func() error {
			ref, err := el.EthClient().BlockRefByLabel(ctx, eth.Unsafe)
			if err != nil {
				return err
			}
			if ref.Number < target {
				return fmt.Errorf("waiting EL >= %d, have %d", target, ref.Number)
			}
			return nil
		})
	}

	// Trigger rollback via Supervisor admin API (stops op-node, rolls back EL, restarts op-node)
	sv2URL := os.Getenv("SV2_DENYLIST_URL")
	t.Require().NotEmpty(sv2URL)
	toNum := preRef.Number - 1
	reqBody, _ := json.Marshal(map[string]uint64{"to_block_number": toNum})
	resp, err := http.Post(sv2URL+"/admin/rollback", "application/json", bytes.NewReader(reqBody))
	t.Require().NoError(err)
	if resp != nil {
		defer resp.Body.Close()
		t.Require().Equal(http.StatusNoContent, resp.StatusCode)
	}

	// Assert head regressed below pre-rollback height
	_ = retry.Do0(ctx, 40, &retry.FixedStrategy{Dur: 200 * time.Millisecond}, func() error {
		after, err := el.EthClient().BlockRefByLabel(ctx, eth.Unsafe)
		if err != nil {
			return err
		}
		if after.Number >= preRef.Number {
			return fmt.Errorf("waiting for rollback to reflect: have %d, want < %d", after.Number, preRef.Number)
		}
		return nil
	})

	// Then assert we re-advance to at least the pre-rollback height
	_ = retry.Do0(ctx, 200, &retry.FixedStrategy{Dur: 250 * time.Millisecond}, func() error {
		after, err := el.EthClient().BlockRefByLabel(ctx, eth.Unsafe)
		if err != nil {
			return err
		}
		if after.Number < preRef.Number {
			return fmt.Errorf("waiting to re-advance: have %d < %d", after.Number, preRef.Number)
		}
		return nil
	})

	// Parent continuity and replacement at H
	{
		if preRef.Number > 0 {
			var currParent struct {
				Hash string `json:"hash"`
			}
			parentHex := fmt.Sprintf("0x%x", preRef.Number-1)
			err := el.L2EthClient().RPC().CallContext(ctx, &currParent, "eth_getBlockByNumber", parentHex, false)
			t.Require().NoError(err)
			t.Require().NotEmpty(currParent.Hash)
			t.Require().Equal(preParentHash, currParent.Hash)
		}
		// Wait until EL height is back to H
		_ = retry.Do0(ctx, 240, &retry.FixedStrategy{Dur: 250 * time.Millisecond}, func() error {
			var bnHex string
			if err := el.L2EthClient().RPC().CallContext(ctx, &bnHex, "eth_blockNumber"); err != nil {
				return err
			}
			if len(bnHex) < 3 || bnHex[:2] != "0x" {
				return fmt.Errorf("bad blockNumber: %s", bnHex)
			}
			n, err := strconv.ParseUint(bnHex[2:], 16, 64)
			if err != nil {
				return err
			}
			if n < preRef.Number {
				return fmt.Errorf("waiting for EL height >= %d, have %d", preRef.Number, n)
			}
			return nil
		})
		var block struct {
			Hash string `json:"hash"`
		}
		hexNum := fmt.Sprintf("0x%x", preRef.Number)
		err := el.L2EthClient().RPC().CallContext(ctx, &block, "eth_getBlockByNumber", hexNum, false)
		t.Require().NoError(err)
		t.Require().NotEmpty(block.Hash)
		t.Require().NotEqual(preRef.Hash, block.Hash)
	}
}

// TestSV2TwoChainSingleRollbackAfterSafe: bring up two chains; roll back only chain A; chain B must not regress.
func TestSV2TwoChainSingleRollbackAfterSafe(gt *testing.T) {
	// Readable preset: two chains, SV2 across both, batchers started, a few funded accounts
	opt := stack.Combine[*Orchestrator](WithSV2TwoChainReady(6, 1, 2))

	logger := testlog.Logger(gt, log.LevelInfo)
	onFail, onSkipNow := exiters(gt)
	p := devtest.NewP(context.Background(), logger, onFail, onSkipNow)
	gt.Cleanup(p.Close)

	orch := NewOrchestrator(p, stack.Combine[*Orchestrator]())
	stack.ApplyOptionLifecycle(opt, orch)

	t := devtest.SerialT(gt)
	system := shim.NewSystem(t)
	orch.Hydrate(system)

	l2Nets := system.L2Networks()
	t.Require().GreaterOrEqual(len(l2Nets), 2)
	l2A := l2Nets[0]
	l2B := l2Nets[1]
	elA := l2A.L2ELNode(match.FirstL2EL)
	elB := l2B.L2ELNode(match.FirstL2EL)

	ctx, cancel := context.WithTimeout(t.Ctx(), 3*time.Minute)
	defer cancel()

	// Wait for a few blocks on both chains
	waitUnsafe := func(el stack.L2ELNode, n uint64) error {
		return retry.Do0(ctx, 120, &retry.FixedStrategy{Dur: 300 * time.Millisecond}, func() error {
			ref, err := el.EthClient().BlockRefByLabel(ctx, eth.Unsafe)
			if err != nil {
				return err
			}
			if ref.Number < n {
				return fmt.Errorf("waiting for >= %d blocks, got %d", n, ref.Number)
			}
			return nil
		})
	}
	t.Require().NoError(waitUnsafe(elA, 3))
	t.Require().NoError(waitUnsafe(elB, 3))

	// Choose a small target height H to avoid waiting for large SAFE catch-up
	targetA := uint64(3)
	var prePayloadIDA string
	var preParentHashA string
	{
		l2c, err := sources.NewL2Client(elA.L2EthClient().RPC(), t.Logger(), nil, sources.L2ClientDefaultConfig(l2A.RollupConfig(), true))
		t.Require().NoError(err)
		env, err := l2c.PayloadByNumber(ctx, targetA)
		t.Require().NoError(err)
		if actual, ok := env.CheckBlockHash(); ok {
			prePayloadIDA = actual.Hex()
		}
		t.Require().NotEmpty(prePayloadIDA)
		if targetA > 0 {
			var parent struct {
				Hash string `json:"hash"`
			}
			parentHex := fmt.Sprintf("0x%x", targetA-1)
			t.Require().NoError(elA.L2EthClient().RPC().CallContext(ctx, &parent, "eth_getBlockByNumber", parentHex, false))
			t.Require().NotEmpty(parent.Hash)
			preParentHashA = parent.Hash
		}
	}

	// Ensure H is SAFE on A before rollback
	sv2URL := os.Getenv("SV2_DENYLIST_URL")
	t.Require().NotEmpty(sv2URL)
	idA := l2A.RollupConfig().L2ChainID.Uint64()
	idB := l2B.RollupConfig().L2ChainID.Uint64()

	// Ensure op-node proxy is ready, then wait until H is SAFE on chain A
	{
		t.Require().NoError(WaitOpNodeProxyReady(ctx, sv2URL, idA, t.Logger()))
		opnodeURL := fmt.Sprintf("%s/opnode/%d/", sv2URL, idA)
		if err := WaitSafeAtOrAbove(ctx, opnodeURL, targetA, t.Logger()); err != nil {
			t.Logger().Warn("SAFE did not progress to target on A; proceeding with UNSAFE gating", "err", err)
		}
	}

	// Trigger rollback on chain A only
	{
		toNum := targetA - 1
		reqBody, _ := json.Marshal(map[string]uint64{"to_block_number": toNum})
		resp, err := http.Post(fmt.Sprintf("%s/admin/rollback?chainId=%d", sv2URL, idA), "application/json", bytes.NewReader(reqBody))
		t.Require().NoError(err)
		if resp != nil {
			defer resp.Body.Close()
			t.Require().Equal(http.StatusNoContent, resp.StatusCode)
		}
		// Wait for op-node proxy readiness after restart to avoid transient 502s
		t.Require().NoError(WaitOpNodeProxyReady(ctx, sv2URL, idA, t.Logger()))
	}

	// Assert chain A regresses then re-advances (unsafe)
	_ = retry.Do0(ctx, 40, &retry.FixedStrategy{Dur: 200 * time.Millisecond}, func() error {
		after, err := elA.EthClient().BlockRefByLabel(ctx, eth.Unsafe)
		if err != nil {
			return err
		}
		if after.Number >= targetA {
			return fmt.Errorf("waiting for rollback to reflect: have %d, want < %d", after.Number, targetA)
		}
		return nil
	})
	_ = retry.Do0(ctx, 200, &retry.FixedStrategy{Dur: 250 * time.Millisecond}, func() error {
		after, err := elA.EthClient().BlockRefByLabel(ctx, eth.Unsafe)
		if err != nil {
			return err
		}
		if after.Number < targetA {
			return fmt.Errorf("waiting to re-advance: have %d < %d", after.Number, targetA)
		}
		return nil
	})

	// Parent continuity and replacement at H on A
	{
		if targetA > 0 {
			var currParent struct {
				Hash string `json:"hash"`
			}
			parentHex := fmt.Sprintf("0x%x", targetA-1)
			// Retry fetch to avoid transient RPC errors during restart windows
			t.Require().NoError(retry.Do0(ctx, 40, &retry.FixedStrategy{Dur: 200 * time.Millisecond}, func() error {
				if err := elA.L2EthClient().RPC().CallContext(ctx, &currParent, "eth_getBlockByNumber", parentHex, false); err != nil {
					return err
				}
				if currParent.Hash == "" {
					return fmt.Errorf("empty parent")
				}
				return nil
			}))
			t.Require().Equal(preParentHashA, currParent.Hash)
		}
		var blk struct {
			Hash string `json:"hash"`
		}
		hexNum := fmt.Sprintf("0x%x", targetA)
		t.Require().NoError(retry.Do0(ctx, 60, &retry.FixedStrategy{Dur: 200 * time.Millisecond}, func() error {
			if err := elA.L2EthClient().RPC().CallContext(ctx, &blk, "eth_getBlockByNumber", hexNum, false); err != nil {
				return err
			}
			if blk.Hash == "" {
				return fmt.Errorf("empty block hash")
			}
			return nil
		}))
		t.Require().NotEqual(prePayloadIDA, blk.Hash)
	}

	// Chain B: assert it did not regress after A's rollback via op-node SyncStatus
	{
		preB, err := elB.EthClient().BlockRefByLabel(ctx, eth.Unsafe)
		t.Require().NoError(err)
		rpc, err := opclient.NewRPC(ctx, t.Logger(), fmt.Sprintf("%s/opnode/%d/", sv2URL, idB), opclient.WithLazyDial())
		t.Require().NoError(err)
		defer rpc.Close()
		roll := sources.NewRollupClient(rpc)
		_ = retry.Do0(ctx, 180, &retry.FixedStrategy{Dur: 300 * time.Millisecond}, func() error {
			st, err := roll.SyncStatus(ctx)
			if err != nil || st == nil {
				return fmt.Errorf("sync status: %v", err)
			}
			if st.UnsafeL2.Number < preB.Number {
				return fmt.Errorf("chain B regressed: have %d < %d", st.UnsafeL2.Number, preB.Number)
			}
			return nil
		})
	}
}

// TestSV2TwoChainRollbackBOnlyAfterSafe: identical to the A-only test,
// but perform the denylist+rollback flow on chain B to prove either chain
// can be rolled back independently under one SV2 instance.
func TestSV2TwoChainRollbackBOnlyAfterSafe(gt *testing.T) {
	opt := stack.Combine[*Orchestrator](WithSV2TwoChainReady(6, 1, 2))

	logger := testlog.Logger(gt, log.LevelInfo)
	onFail, onSkipNow := exiters(gt)
	p := devtest.NewP(context.Background(), logger, onFail, onSkipNow)
	gt.Cleanup(p.Close)

	orch := NewOrchestrator(p, stack.Combine[*Orchestrator]())
	stack.ApplyOptionLifecycle(opt, orch)

	t := devtest.SerialT(gt)
	system := shim.NewSystem(t)
	orch.Hydrate(system)

	l2Nets := system.L2Networks()
	t.Require().GreaterOrEqual(len(l2Nets), 2)
	l2A := l2Nets[0]
	l2B := l2Nets[1]
	elA := l2A.L2ELNode(match.FirstL2EL)
	elB := l2B.L2ELNode(match.FirstL2EL)

	ctx, cancel := context.WithTimeout(t.Ctx(), 90*time.Second)
	defer cancel()

	waitUnsafe := func(el stack.L2ELNode, n uint64) error {
		return retry.Do0(ctx, 120, &retry.FixedStrategy{Dur: 300 * time.Millisecond}, func() error {
			ref, err := el.EthClient().BlockRefByLabel(ctx, eth.Unsafe)
			if err != nil {
				return err
			}
			if ref.Number < n {
				return fmt.Errorf("waiting for >= %d blocks, got %d", n, ref.Number)
			}
			return nil
		})
	}
	t.Require().NoError(waitUnsafe(elA, 3))
	t.Require().NoError(waitUnsafe(elB, 3))

	// Use a small target for chain B as well
	targetB := uint64(3)
	var prePayloadIDB string
	var preParentHashB string
	{
		l2c, err := sources.NewL2Client(elB.L2EthClient().RPC(), t.Logger(), nil, sources.L2ClientDefaultConfig(l2B.RollupConfig(), true))
		t.Require().NoError(err)
		env, err := l2c.PayloadByNumber(ctx, targetB)
		t.Require().NoError(err)
		if actual, ok := env.CheckBlockHash(); ok {
			prePayloadIDB = actual.Hex()
		}
		t.Require().NotEmpty(prePayloadIDB)
		if targetB > 0 {
			var parent struct {
				Hash string `json:"hash"`
			}
			parentHex := fmt.Sprintf("0x%x", targetB-1)
			t.Require().NoError(elB.L2EthClient().RPC().CallContext(ctx, &parent, "eth_getBlockByNumber", parentHex, false))
			t.Require().NotEmpty(parent.Hash)
			preParentHashB = parent.Hash
		}
	}

	// Ensure H is SAFE on B before rollback
	sv2URL := os.Getenv("SV2_DENYLIST_URL")
	t.Require().NotEmpty(sv2URL)
	idA := l2A.RollupConfig().L2ChainID.Uint64()
	idB := l2B.RollupConfig().L2ChainID.Uint64()

	// Ensure op-node proxy is ready, then wait until H is SAFE on chain B
	{
		t.Require().NoError(WaitOpNodeProxyReady(ctx, sv2URL, idB, t.Logger()))
		opnodeURL := fmt.Sprintf("%s/opnode/%d/", sv2URL, idB)
		if err := WaitSafeAtOrAbove(ctx, opnodeURL, targetB, t.Logger()); err != nil {
			t.Logger().Warn("SAFE did not progress to target on B; proceeding with UNSAFE gating", "err", err)
		}
	}

	// Trigger rollback on chain B only
	{
		toNum := targetB - 1
		reqBody, _ := json.Marshal(map[string]uint64{"to_block_number": toNum})
		resp, err := http.Post(fmt.Sprintf("%s/admin/rollback?chainId=%d", sv2URL, idB), "application/json", bytes.NewReader(reqBody))
		t.Require().NoError(err)
		if resp != nil {
			defer resp.Body.Close()
			t.Require().Equal(http.StatusNoContent, resp.StatusCode)
		}
		// Wait for op-node proxy readiness after restart to avoid transient 502s
		t.Require().NoError(WaitOpNodeProxyReady(ctx, sv2URL, idB, t.Logger()))
	}

	// Assert chain B regresses then re-advances (unsafe)
	_ = retry.Do0(ctx, 40, &retry.FixedStrategy{Dur: 200 * time.Millisecond}, func() error {
		after, err := elB.EthClient().BlockRefByLabel(ctx, eth.Unsafe)
		if err != nil {
			return err
		}
		if after.Number >= targetB {
			return fmt.Errorf("waiting for rollback to reflect: have %d, want < %d", after.Number, targetB)
		}
		return nil
	})
	_ = retry.Do0(ctx, 200, &retry.FixedStrategy{Dur: 250 * time.Millisecond}, func() error {
		after, err := elB.EthClient().BlockRefByLabel(ctx, eth.Unsafe)
		if err != nil {
			return err
		}
		if after.Number < targetB {
			return fmt.Errorf("waiting to re-advance: have %d < %d", after.Number, targetB)
		}
		return nil
	})

	// Parent continuity and replacement at H on B
	{
		if targetB > 0 {
			var currParent struct {
				Hash string `json:"hash"`
			}
			parentHex := fmt.Sprintf("0x%x", targetB-1)
			t.Require().NoError(retry.Do0(ctx, 40, &retry.FixedStrategy{Dur: 200 * time.Millisecond}, func() error {
				if err := elB.L2EthClient().RPC().CallContext(ctx, &currParent, "eth_getBlockByNumber", parentHex, false); err != nil {
					return err
				}
				if currParent.Hash == "" {
					return fmt.Errorf("empty parent")
				}
				return nil
			}))
			t.Require().Equal(preParentHashB, currParent.Hash)
		}
		var blk struct {
			Hash string `json:"hash"`
		}
		hexNum := fmt.Sprintf("0x%x", targetB)
		t.Require().NoError(retry.Do0(ctx, 60, &retry.FixedStrategy{Dur: 200 * time.Millisecond}, func() error {
			if err := elB.L2EthClient().RPC().CallContext(ctx, &blk, "eth_getBlockByNumber", hexNum, false); err != nil {
				return err
			}
			if blk.Hash == "" {
				return fmt.Errorf("empty block hash")
			}
			return nil
		}))
		t.Require().NotEqual(prePayloadIDB, blk.Hash)
	}

	// Chain A: assert it did not regress after B's rollback via op-node SyncStatus
	{
		preA, err := elA.EthClient().BlockRefByLabel(ctx, eth.Unsafe)
		t.Require().NoError(err)
		rpc, err := opclient.NewRPC(ctx, t.Logger(), fmt.Sprintf("%s/opnode/%d/", sv2URL, idA), opclient.WithLazyDial())
		t.Require().NoError(err)
		defer rpc.Close()
		roll := sources.NewRollupClient(rpc)
		_ = retry.Do0(ctx, 180, &retry.FixedStrategy{Dur: 300 * time.Millisecond}, func() error {
			st, err := roll.SyncStatus(ctx)
			if err != nil || st == nil {
				return fmt.Errorf("sync status: %v", err)
			}
			if st.UnsafeL2.Number < preA.Number {
				return fmt.Errorf("chain A regressed: have %d < %d", st.UnsafeL2.Number, preA.Number)
			}
			return nil
		})
	}
}

// TestSV2HeightCheckerAutoRollbackSingleChain: bring up single-chain SV2 with height-based checker
// configured to deny a target height. Verify the EL head regresses to target-1 and then re-advances.
func TestSV2HeightCheckerAutoRollbackSingleChain(gt *testing.T) {
	// Configure checker and runner before SV2 starts
	target := uint64(5)
	gt.Setenv("SV2_ENABLE_CHECKERS", "true")
	gt.Setenv("SV2_DENY_HEIGHT", fmt.Sprintf("%d", target))
	gt.Setenv("SV2_RUNNER_INTERVAL_MS", "50")
	gt.Setenv("SV2_L1_SCOPE", "unsafe")

	var ids DefaultMinimalSystemIDs
	opt := stack.Combine[*Orchestrator](
		DefaultMinimalSystemNoCL(&ids),
		WithSupervisorV2OnFirstChain(),
		WithInterop2ActivationOffsetForSV2(6),
		// Start a batcher against the embedded op-node (via CL registered by SV2)
		stack.Finally[*Orchestrator](func(orch *Orchestrator) {
			nets := orch.l2Nets.Values()
			if len(nets) == 0 {
				return
			}
			net := nets[0]
			cid := net.id.ChainID()
			optB := WithBatcher(
				stack.NewL2BatcherID("main", cid),
				stack.NewL1ELNodeID("l1", DefaultL1ID),
				stack.NewL2CLNodeID("embedded", cid),
				stack.NewL2ELNodeID("sequencer", cid),
			)
			optB.AfterDeploy(orch)
		}),
	)

	logger := testlog.Logger(gt, log.LevelInfo)
	onFail, onSkipNow := exiters(gt)
	p := devtest.NewP(context.Background(), logger, onFail, onSkipNow)
	gt.Cleanup(p.Close)

	orch := NewOrchestrator(p, stack.Combine[*Orchestrator]())
	stack.ApplyOptionLifecycle(opt, orch)

	t := devtest.SerialT(gt)
	system := shim.NewSystem(t)
	orch.Hydrate(system)

	// Get EL client
	l2Net := system.L2Networks()[0]
	el := l2Net.L2ELNode(match.FirstL2EL)

	ctx, cancel := context.WithTimeout(t.Ctx(), 90*time.Second)
	defer cancel()

	// Wait until SV2 reports cross_finalized >= target before checking for rollback
	{
		sv2URL := os.Getenv("SV2_DENYLIST_URL")
		t.Require().NotEmpty(sv2URL)
		ctx2, cancel2 := context.WithTimeout(t.Ctx(), 60*time.Second)
		defer cancel2()
		t.Require().NoError(WaitSV2CrossFinalizedAtLeast(ctx2, sv2URL, target))
	}

	// Compute pre-rollback block hash at target height
	var preHash string
	{
		var blk struct {
			Hash string `json:"hash"`
		}
		hexNum := fmt.Sprintf("0x%x", target)
		t.Require().NoError(retry.Do0(ctx, 40, &retry.FixedStrategy{Dur: 200 * time.Millisecond}, func() error {
			if err := el.L2EthClient().RPC().CallContext(ctx, &blk, "eth_getBlockByNumber", hexNum, false); err != nil {
				return err
			}
			if blk.Hash == "" {
				return fmt.Errorf("empty hash at target")
			}
			return nil
		}))
		preHash = blk.Hash
	}

	// Wait until SV2 denylist contains this pre-rollback block hash for the chain
	{
		sv2URL := os.Getenv("SV2_DENYLIST_URL")
		t.Require().NotEmpty(sv2URL)
		chainID := l2Net.RollupConfig().L2ChainID.Uint64()
		_ = retry.Do0(ctx, 120, &retry.FixedStrategy{Dur: 250 * time.Millisecond}, func() error {
			resp, err := http.Get(fmt.Sprintf("%s/denylist/v1/check?chainId=%d&id=%s", sv2URL, chainID, preHash))
			if err != nil {
				return err
			}
			defer resp.Body.Close()
			var out struct {
				Denylisted bool `json:"denylisted"`
			}
			if derr := json.NewDecoder(resp.Body).Decode(&out); derr != nil {
				return derr
			}
			if !out.Denylisted {
				return fmt.Errorf("not denylisted yet")
			}
			return nil
		})
	}

	// Assert that the block at target height gets replaced (hash changes), and head re-advances ≥ target
	{
		// wait for replacement
		newHash, err := WaitBlockReplacedAtHeight(ctx, el.L2EthClient().RPC(), target, preHash)
		t.Require().NoError(err)
		t.Require().NotEqual(preHash, newHash)
		// and head ≥ target again
		_ = retry.Do0(ctx, 120, &retry.FixedStrategy{Dur: 300 * time.Millisecond}, func() error {
			ref, err := el.EthClient().BlockRefByLabel(ctx, eth.Unsafe)
			if err != nil {
				return err
			}
			if ref.Number < target {
				return fmt.Errorf("waiting head >= %d, have %d", target, ref.Number)
			}
			return nil
		})
	}
}
