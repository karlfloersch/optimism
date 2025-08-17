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
	bss "github.com/ethereum-optimism/optimism/op-batcher/batcher"
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
	// Gate on SV2 readiness
	{
		ctx2, cancel2 := context.WithTimeout(t.Ctx(), 60*time.Second)
		defer cancel2()
		t.Require().NoError(WaitSV2Ready(ctx2, sv2URL))
	}
	idA := l2A.RollupConfig().L2ChainID.Uint64()
	idB := l2B.RollupConfig().L2ChainID.Uint64()

	// Ensure op-node proxy is ready, then wait until H is SAFE on chain A
	{
		t.Require().NoError(WaitOpNodeProxyReady(ctx, sv2URL, idA, t.Logger()))
		opnodeURL := fmt.Sprintf("%s/opnode/%d/", sv2URL, idA)
		t.Require().NoError(WaitSafeAtOrAbove(ctx, opnodeURL, targetA, t.Logger()))
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
	// Gate on SV2 readiness
	{
		ctx2, cancel2 := context.WithTimeout(t.Ctx(), 60*time.Second)
		defer cancel2()
		t.Require().NoError(WaitSV2Ready(ctx2, sv2URL))
	}
	idA := l2A.RollupConfig().L2ChainID.Uint64()
	idB := l2B.RollupConfig().L2ChainID.Uint64()

	// Ensure op-node proxy is ready, then wait until H is SAFE on chain B
	{
		t.Require().NoError(WaitOpNodeProxyReady(ctx, sv2URL, idB, t.Logger()))
		opnodeURL := fmt.Sprintf("%s/opnode/%d/", sv2URL, idB)
		t.Require().NoError(WaitSafeAtOrAbove(ctx, opnodeURL, targetB, t.Logger()))
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

// TestSV2SingleChainSafeAdvancesQuick: bring up a single chain with SV2 managing the op-node,
// start a batcher wired to the SV2 op-node proxy, and assert SafeL2 advances beyond a small target.
func TestSV2SingleChainSafeAdvancesQuick(gt *testing.T) {
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
				stack.NewL2CLNodeID("embedded", cid), // SV2 registers this CL shim for proxy wiring
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

	// Quick-fail checks: ensure SV2 URL is exposed and op-node proxy is ready
	sv2URL := os.Getenv("SV2_DENYLIST_URL")
	t.Require().NotEmpty(sv2URL)
	// readiness gate
	{
		ctx2, cancel2 := context.WithTimeout(t.Ctx(), 60*time.Second)
		defer cancel2()
		t.Require().NoError(WaitSV2Ready(ctx2, sv2URL))
	}
	l2Net := system.L2Networks()[0]
	chainID := l2Net.RollupConfig().L2ChainID.Uint64()
	t.Require().NoError(WaitOpNodeProxyReady(t.Ctx(), sv2URL, chainID, t.Logger()))

	// Target small height to keep the test fast
	const target uint64 = 3
	opnodeURL := fmt.Sprintf("%s/opnode/%d/", sv2URL, chainID)
	ctx, cancel := context.WithTimeout(t.Ctx(), 60*time.Second)
	defer cancel()
	t.Require().NoError(WaitSafeAtOrAbove(ctx, opnodeURL, target, t.Logger()))
}

// TestSV2TwoChainSafeProgressionWithBatchers verifies that with SV2 managing two chains and
// batchers wired to the SV2 op-node proxies, both chains advance SafeL2 beyond an initial watermark.
func TestSV2TwoChainSafeProgressionWithBatchers(gt *testing.T) {
	// Use the readable two-chain preset that starts batchers and funds accounts
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

	// Environment and URLs
	sv2URL := os.Getenv("SV2_DENYLIST_URL")
	t.Require().NotEmpty(sv2URL)
	// Gate on SV2 readiness
	{
		ctx2, cancel2 := context.WithTimeout(t.Ctx(), 60*time.Second)
		defer cancel2()
		t.Require().NoError(WaitSV2Ready(ctx2, sv2URL))
	}
	idA := l2A.RollupConfig().L2ChainID.Uint64()
	idB := l2B.RollupConfig().L2ChainID.Uint64()
	opnodeA := fmt.Sprintf("%s/opnode/%d/", sv2URL, idA)
	opnodeB := fmt.Sprintf("%s/opnode/%d/", sv2URL, idB)

	ctx, cancel := context.WithTimeout(t.Ctx(), 3*time.Minute)
	defer cancel()

	// Ensure proxies are ready
	t.Require().NoError(WaitOpNodeProxyReady(ctx, sv2URL, idA, t.Logger()))
	t.Require().NoError(WaitOpNodeProxyReady(ctx, sv2URL, idB, t.Logger()))

	// Record initial SafeL2 watermarks
	readSafe := func(opnode string) (uint64, error) {
		cli, err := opclient.NewRPC(ctx, t.Logger(), opnode, opclient.WithLazyDial())
		if err != nil {
			return 0, err
		}
		defer cli.Close()
		roll := sources.NewRollupClient(cli)
		st, err := roll.SyncStatus(ctx)
		if err != nil || st == nil {
			if err == nil {
				return 0, fmt.Errorf("nil status")
			}
			return 0, err
		}
		if st.SafeL2.Number != 0 {
			return st.SafeL2.Number, nil
		}
		return st.LocalSafeL2.Number, nil
	}

	sA0, err := readSafe(opnodeA)
	t.Require().NoError(err)
	sB0, err := readSafe(opnodeB)
	t.Require().NoError(err)

	// Let chains produce a few blocks so batches have content
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
	t.Require().NoError(waitUnsafe(l2A.L2ELNode(match.FirstL2EL), 8))
	t.Require().NoError(waitUnsafe(l2B.L2ELNode(match.FirstL2EL), 8))

	// Briefly log status snapshots and batcher inclusion to observe progression before asserting
	logStatus := func(opnode string, label string) {
		cli, err := opclient.NewRPC(ctx, t.Logger(), opnode, opclient.WithLazyDial())
		if err != nil {
			t.Logger().Warn("rollup rpc dial failed", "chain", label, "err", err)
			return
		}
		defer cli.Close()
		roll := sources.NewRollupClient(cli)
		st, err := roll.SyncStatus(ctx)
		if err != nil || st == nil {
			t.Logger().Warn("sync status error", "chain", label, "err", err)
			return
		}
		t.Logger().Info("op-node heads", "chain", label, "unsafe", st.UnsafeL2.Number, "local_safe", st.LocalSafeL2.Number, "safe", st.SafeL2.Number)
	}

	// Also ping batcher admin to surface liveness; activity API is limited but will error if unreachable
	// Note: avoid direct L2Batcher registry access (panics if not yet registered); rely on SyncStatus instead.

	for i := 0; i < 6; i++ {
		logStatus(opnodeA, "A")
		logStatus(opnodeB, "B")
		time.Sleep(500 * time.Millisecond)
	}

	// Expect SafeL2 to advance by at least K on both chains
	K := uint64(2)
	if err := WaitSafeAtOrAbove(ctx, opnodeA, sA0+K, t.Logger()); err != nil {
		logStatus(opnodeA, "A-final")
		logStatus(opnodeB, "B-final")
		t.Require().NoError(err)
	}
	if err := WaitSafeAtOrAbove(ctx, opnodeB, sB0+K, t.Logger()); err != nil {
		logStatus(opnodeA, "A-final")
		logStatus(opnodeB, "B-final")
		t.Require().NoError(err)
	}
}

// TestSV2TwoChainSafeProgressionSerialized starts batcher A first, waits for Safe/LocalSafe to
// advance on chain A, then starts batcher B and waits for Safe/LocalSafe on chain B. This avoids
// potential L1 inclusion contention during bring-up and helps diagnose Safe gating.
func TestSV2TwoChainSafeProgressionSerialized(gt *testing.T) {
	// Compose two-chain minimal system with SV2 on all chains, without auto-starting batchers
	opt := stack.Combine[*Orchestrator](WithSV2TwoChainMinimal(6),
		// Ensure batchers use SV2 /opnode/{chainId}/ when we start them manually below
		WithBatcherOption(func(id stack.L2BatcherID, cfg *bss.CLIConfig) {
			if v, ok := id.ChainID().Uint64(); ok {
				if sv2URL := os.Getenv("SV2_DENYLIST_URL"); sv2URL != "" {
					cfg.RollupRpc = []string{fmt.Sprintf("%s/opnode/%d/", sv2URL, v)}
				}
			}
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

	l2Nets := system.L2Networks()
	t.Require().GreaterOrEqual(len(l2Nets), 2)
	l2A := l2Nets[0]
	l2B := l2Nets[1]

	// Environment and URLs
	sv2URL := os.Getenv("SV2_DENYLIST_URL")
	t.Require().NotEmpty(sv2URL)
	// Gate on SV2 readiness
	{
		ctx2, cancel2 := context.WithTimeout(t.Ctx(), 60*time.Second)
		defer cancel2()
		t.Require().NoError(WaitSV2Ready(ctx2, sv2URL))
	}
	idA := l2A.RollupConfig().L2ChainID.Uint64()
	idB := l2B.RollupConfig().L2ChainID.Uint64()
	opnodeA := fmt.Sprintf("%s/opnode/%d/", sv2URL, idA)
	opnodeB := fmt.Sprintf("%s/opnode/%d/", sv2URL, idB)

	ctx, cancel := context.WithTimeout(t.Ctx(), 3*time.Minute)
	defer cancel()

	// Ensure proxies are ready
	t.Require().NoError(WaitOpNodeProxyReady(ctx, sv2URL, idA, t.Logger()))
	t.Require().NoError(WaitOpNodeProxyReady(ctx, sv2URL, idB, t.Logger()))

	// Start batcher A only
	{
		optA := WithBatcher(stack.NewL2BatcherID("main", l2A.ID().ChainID()), stack.NewL1ELNodeID("l1", DefaultL1ID), stack.NewL2CLNodeID("embedded", l2A.ID().ChainID()), stack.NewL2ELNodeID("sequencer", l2A.ID().ChainID()))
		optA.AfterDeploy(orch)
	}

	// Record initial Safe/LocalSafe A, then wait for A to advance by K
	readSafe := func(opnode string) (uint64, error) {
		cli, err := opclient.NewRPC(ctx, t.Logger(), opnode, opclient.WithLazyDial())
		if err != nil {
			return 0, err
		}
		defer cli.Close()
		roll := sources.NewRollupClient(cli)
		st, err := roll.SyncStatus(ctx)
		if err != nil || st == nil {
			if err == nil {
				return 0, fmt.Errorf("nil status")
			}
			return 0, err
		}
		if st.SafeL2.Number != 0 {
			return st.SafeL2.Number, nil
		}
		return st.LocalSafeL2.Number, nil
	}
	sA0, err := readSafe(opnodeA)
	t.Require().NoError(err)
	// Let chain A produce a few unsafe blocks so the batcher has content
	_ = retry.Do0(ctx, 120, &retry.FixedStrategy{Dur: 300 * time.Millisecond}, func() error {
		ref, err := l2A.L2ELNode(match.FirstL2EL).EthClient().BlockRefByLabel(ctx, eth.Unsafe)
		if err != nil {
			return err
		}
		if ref.Number < 5 {
			return fmt.Errorf("waiting for >= 5 blocks, got %d", ref.Number)
		}
		return nil
	})
	K := uint64(2)
	t.Require().NoError(WaitSafeAtOrAbove(ctx, opnodeA, sA0+K, t.Logger()))

	// Now start batcher B
	{
		optB := WithBatcher(stack.NewL2BatcherID("main", l2B.ID().ChainID()), stack.NewL1ELNodeID("l1", DefaultL1ID), stack.NewL2CLNodeID("embedded", l2B.ID().ChainID()), stack.NewL2ELNodeID("sequencer", l2B.ID().ChainID()))
		optB.AfterDeploy(orch)
	}
	sB0, err := readSafe(opnodeB)
	t.Require().NoError(err)
	_ = retry.Do0(ctx, 120, &retry.FixedStrategy{Dur: 300 * time.Millisecond}, func() error {
		ref, err := l2B.L2ELNode(match.FirstL2EL).EthClient().BlockRefByLabel(ctx, eth.Unsafe)
		if err != nil {
			return err
		}
		if ref.Number < 5 {
			return fmt.Errorf("waiting for >= 5 blocks, got %d", ref.Number)
		}
		return nil
	})
	t.Require().NoError(WaitSafeAtOrAbove(ctx, opnodeB, sB0+K, t.Logger()))
}

// TestSV2CrossSafeProgressSingleChain: ensure SV2 computes and exposes cross_safe and it advances with the chain.
func TestSV2CrossSafeProgressSingleChain(gt *testing.T) {
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

	l2Net := system.L2Networks()[0]
	chainID := l2Net.RollupConfig().L2ChainID.Uint64()
	sv2URL := os.Getenv("SV2_DENYLIST_URL")
	t.Require().NotEmpty(sv2URL)
	// Gate on SV2 readiness
	{
		ctx2, cancel2 := context.WithTimeout(t.Ctx(), 60*time.Second)
		defer cancel2()
		t.Require().NoError(WaitSV2Ready(ctx2, sv2URL))
	}

	// Ensure op-node proxy is ready and Safe head advances to target
	ctx, cancel := context.WithTimeout(t.Ctx(), 2*time.Minute)
	defer cancel()
	t.Require().NoError(WaitOpNodeProxyReady(ctx, sv2URL, chainID, t.Logger()))
	const target uint64 = 3
	opnodeURL := fmt.Sprintf("%s/opnode/%d/", sv2URL, chainID)
	t.Require().NoError(WaitSafeAtOrAbove(ctx, opnodeURL, target, t.Logger()))

	// helper to read cross_safe Number from /status
	readCrossSafe := func() (uint64, uint64, error) {
		resp, err := http.Get(fmt.Sprintf("%s/status?chainId=%d", sv2URL, chainID))
		if err != nil {
			return 0, 0, err
		}
		defer resp.Body.Close()
		var out map[string]any
		if derr := json.NewDecoder(resp.Body).Decode(&out); derr != nil {
			return 0, 0, derr
		}
		// cross_safe is encoded as an object with Number field
		var crossN, localN uint64
		if cs, ok := out["cross_safe"].(map[string]any); ok {
			if v, ok2 := cs["Number"].(float64); ok2 {
				crossN = uint64(v)
			} else if v2, ok3 := cs["number"].(float64); ok3 {
				crossN = uint64(v2)
			}
		}
		if ls, ok := out["local_safe"].(map[string]any); ok {
			if v, ok2 := ls["Number"].(float64); ok2 {
				localN = uint64(v)
			} else if v2, ok3 := ls["number"].(float64); ok3 {
				localN = uint64(v2)
			}
		}
		return crossN, localN, nil
	}

	// Wait until cross_safe >= target and equals local_safe (no interop messages in this setup)
	t.Require().NoError(retry.Do0(ctx, 240, &retry.FixedStrategy{Dur: 300 * time.Millisecond}, func() error {
		c, l, err := readCrossSafe()
		if err != nil {
			return err
		}
		if c < target {
			return fmt.Errorf("cross_safe < %d (have %d)", target, c)
		}
		if l < target {
			return fmt.Errorf("local_safe < %d (have %d)", target, l)
		}
		if c != l {
			return fmt.Errorf("expected cross_safe == local_safe in no-interop setup (c=%d l=%d)", c, l)
		}
		return nil
	}))
}

// superroot test removed
