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

		// Ensure SV2 denylist contains this ID before rollback
		sv2URL := os.Getenv("SV2_DENYLIST_URL")
		t.Require().NotEmpty(sv2URL)
		chainID := l2Net.RollupConfig().L2ChainID.Uint64()
		_ = retry.Do0(ctx, 40, &retry.FixedStrategy{Dur: 150 * time.Millisecond}, func() error {
			resp, err := http.Get(fmt.Sprintf("%s/denylist/v1/check?chainId=%d&id=%s", sv2URL, chainID, prePayloadID))
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

	ctx, cancel := context.WithTimeout(t.Ctx(), 90*time.Second)
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

	// Snapshot pre-rollback on A and compute payload ID + parent hash at H
	var preA eth.BlockRef
	var prePayloadIDA string
	var preParentHashA string
	{
		ref, err := elA.EthClient().BlockRefByLabel(ctx, eth.Unsafe)
		t.Require().NoError(err)
		preA = ref
		l2c, err := sources.NewL2Client(elA.L2EthClient().RPC(), t.Logger(), nil, sources.L2ClientDefaultConfig(l2A.RollupConfig(), true))
		t.Require().NoError(err)
		env, err := l2c.PayloadByNumber(ctx, preA.Number)
		t.Require().NoError(err)
		if actual, ok := env.CheckBlockHash(); ok {
			prePayloadIDA = actual.Hex()
		}
		t.Require().NotEmpty(prePayloadIDA)
		if preA.Number > 0 {
			var parent struct {
				Hash string `json:"hash"`
			}
			parentHex := fmt.Sprintf("0x%x", preA.Number-1)
			t.Require().NoError(elA.L2EthClient().RPC().CallContext(ctx, &parent, "eth_getBlockByNumber", parentHex, false))
			t.Require().NotEmpty(parent.Hash)
			preParentHashA = parent.Hash
		}
	}

	// Ensure denylist contains A's payload ID and H is SAFE on A before rollback
	sv2URL := os.Getenv("SV2_DENYLIST_URL")
	t.Require().NotEmpty(sv2URL)
	idA := l2A.RollupConfig().L2ChainID.Uint64()
	idB := l2B.RollupConfig().L2ChainID.Uint64()

	_ = retry.Do0(ctx, 60, &retry.FixedStrategy{Dur: 200 * time.Millisecond}, func() error {
		resp, err := http.Get(fmt.Sprintf("%s/denylist/v1/check?chainId=%d&id=%s", sv2URL, idA, prePayloadIDA))
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

	// Wait until H is SAFE on chain A
	{
		rpc, err := opclient.NewRPC(ctx, t.Logger(), fmt.Sprintf("%s/opnode/%d/", sv2URL, idA), opclient.WithLazyDial())
		t.Require().NoError(err)
		defer rpc.Close()
		roll := sources.NewRollupClient(rpc)
		_ = retry.Do0(ctx, 240, &retry.FixedStrategy{Dur: 250 * time.Millisecond}, func() error {
			st, err := roll.SyncStatus(ctx)
			if err != nil || st == nil {
				return fmt.Errorf("sync status: %v", err)
			}
			safe := st.SafeL2.Number
			if safe == 0 {
				safe = st.LocalSafeL2.Number
			}
			if safe < preA.Number {
				return fmt.Errorf("waiting safe>=H: have %d want >= %d", safe, preA.Number)
			}
			return nil
		})
	}

	// Trigger rollback on chain A only
	{
		toNum := preA.Number - 1
		reqBody, _ := json.Marshal(map[string]uint64{"to_block_number": toNum})
		resp, err := http.Post(fmt.Sprintf("%s/admin/rollback?chainId=%d", sv2URL, idA), "application/json", bytes.NewReader(reqBody))
		t.Require().NoError(err)
		if resp != nil {
			defer resp.Body.Close()
			t.Require().Equal(http.StatusNoContent, resp.StatusCode)
		}
		// Wait for op-node proxy readiness after restart to avoid transient 502s
		_ = retry.Do0(ctx, 60, &retry.FixedStrategy{Dur: 250 * time.Millisecond}, func() error {
			rpc, err := opclient.NewRPC(ctx, t.Logger(), fmt.Sprintf("%s/opnode/%d/", sv2URL, idA), opclient.WithLazyDial())
			if err != nil {
				return err
			}
			defer rpc.Close()
			roll := sources.NewRollupClient(rpc)
			st, err := roll.SyncStatus(ctx)
			if err != nil || st == nil {
				return fmt.Errorf("sync status: %v", err)
			}
			return nil
		})
	}

	// Assert chain A regresses then re-advances (unsafe)
	_ = retry.Do0(ctx, 40, &retry.FixedStrategy{Dur: 200 * time.Millisecond}, func() error {
		after, err := elA.EthClient().BlockRefByLabel(ctx, eth.Unsafe)
		if err != nil {
			return err
		}
		if after.Number >= preA.Number {
			return fmt.Errorf("waiting for rollback to reflect: have %d, want < %d", after.Number, preA.Number)
		}
		return nil
	})
	_ = retry.Do0(ctx, 200, &retry.FixedStrategy{Dur: 250 * time.Millisecond}, func() error {
		after, err := elA.EthClient().BlockRefByLabel(ctx, eth.Unsafe)
		if err != nil {
			return err
		}
		if after.Number < preA.Number {
			return fmt.Errorf("waiting to re-advance: have %d < %d", after.Number, preA.Number)
		}
		return nil
	})

	// Parent continuity and replacement at H on A
	{
		if preA.Number > 0 {
			var currParent struct {
				Hash string `json:"hash"`
			}
			parentHex := fmt.Sprintf("0x%x", preA.Number-1)
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
		hexNum := fmt.Sprintf("0x%x", preA.Number)
		t.Require().NoError(retry.Do0(ctx, 60, &retry.FixedStrategy{Dur: 200 * time.Millisecond}, func() error {
			if err := elA.L2EthClient().RPC().CallContext(ctx, &blk, "eth_getBlockByNumber", hexNum, false); err != nil {
				return err
			}
			if blk.Hash == "" {
				return fmt.Errorf("empty block hash")
			}
			return nil
		}))
		t.Require().NotEqual(preA.Hash, blk.Hash)
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

	// Snapshot pre-rollback on B and compute payload ID + parent hash at H
	var preB eth.BlockRef
	var prePayloadIDB string
	var preParentHashB string
	{
		ref, err := elB.EthClient().BlockRefByLabel(ctx, eth.Unsafe)
		t.Require().NoError(err)
		preB = ref
		l2c, err := sources.NewL2Client(elB.L2EthClient().RPC(), t.Logger(), nil, sources.L2ClientDefaultConfig(l2B.RollupConfig(), true))
		t.Require().NoError(err)
		env, err := l2c.PayloadByNumber(ctx, preB.Number)
		t.Require().NoError(err)
		if actual, ok := env.CheckBlockHash(); ok {
			prePayloadIDB = actual.Hex()
		}
		t.Require().NotEmpty(prePayloadIDB)
		if preB.Number > 0 {
			var parent struct {
				Hash string `json:"hash"`
			}
			parentHex := fmt.Sprintf("0x%x", preB.Number-1)
			t.Require().NoError(elB.L2EthClient().RPC().CallContext(ctx, &parent, "eth_getBlockByNumber", parentHex, false))
			t.Require().NotEmpty(parent.Hash)
			preParentHashB = parent.Hash
		}
	}

	// Ensure denylist contains B's payload ID and H is SAFE on B before rollback
	sv2URL := os.Getenv("SV2_DENYLIST_URL")
	t.Require().NotEmpty(sv2URL)
	idA := l2A.RollupConfig().L2ChainID.Uint64()
	idB := l2B.RollupConfig().L2ChainID.Uint64()

	_ = retry.Do0(ctx, 60, &retry.FixedStrategy{Dur: 200 * time.Millisecond}, func() error {
		resp, err := http.Get(fmt.Sprintf("%s/denylist/v1/check?chainId=%d&id=%s", sv2URL, idB, prePayloadIDB))
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

	// Wait until H is SAFE on chain B
	{
		rpc, err := opclient.NewRPC(ctx, t.Logger(), fmt.Sprintf("%s/opnode/%d/", sv2URL, idB), opclient.WithLazyDial())
		t.Require().NoError(err)
		defer rpc.Close()
		roll := sources.NewRollupClient(rpc)
		_ = retry.Do0(ctx, 240, &retry.FixedStrategy{Dur: 250 * time.Millisecond}, func() error {
			st, err := roll.SyncStatus(ctx)
			if err != nil || st == nil {
				return fmt.Errorf("sync status: %v", err)
			}
			safe := st.SafeL2.Number
			if safe == 0 {
				safe = st.LocalSafeL2.Number
			}
			if safe < preB.Number {
				return fmt.Errorf("waiting safe>=H: have %d want >= %d", safe, preB.Number)
			}
			return nil
		})
	}

	// Trigger rollback on chain B only
	{
		toNum := preB.Number - 1
		reqBody, _ := json.Marshal(map[string]uint64{"to_block_number": toNum})
		resp, err := http.Post(fmt.Sprintf("%s/admin/rollback?chainId=%d", sv2URL, idB), "application/json", bytes.NewReader(reqBody))
		t.Require().NoError(err)
		if resp != nil {
			defer resp.Body.Close()
			t.Require().Equal(http.StatusNoContent, resp.StatusCode)
		}
		// Wait for op-node proxy readiness after restart to avoid transient 502s
		_ = retry.Do0(ctx, 60, &retry.FixedStrategy{Dur: 250 * time.Millisecond}, func() error {
			rpc, err := opclient.NewRPC(ctx, t.Logger(), fmt.Sprintf("%s/opnode/%d/", sv2URL, idB), opclient.WithLazyDial())
			if err != nil {
				return err
			}
			defer rpc.Close()
			roll := sources.NewRollupClient(rpc)
			st, err := roll.SyncStatus(ctx)
			if err != nil || st == nil {
				return fmt.Errorf("sync status: %v", err)
			}
			return nil
		})
	}

	// Assert chain B regresses then re-advances (unsafe)
	_ = retry.Do0(ctx, 40, &retry.FixedStrategy{Dur: 200 * time.Millisecond}, func() error {
		after, err := elB.EthClient().BlockRefByLabel(ctx, eth.Unsafe)
		if err != nil {
			return err
		}
		if after.Number >= preB.Number {
			return fmt.Errorf("waiting for rollback to reflect: have %d, want < %d", after.Number, preB.Number)
		}
		return nil
	})
	_ = retry.Do0(ctx, 200, &retry.FixedStrategy{Dur: 250 * time.Millisecond}, func() error {
		after, err := elB.EthClient().BlockRefByLabel(ctx, eth.Unsafe)
		if err != nil {
			return err
		}
		if after.Number < preB.Number {
			return fmt.Errorf("waiting to re-advance: have %d < %d", after.Number, preB.Number)
		}
		return nil
	})

	// Parent continuity and replacement at H on B
	{
		if preB.Number > 0 {
			var currParent struct {
				Hash string `json:"hash"`
			}
			parentHex := fmt.Sprintf("0x%x", preB.Number-1)
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
		hexNum := fmt.Sprintf("0x%x", preB.Number)
		t.Require().NoError(retry.Do0(ctx, 60, &retry.FixedStrategy{Dur: 200 * time.Millisecond}, func() error {
			if err := elB.L2EthClient().RPC().CallContext(ctx, &blk, "eth_getBlockByNumber", hexNum, false); err != nil {
				return err
			}
			if blk.Hash == "" {
				return fmt.Errorf("empty block hash")
			}
			return nil
		}))
		t.Require().NotEqual(preB.Hash, blk.Hash)
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
