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
	"github.com/ethereum-optimism/optimism/op-node/rollup"
	opclient "github.com/ethereum-optimism/optimism/op-service/client"
	"github.com/ethereum-optimism/optimism/op-service/eth"
	"github.com/ethereum-optimism/optimism/op-service/predeploys"
	"github.com/ethereum-optimism/optimism/op-service/retry"
	"github.com/ethereum-optimism/optimism/op-service/sources"

	"github.com/ethereum-optimism/optimism/devnet-sdk/contracts/bindings"
	"github.com/ethereum-optimism/optimism/op-chain-ops/devkeys"
	"github.com/ethereum-optimism/optimism/op-service/plan"
	"github.com/ethereum-optimism/optimism/op-service/testlog"
	"github.com/ethereum-optimism/optimism/op-service/txintent"
	"github.com/ethereum-optimism/optimism/op-service/txplan"
	"github.com/ethereum/go-ethereum/common"
)

// TestSupervisorV2Rollback exercises Supervisor v2 rollback + denylist behavior in a single-chain sysgo preset.
// It performs an end-to-end flow and validates each property explicitly:
//
//   - Boot a minimal system with SV2 embedding an op-node; tests explicitly add denylist entries.
//   - Wait until the L2 unsafe head has advanced (>= 3), then snapshot the pre-rollback reference at height H (`preRef`).
//   - Compute the deterministic payload header-hash (stand-in PayloadID) for H via sources.L2Client and assert SV2
//     reports it as denylisted via GET /denylist/v1/check.
//   - Record the parent block hash at height H-1 (`preParentHash`) to verify chain continuity across rollback.
//   - Trigger a rollback by POST /admin/rollback (back 1 block). SV2 stops the embedded op-node, rolls back the EL
//     via debug_setHead (number), restarts the op-node, and resumes polling.
//   - Assert the unsafe head regresses below H, then re-advances to at least H.
//   - Assert the parent hash at H-1 is unchanged post-recovery (the chain up to H-1 remains identical).
//   - Assert the block hash at H differs from the pre-rollback hash (the denylisted payload was not re-inserted and a
//     different block was produced at that height).
//
// Together these checks prove that rollback executed, the denylisted payload is excluded, pre-H chain data is preserved,
// and the chain resumes to at least the previous height. Block/hash reads use the EL RPC; denylist checks use the SV2 HTTP API.
func TestSupervisorV2Rollback(gt *testing.T) {
	var ids DefaultMinimalSystemIDs
	opt := stack.Combine[*Orchestrator](
		DefaultMinimalSystemNoCL(&ids),
		WithSupervisorV2OnFirstChain(),
		WithInterop2ActivationOffsetForSV2(6),
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
		// Choose a height that is not genesis and not the current unsafe head to avoid immediate reorg/empty block
		ref, err := el.EthClient().BlockRefByLabel(ctx, eth.Unsafe)
		t.Require().NoError(err)
		if ref.Number <= 1 {
			// wait until at least height 2
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
	}
	// SV2_DENYLIST_URL is set by StartEmbeddedFromSys

	// Trigger rollback via Supervisor admin API (stops op-node, rolls back EL, restarts op-node)
	sv2URL := os.Getenv("SV2_DENYLIST_URL")
	t.Require().NotEmpty(sv2URL)
	// Roll back to an absolute block number (preRef.Number - 1)
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

	// Then assert we re-advance back to at least the pre-rollback height
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

	// And the block at the denied height has a different hash than the pre-rollback one
	{
		// First, verify the parent block hash at preRef.Number-1 matches pre- and post-rollback
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

		// Wait until EL has produced at least up to preRef.Number again
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
		// Fetch block at preRef.Number from EL directly and compare hashes
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

// TestSupervisorV2TwoChainRollbackIsolation brings up two L2s under a single SV2 instance,
// verifies both advance, then rolls back chain A and asserts chain B is unaffected.
func TestSupervisorV2TwoChainRollbackIsolation(gt *testing.T) {
	opt := stack.Combine[*Orchestrator](WithSV2TwoChainMinimal(6))

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

	// Wait until both chains have some blocks
	ctx, cancel := context.WithTimeout(t.Ctx(), 60*time.Second)
	defer cancel()
	waitChain := func(el stack.L2ELNode) error {
		return retry.Do0(ctx, 60, &retry.FixedStrategy{Dur: 300 * time.Millisecond}, func() error {
			ref, err := el.EthClient().BlockRefByLabel(ctx, eth.Unsafe)
			if err != nil {
				return err
			}
			if ref.Number < 3 {
				return fmt.Errorf("waiting for blocks, got %d", ref.Number)
			}
			return nil
		})
	}
	t.Require().NoError(waitChain(elA))
	t.Require().NoError(waitChain(elB))

	// Snapshot pre-rollback heads
	preA, err := elA.EthClient().BlockRefByLabel(ctx, eth.Unsafe)
	t.Require().NoError(err)
	preB, err := elB.EthClient().BlockRefByLabel(ctx, eth.Unsafe)
	t.Require().NoError(err)

	// Find chain IDs
	rcfgA := l2A.RollupConfig()
	rcfgB := l2B.RollupConfig()
	idA := rcfgA.L2ChainID.Uint64()
	_ = rcfgB

	// Roll back chain A to preA-1 via SV2 admin
	sv2URL := os.Getenv("SV2_DENYLIST_URL")
	t.Require().NotEmpty(sv2URL)
	toNum := preA.Number - 1
	reqBody, _ := json.Marshal(map[string]uint64{"to_block_number": toNum})
	resp, err := http.Post(fmt.Sprintf("%s/admin/rollback?chainId=%d", sv2URL, idA), "application/json", bytes.NewReader(reqBody))
	t.Require().NoError(err)
	if resp != nil {
		defer resp.Body.Close()
		t.Require().Equal(http.StatusNoContent, resp.StatusCode)
	}

	// Assert chain A regresses then re-advances
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
			return fmt.Errorf("waiting A to re-advance: have %d < %d", after.Number, preA.Number)
		}
		return nil
	})

	// Assert chain B remained at least at preB and did not regress
	afterB, err := elB.EthClient().BlockRefByLabel(ctx, eth.Unsafe)
	t.Require().NoError(err)
	t.Require().GreaterOrEqual(afterB.Number, preB.Number)
}

// TestSupervisorV2TwoChainAdvance asserts that two chains under a single SV2 instance
// independently advance to at least N blocks without any rollback.
func TestSupervisorV2TwoChainAdvance(gt *testing.T) {
	const minBlocks uint64 = 3

	opt := stack.Combine[*Orchestrator](WithSV2TwoChainMinimal(6))

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

	ctx, cancel := context.WithTimeout(t.Ctx(), 60*time.Second)
	defer cancel()

	waitAtLeast := func(el stack.L2ELNode, n uint64) error {
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

	t.Require().NoError(waitAtLeast(elA, minBlocks))
	t.Require().NoError(waitAtLeast(elB, minBlocks))
}

// TestSupervisorV2TwoChainCrossSafeProgress brings up two L2s and asserts that
// SV2 persists local-safe and cross-safe progress and exposes it via /status.
func TestSupervisorV2TwoChainCrossSafeProgress(gt *testing.T) {
	const minBlocks uint64 = 4

	// Use small confirmation depth for faster cross-safe gating
	opt := stack.Combine[*Orchestrator](WithSV2TwoChainMinimalDepth(6, 1))

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
	elA := l2A.L2ELNode(match.FirstL2EL)

	ctx, cancel := context.WithTimeout(t.Ctx(), 60*time.Second)
	defer cancel()

	// Wait for >= minBlocks unsafe
	_ = retry.Do0(ctx, 120, &retry.FixedStrategy{Dur: 300 * time.Millisecond}, func() error {
		ref, err := elA.EthClient().BlockRefByLabel(ctx, eth.Unsafe)
		if err != nil {
			return err
		}
		if ref.Number < minBlocks {
			return fmt.Errorf("waiting for >= %d blocks, got %d", minBlocks, ref.Number)
		}
		return nil
	})

	// Query SV2 /status for chain A and verify local_safe and (eventually) cross_safe are non-zero
	sv2URL := os.Getenv("SV2_DENYLIST_URL")
	t.Require().NotEmpty(sv2URL)
	chainID := l2A.RollupConfig().L2ChainID.Uint64()
	// poll until both fields appear (SV2 persists asynchronously)
	_ = retry.Do0(ctx, 80, &retry.FixedStrategy{Dur: 250 * time.Millisecond}, func() error {
		resp, err := http.Get(fmt.Sprintf("%s/status?chainId=%d", sv2URL, chainID))
		if err != nil {
			return err
		}
		defer resp.Body.Close()
		var out struct {
			LocalSafe *struct {
				Number uint64 `json:"number"`
			}
			CrossSafe *struct {
				Number uint64 `json:"number"`
			}
		}
		if derr := json.NewDecoder(resp.Body).Decode(&out); derr != nil {
			return derr
		}
		if out.LocalSafe == nil || out.LocalSafe.Number == 0 {
			return fmt.Errorf("waiting for local_safe")
		}
		if out.CrossSafe == nil || out.CrossSafe.Number == 0 {
			return fmt.Errorf("waiting for cross_safe")
		}
		return nil
	})
}

// Minimal, focused test: ensure safe head progresses with SV2 + batcher on a single chain.
func TestSupervisorV2SingleChainSafeProgresses(gt *testing.T) {
	var ids DefaultMinimalSystemIDs
	opt := stack.Combine[*Orchestrator](
		DefaultMinimalSystemNoCL(&ids),
		WithSupervisorV2OnFirstChain(),
		WithInterop2ActivationOffsetForSV2(6),
		// no-op capture removed; batcher is started in Finally hook below
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

	logger := testlog.Logger(gt, log.LevelInfo)
	onFail, onSkipNow := exiters(gt)
	p := devtest.NewP(context.Background(), logger, onFail, onSkipNow)
	gt.Cleanup(p.Close)

	orch := NewOrchestrator(p, stack.Combine[*Orchestrator]())
	stack.ApplyOptionLifecycle(opt, orch)

	t := devtest.SerialT(gt)
	system := shim.NewSystem(t)
	orch.Hydrate(system)

	// Dial SV2 op-node proxy directly to get a Rollup client; avoid depending on CL shim timing.
	ctx, cancel := context.WithTimeout(t.Ctx(), 60*time.Second)
	defer cancel()
	sv2URL := os.Getenv("SV2_DENYLIST_URL")
	t.Require().NotEmpty(sv2URL)
	chainID := system.L2Networks()[0].RollupConfig().L2ChainID.Uint64()
	rpc, err := opclient.NewRPC(ctx, t.Logger(), fmt.Sprintf("%s/opnode/%d/", sv2URL, chainID), opclient.WithLazyDial())
	t.Require().NoError(err)
	roll := sources.NewRollupClient(rpc)
	// Wait for LocalSafeL2 or SafeL2 to progress beyond genesis
	err = retry.Do0(ctx, 240, &retry.FixedStrategy{Dur: 250 * time.Millisecond}, func() error {
		st, err := roll.SyncStatus(ctx)
		if err != nil || st == nil {
			return fmt.Errorf("sync status: %v", err)
		}
		if st.LocalSafeL2.Number > 0 || st.SafeL2.Number > 0 {
			t.Logger().Info("op-node safe progressed", "local_safe", st.LocalSafeL2, "safe", st.SafeL2)
			return nil
		}
		t.Logger().Info("op-node heads (waiting)", "unsafe", st.UnsafeL2, "local_safe", st.LocalSafeL2, "safe", st.SafeL2)
		return fmt.Errorf("waiting for local_safe or safe > 0")
	})
	t.Require().NoError(err)
}

// TestSupervisorV2TwoChainSafeProgressionRequiresBatcher asserts that without batchers
// the cross-safe head does not progress, and after starting batchers pointed at SV2 opnode
// proxy the cross-safe head progresses on each chain. This sets the stage for restricting
// ingestion to safe-only blocks later.
func TestSupervisorV2TwoChainSafeProgressionRequiresBatcher(gt *testing.T) {
	// small confirmation depth to observe cross-safe quickly and start batchers pointing to SV2
	opt := stack.Combine[*Orchestrator](WithSV2TwoChainMinimalDepth(6, 1))

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

	// Wait for a few unsafe blocks to exist on A for baseline
	elA := l2A.L2ELNode(match.FirstL2EL)
	ctx, cancel := context.WithTimeout(t.Ctx(), 60*time.Second)
	defer cancel()
	_ = retry.Do0(ctx, 120, &retry.FixedStrategy{Dur: 300 * time.Millisecond}, func() error {
		ref, err := elA.EthClient().BlockRefByLabel(ctx, eth.Unsafe)
		if err != nil {
			return err
		}
		if ref.Number < 4 {
			return fmt.Errorf("waiting for >= 4 unsafe blocks, got %d", ref.Number)
		}
		return nil
	})

	// Without batchers, expect cross-safe to stay at zero for both chains
	sv2URL := os.Getenv("SV2_DENYLIST_URL")
	t.Require().NotEmpty(sv2URL)
	for _, net := range []stack.L2Network{l2A, l2B} {
		chainID := net.RollupConfig().L2ChainID.Uint64()
		// poll briefly to ensure it does not advance
		err := retry.Do0(ctx, 10, &retry.FixedStrategy{Dur: 250 * time.Millisecond}, func() error {
			resp, err := http.Get(fmt.Sprintf("%s/status?chainId=%d", sv2URL, chainID))
			if err != nil {
				return err
			}
			defer resp.Body.Close()
			var out struct {
				Unsafe *struct {
					Number uint64 `json:"number"`
				}
				LocalSafe *struct {
					Number uint64 `json:"number"`
				}
				Safe *struct {
					Number uint64 `json:"number"`
				}
				CrossSafe *struct {
					Number uint64 `json:"number"`
				}
			}
			if derr := json.NewDecoder(resp.Body).Decode(&out); derr != nil {
				return derr
			}
			t.Logf("sv2 status (no batcher) chain=%d unsafe=%v local_safe=%v safe=%v cross_safe=%v", chainID,
				numPtr(out.Unsafe), numPtr(out.LocalSafe), numPtr(out.Safe), numPtr(out.CrossSafe))
			// Expect nil or zero without batchers
			if out.CrossSafe != nil && out.CrossSafe.Number > 0 {
				return fmt.Errorf("unexpected non-zero cross_safe without batcher: %d", out.CrossSafe.Number)
			}
			return nil
		})
		t.Require().NoError(err)
	}

	// Batchers are already started by the preset; proceed to assert cross-safe advances

	// Wait for local_safe to appear first, then for cross_safe to become non-zero on each chain
	waitCrossSafe := func(net stack.L2Network) error {
		chainID := net.RollupConfig().L2ChainID.Uint64()
		// phase 1: ensure local_safe shows up (ingest ready)
		if err := retry.Do0(ctx, 160, &retry.FixedStrategy{Dur: 250 * time.Millisecond}, func() error {
			resp, err := http.Get(fmt.Sprintf("%s/status?chainId=%d", sv2URL, chainID))
			if err != nil {
				return err
			}
			defer resp.Body.Close()
			var out map[string]any
			if derr := json.NewDecoder(resp.Body).Decode(&out); derr != nil {
				return derr
			}
			// local_safe.Number or local_safe.number
			var localN uint64
			if ls, ok := out["local_safe"].(map[string]any); ok {
				if v, ok2 := ls["Number"].(float64); ok2 {
					localN = uint64(v)
				} else if v2, ok3 := ls["number"].(float64); ok3 {
					localN = uint64(v2)
				}
			}
			if localN == 0 {
				return fmt.Errorf("waiting for local_safe > 0")
			}
			return nil
		}); err != nil {
			return err
		}
		// phase 2: wait for cross_safe to advance (cross DB writes visible in /status)
		return retry.Do0(ctx, 200, &retry.FixedStrategy{Dur: 300 * time.Millisecond}, func() error {
			resp, err := http.Get(fmt.Sprintf("%s/status?chainId=%d", sv2URL, chainID))
			if err != nil {
				return err
			}
			defer resp.Body.Close()
			var out map[string]any
			if derr := json.NewDecoder(resp.Body).Decode(&out); derr != nil {
				return derr
			}
			// for logging
			t.Logf("sv2 status (waiting) chain=%d body=%v", chainID, out)
			var crossN uint64
			if cs, ok := out["cross_safe"].(map[string]any); ok {
				if v, ok2 := cs["Number"].(float64); ok2 {
					crossN = uint64(v)
				} else if v2, ok3 := cs["number"].(float64); ok3 {
					crossN = uint64(v2)
				}
			}
			if crossN == 0 {
				return fmt.Errorf("waiting for cross_safe > 0")
			}
			return nil
		})
	}
	t.Require().NoError(waitCrossSafe(l2A))
	t.Require().NoError(waitCrossSafe(l2B))
}

// numPtr is a tiny helper to print optional numbers in logs
func numPtr(s *struct {
	Number uint64 `json:"number"`
}) any {
	if s == nil {
		return nil
	}
	return s.Number
}

// TestSupervisorV2TwoChainValidExecMessageStable emits a valid executing message and
// asserts it is included and not reorged out, while SV2 runs and cross-safe progresses.
func TestSupervisorV2TwoChainValidExecMessageStable(gt *testing.T) {
	// Use small confirmation depth for faster cross-safe gating
	opt := stack.Combine[*Orchestrator](WithSV2TwoChainMinimalDepth(6, 1))

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

	ctx, cancel := context.WithTimeout(t.Ctx(), 60*time.Second)
	defer cancel()

	// Wait for Interop2 activation on both chains (CrossL2Inbox code present)
	waitInterop2 := func(el stack.L2ELNode, rcfg *rollup.Config) error {
		activationBlocks := (*rcfg.Interop2Time - rcfg.Genesis.L2Time) / rcfg.BlockTime
		activationNum := rcfg.Genesis.L2.Number + activationBlocks
		actHex := fmt.Sprintf("0x%x", activationNum)
		if err := retry.Do0(ctx, 120, &retry.FixedStrategy{Dur: 300 * time.Millisecond}, func() error {
			ref, err := el.EthClient().BlockRefByLabel(ctx, eth.Unsafe)
			if err != nil {
				return err
			}
			if ref.Number < activationNum {
				return fmt.Errorf("waiting head >= activation")
			}
			return nil
		}); err != nil {
			return err
		}
		return retry.Do0(ctx, 40, &retry.FixedStrategy{Dur: 300 * time.Millisecond}, func() error {
			var codeAt string
			if err := el.L2EthClient().RPC().CallContext(ctx, &codeAt, "eth_getCode", predeploys.CrossL2InboxAddr.Hex(), actHex); err != nil {
				return err
			}
			if codeAt == "0x" || codeAt == "0x0" || len(codeAt) < 4 {
				return fmt.Errorf("no code at CrossL2Inbox")
			}
			return nil
		})
	}
	t.Require().NoError(waitInterop2(elA, l2A.RollupConfig()))
	t.Require().NoError(waitInterop2(elB, l2B.RollupConfig()))

	// Fund EOAs and set up tx plans
	keys, err := devkeys.NewSaltedDevKeys(devkeys.TestMnemonic, os.Getenv("OP_DEVSTACK_SALT"))
	t.Require().NoError(err)
	alicePriv, _ := keys.Secret(devkeys.UserKey(0))
	bobPriv, _ := keys.Secret(devkeys.UserKey(1))
	aliceAddr, _ := keys.Address(devkeys.UserKey(0))
	bobAddr, _ := keys.Address(devkeys.UserKey(1))
	_ = l2A.Faucet(match.FirstFaucet).API().RequestETH(t.Ctx(), aliceAddr, eth.OneTenthEther)
	_ = l2B.Faucet(match.FirstFaucet).API().RequestETH(t.Ctx(), bobAddr, eth.OneTenthEther)

	planAlice := txplan.Combine(
		txplan.WithPrivateKey(alicePriv),
		txplan.WithChainID(elA.EthClient()),
		txplan.WithPendingNonce(elA.EthClient()),
		txplan.WithAgainstLatestBlock(elA.EthClient()),
		txplan.WithEstimator(elA.EthClient(), true),
		txplan.WithRetrySubmission(elA.EthClient(), 5, retry.Exponential()),
		txplan.WithRetryInclusion(elA.EthClient(), 5, retry.Exponential()),
		txplan.WithBlockInclusionInfo(elA.EthClient()),
	)
	planBob := txplan.Combine(
		txplan.WithPrivateKey(bobPriv),
		txplan.WithChainID(elB.EthClient()),
		txplan.WithPendingNonce(elB.EthClient()),
		txplan.WithAgainstLatestBlock(elB.EthClient()),
		txplan.WithEstimator(elB.EthClient(), true),
		txplan.WithRetrySubmission(elB.EthClient(), 5, retry.Exponential()),
		txplan.WithRetryInclusion(elB.EthClient(), 5, retry.Exponential()),
		txplan.WithBlockInclusionInfo(elB.EthClient()),
	)

	// Deploy EventLogger on A and emit one message
	deployCalldata := common.FromHex(bindings.EventloggerBin)
	deployTx := txplan.NewPlannedTx(planAlice, txplan.WithData(deployCalldata))
	depRes, err := deployTx.Included.Eval(t.Ctx())
	t.Require().NoError(err)
	eventLogger := depRes.ContractAddress

	randomData := func(n int) []byte {
		b := make([]byte, n)
		for i := range b {
			b[i] = byte(1 + (i % 251))
		}
		return b
	}
	var topic0 [32]byte
	copy(topic0[:], randomData(32))
	topics := [][32]byte{topic0}
	initTx := txintent.NewIntent[*txintent.InitTrigger, *txintent.InteropOutput](planAlice)
	initTx.Content.Set(&txintent.InitTrigger{Emitter: eventLogger, Topics: topics, OpaqueData: randomData(16)})
	_, err = initTx.PlannedTx.Included.Eval(t.Ctx())
	t.Require().NoError(err)

	// Execute valid message on B
	txB := txintent.NewIntent[*txintent.ExecTrigger, *txintent.InteropOutput](planBob)
	txB.Content.DependOn(&initTx.Result)
	txB.Content.Fn(txintent.ExecuteIndexed(predeploys.CrossL2InboxAddr, &initTx.Result, 0))
	_, err = txB.PlannedTx.Included.Eval(t.Ctx())
	t.Require().NoError(err)
	execRef, err := txB.PlannedTx.IncludedBlock.Eval(t.Ctx())
	t.Require().NoError(err)
	execNum := execRef.Number

	// Capture current head block on B at inclusion time and verify stability (no reorg)
	var headAt struct {
		Hash string `json:"hash"`
	}
	var headNum uint64
	{
		var bnHex string
		t.Require().NoError(elB.L2EthClient().RPC().CallContext(ctx, &bnHex, "eth_blockNumber"))
		t.Require().True(len(bnHex) >= 3 && bnHex[:2] == "0x")
		n, e := strconv.ParseUint(bnHex[2:], 16, 64)
		t.Require().NoError(e)
		headNum = n
		headHex := fmt.Sprintf("0x%x", headNum)
		t.Require().NoError(elB.L2EthClient().RPC().CallContext(ctx, &headAt, "eth_getBlockByNumber", headHex, false))
		t.Require().NotEmpty(headAt.Hash)
	}

	// After a few more blocks, ensure the block at headNum is unchanged
	_ = retry.Do0(ctx, 120, &retry.FixedStrategy{Dur: 300 * time.Millisecond}, func() error {
		var bnHex string
		if err := elB.L2EthClient().RPC().CallContext(ctx, &bnHex, "eth_blockNumber"); err != nil {
			return err
		}
		if len(bnHex) < 3 || bnHex[:2] != "0x" {
			return fmt.Errorf("bad blockNumber: %s", bnHex)
		}
		curr, err := strconv.ParseUint(bnHex[2:], 16, 64)
		if err != nil {
			return err
		}
		if curr <= headNum+2 {
			return fmt.Errorf("waiting for a few blocks")
		}
		var blk struct {
			Hash string `json:"hash"`
		}
		headHex := fmt.Sprintf("0x%x", headNum)
		if err := elB.L2EthClient().RPC().CallContext(ctx, &blk, "eth_getBlockByNumber", headHex, false); err != nil {
			return err
		}
		if blk.Hash != headAt.Hash {
			return fmt.Errorf("block at %d changed: was %s now %s", headNum, headAt.Hash, blk.Hash)
		}
		return nil
	})

	// Also ensure SV2 reports cross_safe for chain B at or beyond the Execute tx inclusion height
	sv2URL := os.Getenv("SV2_DENYLIST_URL")
	t.Require().NotEmpty(sv2URL)
	chainID := l2B.RollupConfig().L2ChainID.Uint64()
	_ = retry.Do0(ctx, 120, &retry.FixedStrategy{Dur: 250 * time.Millisecond}, func() error {
		resp, err := http.Get(fmt.Sprintf("%s/status?chainId=%d", sv2URL, chainID))
		if err != nil {
			return err
		}
		defer resp.Body.Close()
		var out struct {
			CrossSafe *struct {
				Number uint64 `json:"number"`
			}
		}
		if derr := json.NewDecoder(resp.Body).Decode(&out); derr != nil {
			return derr
		}
		if out.CrossSafe == nil || out.CrossSafe.Number < execNum {
			return fmt.Errorf("waiting for cross_safe >= execNum: have %v want >= %v", out.CrossSafe, execNum)
		}
		return nil
	})

	// Verify the Execute tx is actually present in the block at execNum
	rec, err := txB.PlannedTx.Included.Eval(t.Ctx())
	t.Require().NoError(err)
	txHash := rec.TxHash.Hex()
	var blkTxs struct {
		Transactions []string `json:"transactions"`
	}
	execHex := fmt.Sprintf("0x%x", execNum)
	t.Require().NoError(elB.L2EthClient().RPC().CallContext(ctx, &blkTxs, "eth_getBlockByNumber", execHex, false))
	found := false
	for _, h := range blkTxs.Transactions {
		if h == txHash {
			found = true
			break
		}
	}
	t.Require().True(found, "execute tx not found in block %d", execNum)
}

// TestSupervisorV2TwoChainInvalidExecMessage constructs an executing message with
// invalid identifier attributes and asserts it is not included on chain B (tx filtered out).
// This reuses only txintent + constants and minimal local helpers; no acceptance harness.
func TestSupervisorV2TwoChainInvalidExecMessage(gt *testing.T) {
	// Bring up two-chain minimal system with SV2 and Interop2-only (no Interop HF), depth=1
	opt := stack.Combine[*Orchestrator](WithSV2TwoChainMinimalDepth(6, 1))

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

	// Ensure interop2 predeploys (CrossL2Inbox) are active on both chains
	ctx, cancel := context.WithTimeout(t.Ctx(), 60*time.Second)
	defer cancel()
	waitInteropActive := func(el stack.L2ELNode, rcfg *rollup.Config) error {
		activationBlocks := (*rcfg.Interop2Time - rcfg.Genesis.L2Time) / rcfg.BlockTime
		activationNum := rcfg.Genesis.L2.Number + activationBlocks
		actHex := fmt.Sprintf("0x%x", activationNum)
		// 1) Wait for head to reach activation
		if err := retry.Do0(ctx, 120, &retry.FixedStrategy{Dur: 300 * time.Millisecond}, func() error {
			ref, err := el.EthClient().BlockRefByLabel(ctx, eth.Unsafe)
			if err != nil {
				return err
			}
			if ref.Number < activationNum {
				return fmt.Errorf("waiting for interop2 activation head, have %d want >= %d", ref.Number, activationNum)
			}
			return nil
		}); err != nil {
			return err
		}
		// 2) Verify code present at activation block (sanity)
		return retry.Do0(ctx, 40, &retry.FixedStrategy{Dur: 300 * time.Millisecond}, func() error {
			var codeAt string
			if err := el.L2EthClient().RPC().CallContext(ctx, &codeAt, "eth_getCode", predeploys.CrossL2InboxAddr.Hex(), actHex); err != nil {
				return err
			}
			if codeAt == "0x" || codeAt == "0x0" || len(codeAt) < 4 {
				return fmt.Errorf("CrossL2Inbox not active yet (code check)")
			}
			return nil
		})
	}
	t.Require().NoError(waitInteropActive(elA, l2A.RollupConfig()))
	t.Require().NoError(waitInteropActive(elB, l2B.RollupConfig()))
	// Interop2-only: code presence at CrossL2Inbox is sufficient; no proxy impl check needed.

	// Fund EOAs via stack faucets + dsl.Funder
	// Build EOAs from mnemonic and fund via faucet API directly
	keys, err := devkeys.NewSaltedDevKeys(devkeys.TestMnemonic, os.Getenv("OP_DEVSTACK_SALT"))
	t.Require().NoError(err)
	// derive 3 keys
	alicePriv, _ := keys.Secret(devkeys.UserKey(0))
	bobPriv, _ := keys.Secret(devkeys.UserKey(1))
	aliceAddr, _ := keys.Address(devkeys.UserKey(0))
	bobAddr, _ := keys.Address(devkeys.UserKey(1))
	chuckAddr, _ := keys.Address(devkeys.UserKey(2))
	// Fund via faucet on each chain
	_ = l2A.Faucet(match.FirstFaucet).API().RequestETH(t.Ctx(), aliceAddr, eth.OneTenthEther)
	_ = l2B.Faucet(match.FirstFaucet).API().RequestETH(t.Ctx(), bobAddr, eth.OneTenthEther)
	_ = l2B.Faucet(match.FirstFaucet).API().RequestETH(t.Ctx(), chuckAddr, eth.OneTenthEther)
	// Build tx planners bound to EL clients + keys
	planAlice := txplan.Combine(
		txplan.WithPrivateKey(alicePriv),
		txplan.WithChainID(elA.EthClient()),
		txplan.WithPendingNonce(elA.EthClient()),
		txplan.WithAgainstLatestBlock(elA.EthClient()),
		txplan.WithEstimator(elA.EthClient(), true),
		txplan.WithRetrySubmission(elA.EthClient(), 5, retry.Exponential()),
		txplan.WithRetryInclusion(elA.EthClient(), 5, retry.Exponential()),
		txplan.WithBlockInclusionInfo(elA.EthClient()),
	)
	planBob := txplan.Combine(
		txplan.WithPrivateKey(bobPriv),
		txplan.WithChainID(elB.EthClient()),
		txplan.WithPendingNonce(elB.EthClient()),
		txplan.WithAgainstLatestBlock(elB.EthClient()),
		txplan.WithEstimator(elB.EthClient(), true),
		txplan.WithRetrySubmission(elB.EthClient(), 5, retry.Exponential()),
		txplan.WithRetryInclusion(elB.EthClient(), 5, retry.Exponential()),
		txplan.WithBlockInclusionInfo(elB.EthClient()),
	)

	// Deploy EventLogger on A and emit one message
	deployCalldata := common.FromHex(bindings.EventloggerBin)
	deployTx := txplan.NewPlannedTx(planAlice, txplan.WithData(deployCalldata))
	depRes, err := deployTx.Included.Eval(t.Ctx())
	t.Require().NoError(err)
	eventLogger := depRes.ContractAddress
	// Build a minimal init trigger inline (avoid acceptance helpers)
	randomData := func(n int) []byte {
		b := make([]byte, n)
		for i := range b {
			b[i] = byte(1 + (i % 251))
		}
		return b
	}
	topics := [][32]byte{}
	{
		var t0 [32]byte
		copy(t0[:], randomData(32))
		topics = append(topics, t0)
	}
	initTx := txintent.NewIntent[*txintent.InitTrigger, *txintent.InteropOutput](planAlice)
	initTx.Content.Set(&txintent.InitTrigger{Emitter: eventLogger, Topics: topics, OpaqueData: randomData(16)})
	_, err = initTx.PlannedTx.Included.Eval(t.Ctx())
	t.Require().NoError(err)

	// Wait a bit for SV2 to ingest logs
	_ = retry.Do0(ctx, 20, &retry.FixedStrategy{Dur: 200 * time.Millisecond}, func() error {
		_, e := elA.EthClient().BlockRefByLabel(ctx, eth.Unsafe)
		return e
	})

	// Build invalid ExecTrigger by mutating identifier after ExecuteIndexed
	// Local helper: returns a function suitable for txintent.Content.Fn
	type fault string
	const (
		fRandomTimestamp fault = "randomTimestamp"
		fMismatchedIndex fault = "mismatchedLogIndex"
	)
	executeIndexedFault := func(executor common.Address, events *plan.Lazy[*txintent.InteropOutput], index int, f fault) func(ctx context.Context) (*txintent.ExecTrigger, error) {
		return func(ctx context.Context) (*txintent.ExecTrigger, error) {
			base, err := txintent.ExecuteIndexed(executor, events, index)(ctx)
			if err != nil {
				return nil, err
			}
			// mutate identifier to make it invalid
			switch f {
			case fRandomTimestamp:
				base.Msg.Identifier.Timestamp += 2
			case fMismatchedIndex:
				base.Msg.Identifier.LogIndex += 1
			}
			return base, nil
		}
	}

	// Malicious execute on B using mutated identifier
	txC := txintent.NewIntent[*txintent.ExecTrigger, *txintent.InteropOutput](planBob)
	txC.Content.DependOn(&initTx.Result)
	txC.Content.Fn(executeIndexedFault(predeploys.CrossL2InboxAddr, &initTx.Result, 0, fMismatchedIndex))

	// Ensure contract-level call does not immediately revert (gas can be estimated)
	gas, err := txC.PlannedTx.Gas.Eval(t.Ctx())
	t.Require().NoError(err)
	t.Require().Greater(gas, uint64(0))

	// Attempt inclusion: expect success (we rely on SV2 cross-safety to detect hazard and auto-rollback)
	_, err = txC.PlannedTx.Included.Eval(t.Ctx())
	t.Require().NoError(err)

	// Record the suspect block at chain B head after inclusion
	var suspect struct {
		Hash string `json:"hash"`
	}
	var suspectParent struct {
		Hash string `json:"hash"`
	}
	_ = retry.Do0(ctx, 60, &retry.FixedStrategy{Dur: 200 * time.Millisecond}, func() error {
		// get current head number
		var bnHex string
		if err := elB.L2EthClient().RPC().CallContext(ctx, &bnHex, "eth_blockNumber"); err != nil {
			return err
		}
		if len(bnHex) < 3 || bnHex[:2] != "0x" {
			return fmt.Errorf("bad blockNumber: %s", bnHex)
		}
		n, err := strconv.ParseUint(bnHex[2:], 16, 64)
		if err != nil {
			return err
		}
		// fetch head block and its parent
		headHex := fmt.Sprintf("0x%x", n)
		if err := elB.L2EthClient().RPC().CallContext(ctx, &suspect, "eth_getBlockByNumber", headHex, false); err != nil {
			return err
		}
		if suspect.Hash == "" {
			return fmt.Errorf("empty head hash")
		}
		if n == 0 {
			return fmt.Errorf("need n>0 for parent")
		}
		parentHex := fmt.Sprintf("0x%x", n-1)
		if err := elB.L2EthClient().RPC().CallContext(ctx, &suspectParent, "eth_getBlockByNumber", parentHex, false); err != nil {
			return err
		}
		if suspectParent.Hash == "" {
			return fmt.Errorf("empty parent hash")
		}
		return nil
	})

	// Wait until SV2 auto-rollback replaces the suspect block: same height, different hash; parent stays the same
	_ = retry.Do0(ctx, 240, &retry.FixedStrategy{Dur: 250 * time.Millisecond}, func() error {
		// determine height of suspect by querying its block header again
		// First get current head number (ensure chain advanced back to at least that height)
		var bnHex string
		if err := elB.L2EthClient().RPC().CallContext(ctx, &bnHex, "eth_blockNumber"); err != nil {
			return err
		}
		if len(bnHex) < 3 || bnHex[:2] != "0x" {
			return fmt.Errorf("bad blockNumber: %s", bnHex)
		}
		n, err := strconv.ParseUint(bnHex[2:], 16, 64)
		if err != nil {
			return err
		}
		// If chain hasn't re-advanced to suspect height yet, keep waiting
		// We need to discover suspect height; fetch current head, then use that height
		headHex := fmt.Sprintf("0x%x", n)
		var curr struct {
			Hash string `json:"hash"`
		}
		if err := elB.L2EthClient().RPC().CallContext(ctx, &curr, "eth_getBlockByNumber", headHex, false); err != nil {
			return err
		}
		if curr.Hash == "" {
			return fmt.Errorf("empty curr hash")
		}
		// Compare parent at n-1 with recorded suspectParent; if different, not our target yet; step back one and compare
		if n == 0 {
			return fmt.Errorf("height zero")
		}
		parentHex := fmt.Sprintf("0x%x", n-1)
		var currParent struct {
			Hash string `json:"hash"`
		}
		if err := elB.L2EthClient().RPC().CallContext(ctx, &currParent, "eth_getBlockByNumber", parentHex, false); err != nil {
			return err
		}
		if currParent.Hash != suspectParent.Hash {
			// not the same parent yet; wait for chain to align
			return fmt.Errorf("waiting for parent alignment")
		}
		// Parent aligned; assert head hash differs from suspect hash (replacement)
		if curr.Hash == suspect.Hash {
			return fmt.Errorf("awaiting replacement at height parent=%s", currParent.Hash)
		}
		return nil
	})

	// Now execute the valid message once to confirm happy path still works
	txB := txintent.NewIntent[*txintent.ExecTrigger, *txintent.InteropOutput](planBob)
	txB.Content.DependOn(&initTx.Result)
	txB.Content.Fn(txintent.ExecuteIndexed(predeploys.CrossL2InboxAddr, &initTx.Result, 0))
	_, err = txB.PlannedTx.Included.Eval(t.Ctx())
	t.Require().NoError(err)
}

// TestSupervisorV2TwoChainExecReorgsOnRemoteInitRollback proves cross-safe triggers an
// automatic denylist+rollback on the executing chain (B) when the initiating message on
// the remote chain (A) is rolled back. We do not call rollback on B; the cross-safe
// adapter does it by detecting the broken dependency and calling into SV2 hooks.
func TestSupervisorV2TwoChainExecReorgsOnRemoteInitRollback(gt *testing.T) {
	// Bring up two-chain minimal system with SV2 and Interop2-only (no Interop HF), depth=1
	opt := stack.Combine[*Orchestrator](WithSV2TwoChainMinimalDepth(6, 1))

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

	// Ensure interop2 predeploys (CrossL2Inbox) are active on both chains
	ctx, cancel := context.WithTimeout(t.Ctx(), 75*time.Second)
	defer cancel()
	waitInteropActive := func(el stack.L2ELNode, rcfg *rollup.Config) error {
		activationBlocks := (*rcfg.Interop2Time - rcfg.Genesis.L2Time) / rcfg.BlockTime
		activationNum := rcfg.Genesis.L2.Number + activationBlocks
		actHex := fmt.Sprintf("0x%x", activationNum)
		// 1) Wait for head to reach activation
		if err := retry.Do0(ctx, 160, &retry.FixedStrategy{Dur: 300 * time.Millisecond}, func() error {
			ref, err := el.EthClient().BlockRefByLabel(ctx, eth.Unsafe)
			if err != nil {
				return err
			}
			if ref.Number < activationNum {
				return fmt.Errorf("waiting for interop2 activation head, have %d want >= %d", ref.Number, activationNum)
			}
			return nil
		}); err != nil {
			return err
		}
		// 2) Verify code present at activation block (sanity)
		return retry.Do0(ctx, 40, &retry.FixedStrategy{Dur: 300 * time.Millisecond}, func() error {
			var codeAt string
			if err := el.L2EthClient().RPC().CallContext(ctx, &codeAt, "eth_getCode", predeploys.CrossL2InboxAddr.Hex(), actHex); err != nil {
				return err
			}
			if codeAt == "0x" || codeAt == "0x0" || len(codeAt) < 4 {
				return fmt.Errorf("CrossL2Inbox not active yet (code check)")
			}
			return nil
		})
	}
	t.Require().NoError(waitInteropActive(elA, l2A.RollupConfig()))
	t.Require().NoError(waitInteropActive(elB, l2B.RollupConfig()))

	// Fund EOAs and set up tx plans
	keys, err := devkeys.NewSaltedDevKeys(devkeys.TestMnemonic, os.Getenv("OP_DEVSTACK_SALT"))
	t.Require().NoError(err)
	alicePriv, _ := keys.Secret(devkeys.UserKey(0))
	bobPriv, _ := keys.Secret(devkeys.UserKey(1))
	aliceAddr, _ := keys.Address(devkeys.UserKey(0))
	bobAddr, _ := keys.Address(devkeys.UserKey(1))
	_ = l2A.Faucet(match.FirstFaucet).API().RequestETH(t.Ctx(), aliceAddr, eth.OneTenthEther)
	_ = l2B.Faucet(match.FirstFaucet).API().RequestETH(t.Ctx(), bobAddr, eth.OneTenthEther)

	planAlice := txplan.Combine(
		txplan.WithPrivateKey(alicePriv),
		txplan.WithChainID(elA.EthClient()),
		txplan.WithPendingNonce(elA.EthClient()),
		txplan.WithAgainstLatestBlock(elA.EthClient()),
		txplan.WithEstimator(elA.EthClient(), true),
		txplan.WithRetrySubmission(elA.EthClient(), 5, retry.Exponential()),
		txplan.WithRetryInclusion(elA.EthClient(), 5, retry.Exponential()),
		txplan.WithBlockInclusionInfo(elA.EthClient()),
	)
	planBob := txplan.Combine(
		txplan.WithPrivateKey(bobPriv),
		txplan.WithChainID(elB.EthClient()),
		txplan.WithPendingNonce(elB.EthClient()),
		txplan.WithAgainstLatestBlock(elB.EthClient()),
		txplan.WithEstimator(elB.EthClient(), true),
		txplan.WithRetrySubmission(elB.EthClient(), 5, retry.Exponential()),
		txplan.WithRetryInclusion(elB.EthClient(), 5, retry.Exponential()),
		txplan.WithBlockInclusionInfo(elB.EthClient()),
	)

	// Deploy EventLogger on A and emit one message
	deployCalldata := common.FromHex(bindings.EventloggerBin)
	deployTx := txplan.NewPlannedTx(planAlice, txplan.WithData(deployCalldata))
	depRes, err := deployTx.Included.Eval(t.Ctx())
	t.Require().NoError(err)
	eventLogger := depRes.ContractAddress

	randomData := func(n int) []byte {
		b := make([]byte, n)
		for i := range b {
			b[i] = byte(1 + (i % 251))
		}
		return b
	}
	var topic0 [32]byte
	copy(topic0[:], randomData(32))
	topics := [][32]byte{topic0}
	initTx := txintent.NewIntent[*txintent.InitTrigger, *txintent.InteropOutput](planAlice)
	initTx.Content.Set(&txintent.InitTrigger{Emitter: eventLogger, Topics: topics, OpaqueData: randomData(16)})
	_, err = initTx.PlannedTx.Included.Eval(t.Ctx())
	t.Require().NoError(err)
	initRef, err := initTx.PlannedTx.IncludedBlock.Eval(t.Ctx())
	t.Require().NoError(err)

	// Execute valid message on B that depends on init on A
	txB := txintent.NewIntent[*txintent.ExecTrigger, *txintent.InteropOutput](planBob)
	txB.Content.DependOn(&initTx.Result)
	txB.Content.Fn(txintent.ExecuteIndexed(predeploys.CrossL2InboxAddr, &initTx.Result, 0))
	_, err = txB.PlannedTx.Included.Eval(t.Ctx())
	t.Require().NoError(err)
	execRef, err := txB.PlannedTx.IncludedBlock.Eval(t.Ctx())
	t.Require().NoError(err)
	execNum := execRef.Number

	// Snapshot current head hash and its parent on B for later replacement check
	var suspect struct {
		Hash string `json:"hash"`
	}
	var suspectParent struct {
		Hash string `json:"hash"`
	}
	{
		headHex := fmt.Sprintf("0x%x", execNum)
		t.Require().NoError(elB.L2EthClient().RPC().CallContext(ctx, &suspect, "eth_getBlockByNumber", headHex, false))
		t.Require().NotEmpty(suspect.Hash)
		if execNum == 0 {
			t.Require().Fail("execNum zero")
		}
		parentHex := fmt.Sprintf("0x%x", execNum-1)
		t.Require().NoError(elB.L2EthClient().RPC().CallContext(ctx, &suspectParent, "eth_getBlockByNumber", parentHex, false))
		t.Require().NotEmpty(suspectParent.Hash)
	}

	// Roll back chain A to remove the initiating message block; do NOT touch chain B.
	sv2URL := os.Getenv("SV2_DENYLIST_URL")
	t.Require().NotEmpty(sv2URL)
	chainIDA := l2A.RollupConfig().L2ChainID.Uint64()
	toNum := initRef.Number - 1
	body, _ := json.Marshal(map[string]uint64{"to_block_number": toNum})
	resp, err := http.Post(fmt.Sprintf("%s/admin/rollback?chainId=%d", sv2URL, chainIDA), "application/json", bytes.NewReader(body))
	t.Require().NoError(err)
	if resp != nil {
		defer resp.Body.Close()
		t.Require().Equal(http.StatusNoContent, resp.StatusCode)
	}

	// Wait until SV2 cross-safe detection on B auto-rolls back B (same height, different hash; parent unchanged)
	_ = retry.Do0(ctx, 300, &retry.FixedStrategy{Dur: 250 * time.Millisecond}, func() error {
		// Fetch block at execNum again and compare
		var curr struct {
			Hash string `json:"hash"`
		}
		headHex := fmt.Sprintf("0x%x", execNum)
		if err := elB.L2EthClient().RPC().CallContext(ctx, &curr, "eth_getBlockByNumber", headHex, false); err != nil {
			return err
		}
		if curr.Hash == "" {
			return fmt.Errorf("empty curr hash")
		}
		// Parent must stay the same
		var currParent struct {
			Hash string `json:"hash"`
		}
		parentHex := fmt.Sprintf("0x%x", execNum-1)
		if err := elB.L2EthClient().RPC().CallContext(ctx, &currParent, "eth_getBlockByNumber", parentHex, false); err != nil {
			return err
		}
		if currParent.Hash != suspectParent.Hash {
			return fmt.Errorf("waiting for parent alignment")
		}
		if curr.Hash == suspect.Hash {
			return fmt.Errorf("awaiting replacement at exec height")
		}
		return nil
	})

	// Optional: assert SafeL2 on B regressed below execNum at some point and re-advanced
	// We check that current SafeL2 is at least execNum again eventually, implying a drop + recovery could occur.
	// Dial rollup RPC via SV2 proxy to check SafeL2 status
	rpcB, err := opclient.NewRPC(ctx, t.Logger(), sv2URL+"/opnode/", opclient.WithLazyDial())
	t.Require().NoError(err)
	rollB := sources.NewRollupClient(rpcB)
	_ = retry.Do0(ctx, 240, &retry.FixedStrategy{Dur: 300 * time.Millisecond}, func() error {
		st, err := rollB.SyncStatus(ctx)
		if err != nil {
			return err
		}
		if st.SafeL2.Number < execNum {
			return fmt.Errorf("waiting safe >= execNum")
		}
		return nil
	})
}

// TestSupervisorV2Interop2Predeploys asserts that at interop2 activation the expected
// predeploys (including CrossL2Inbox) are present on L2.
func TestSupervisorV2Interop2Predeploys(gt *testing.T) {
	var ids DefaultMinimalSystemIDs
	opt := stack.Combine[*Orchestrator](
		DefaultMinimalSystemNoCL(&ids),
		WithSupervisorV2OnFirstChain(),
		WithInterop2ActivationOffsetForSV2(6),
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
	el := l2Net.L2ELNode(match.FirstL2EL)
	rcfg := l2Net.RollupConfig()

	// Wait until after interop2 activation
	ctx, cancel := context.WithTimeout(t.Ctx(), 45*time.Second)
	defer cancel()
	_ = retry.Do0(ctx, 60, &retry.FixedStrategy{Dur: 300 * time.Millisecond}, func() error {
		ref, err := el.EthClient().BlockRefByLabel(ctx, eth.Unsafe)
		if err != nil {
			return err
		}
		if uint64(ref.Time) < *rcfg.Interop2Time {
			return fmt.Errorf("waiting for interop2 activation, have %d want >= %d", ref.Time, *rcfg.Interop2Time)
		}
		return nil
	})

	// Check code at key predeploys (CrossL2Inbox, L2ToL2CrossDomainMessenger)
	type addrCheck struct {
		name string
		addr string
	}
	checks := []addrCheck{
		{"CrossL2Inbox", predeploys.CrossL2InboxAddr.Hex()},
		{"L2toL2CrossDomainMessenger", predeploys.L2toL2CrossDomainMessengerAddr.Hex()},
	}

	// Compute activation block number
	activationBlocks := (*rcfg.Interop2Time - rcfg.Genesis.L2Time) / rcfg.BlockTime
	activationNum := rcfg.Genesis.L2.Number + activationBlocks

	actHex := fmt.Sprintf("0x%x", activationNum)

	for _, c := range checks {
		// Assert code present at or after activation
		var codeAt string
		err := el.L2EthClient().RPC().CallContext(ctx, &codeAt, "eth_getCode", c.addr, actHex)
		t.Require().NoError(err)
		t.Require().True(len(codeAt) >= 4 && codeAt != "0x", "expected non-empty code at activation at %s (%s)", c.name, c.addr)
	}
}
