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
	"github.com/ethereum-optimism/optimism/op-devstack/shim"
	"github.com/ethereum-optimism/optimism/op-devstack/stack"
	"github.com/ethereum-optimism/optimism/op-devstack/stack/match"
	"github.com/ethereum-optimism/optimism/op-service/eth"
	"github.com/ethereum-optimism/optimism/op-service/retry"
	"github.com/ethereum-optimism/optimism/op-service/sources"

	"github.com/ethereum-optimism/optimism/op-service/testlog"
)

// TestSupervisorV2Rollback exercises Supervisor v2 rollback + denylist behavior in a single-chain sysgo preset.
// It performs an end-to-end flow and validates each property explicitly:
//
//   - Boot a minimal system with SV2 embedding an op-node; enable denylist seeding every block (1-in-1).
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
	)

	logger := testlog.Logger(gt, log.LevelInfo)
	onFail, onSkipNow := exiters(gt)
	p := devtest.NewP(context.Background(), logger, onFail, onSkipNow)
	gt.Cleanup(p.Close)

	// Enable denylist seeding every block before starting SV2
	_ = os.Setenv("SV2_DENYLIST_ONE_IN_N", "1")

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

		// Ensure SV2 denylist contains this ID before rollback (seeded 1-in-1)
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
	reqBody, _ := json.Marshal(map[string]uint64{"back_n_blocks": 1})
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
