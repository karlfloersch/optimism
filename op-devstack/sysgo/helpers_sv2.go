package sysgo

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/ethereum/go-ethereum/log"

	"github.com/ethereum-optimism/optimism/op-node/rollup"
	opclient "github.com/ethereum-optimism/optimism/op-service/client"
	"github.com/ethereum-optimism/optimism/op-service/retry"
	"github.com/ethereum-optimism/optimism/op-service/sources"
)

// WaitOpNodeProxyReady polls the SV2 op-node proxy for a specific chain until SyncStatus responds.
func WaitOpNodeProxyReady(ctx context.Context, sv2URL string, chainID uint64, logger log.Logger) error {
	opnode := fmt.Sprintf("%s/opnode/%d/", sv2URL, chainID)
	return retry.Do0(ctx, 120, &retry.FixedStrategy{Dur: 250 * time.Millisecond}, func() error {
		cli, err := opclient.NewRPC(ctx, logger, opnode, opclient.WithLazyDial())
		if err != nil {
			return err
		}
		defer cli.Close()
		roll := sources.NewRollupClient(cli)
		_, err = roll.SyncStatus(ctx)
		return err
	})
}

// WaitSafeAtOrAbove polls rollup SyncStatus (via op-node user RPC URL) until Safe (or LocalSafe) >= min.
func WaitSafeAtOrAbove(ctx context.Context, opnodeURL string, min uint64, logger log.Logger) error {
	return retry.Do0(ctx, 240, &retry.FixedStrategy{Dur: 250 * time.Millisecond}, func() error {
		cli, err := opclient.NewRPC(ctx, logger, opnodeURL, opclient.WithLazyDial())
		if err != nil {
			return err
		}
		defer cli.Close()
		roll := sources.NewRollupClient(cli)
		st, err := roll.SyncStatus(ctx)
		if err != nil || st == nil {
			if err == nil {
				return fmt.Errorf("nil status")
			}
			return err
		}
		safe := st.SafeL2.Number
		if safe == 0 {
			safe = st.LocalSafeL2.Number
		}
		if safe < min {
			return fmt.Errorf("waiting safe >= %d, have %d", min, safe)
		}
		return nil
	})
}

// WaitSV2CrossFinalizedAtLeast polls SV2 /status until cross_finalized >= min.
// Uses the first available chain ID from the system.
// Deprecated: Use WaitSV2CrossFinalizedAtLeastForChain instead.
func WaitSV2CrossFinalizedAtLeast(ctx context.Context, sv2URL string, min uint64) error {
	// For backward compatibility, we need to determine a chainID to use
	// This is a helper function that should ideally be updated to take chainID as parameter
	chainID := uint64(901) // Default to first chain ID commonly used in tests
	return WaitSV2CrossFinalizedAtLeastForChain(ctx, sv2URL, chainID, min)
}

// WaitSV2CrossFinalizedAtLeastForChain polls SV2 /status until cross_finalized >= min for a specific chain.
func WaitSV2CrossFinalizedAtLeastForChain(ctx context.Context, sv2URL string, chainID uint64, min uint64) error {
	statusURL := fmt.Sprintf("%s/status?chainId=%d", sv2URL, chainID)
	return retry.Do0(ctx, 120, &retry.FixedStrategy{Dur: 300 * time.Millisecond}, func() error {
		resp, err := http.Get(statusURL)
		if err != nil {
			return err
		}
		defer resp.Body.Close()
		var out map[string]any
		if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
			return err
		}
		v, ok := out["cross_finalized"].(float64)
		if !ok || uint64(v) < min {
			return fmt.Errorf("cross_finalized < %d (got %v)", min, out["cross_finalized"])
		}
		return nil
	})
}

// WaitDenylistContains polls SV2 denylist check until {denylisted:true} for the given payload ID and chain.
func WaitDenylistContains(ctx context.Context, sv2URL string, chainID uint64, payloadID string) error {
	url := fmt.Sprintf("%s/denylist/v1/check?chainId=%d&id=%s", sv2URL, chainID, payloadID)
	return retry.Do0(ctx, 120, &retry.FixedStrategy{Dur: 250 * time.Millisecond}, func() error {
		resp, err := http.Get(url)
		if err != nil {
			return err
		}
		defer resp.Body.Close()
		var out struct {
			Denylisted bool `json:"denylisted"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
			return err
		}
		if !out.Denylisted {
			return fmt.Errorf("not denylisted yet")
		}
		return nil
	})
}

// WaitSV2Ready polls the SV2 v1-compatible sync status endpoint until it returns 200 OK.
func WaitSV2Ready(ctx context.Context, sv2URL string) error {
	return retry.Do0(ctx, 160, &retry.FixedStrategy{Dur: 250 * time.Millisecond}, func() error {
		resp, err := http.Get(fmt.Sprintf("%s/v1/sync_status", sv2URL))
		if err != nil {
			return err
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("not ready: %d", resp.StatusCode)
		}
		return nil
	})
}

// GetBlockHashByNumber fetches the block hash at a given L2 number using eth_getBlockByNumber.
func GetBlockHashByNumber(ctx context.Context, rpc opclient.RPC, num uint64) (string, error) {
	var out struct {
		Hash string `json:"hash"`
	}
	hexNum := fmt.Sprintf("0x%x", num)
	if err := rpc.CallContext(ctx, &out, "eth_getBlockByNumber", hexNum, false); err != nil {
		return "", err
	}
	if out.Hash == "" {
		return "", fmt.Errorf("empty hash at %d", num)
	}
	return out.Hash, nil
}

// WaitBlockReplacedAtHeight waits until the block hash at the given height differs from oldHash, returns the new hash.
func WaitBlockReplacedAtHeight(ctx context.Context, rpc opclient.RPC, height uint64, oldHash string) (string, error) {
	var newHash string
	if err := retry.Do0(ctx, 240, &retry.FixedStrategy{Dur: 250 * time.Millisecond}, func() error {
		h, err := GetBlockHashByNumber(ctx, rpc, height)
		if err != nil {
			return err
		}
		if h == oldHash {
			return fmt.Errorf("not replaced yet")
		}
		newHash = h
		return nil
	}); err != nil {
		return "", err
	}
	return newHash, nil
}

// ComputePayloadIDAtNumber returns the execution block hash (payload ID) at the given L2 height.
func ComputePayloadIDAtNumber(ctx context.Context, l2RPC opclient.RPC, rcfg *rollup.Config, num uint64, logger log.Logger) (string, error) {
	l2, err := sources.NewL2Client(l2RPC, logger, nil, sources.L2ClientDefaultConfig(rcfg, true))
	if err != nil {
		return "", err
	}
	env, err := l2.PayloadByNumber(ctx, num)
	if err != nil {
		return "", err
	}
	if hash, ok := env.CheckBlockHash(); ok {
		return hash.Hex(), nil
	}
	return "", fmt.Errorf("no payload hash at %d", num)
}
