package supervisor

import (
	"context"
	"os"
	"strconv"
)

// heightChecker proposes denylist+rollback based on configured heights.
// Config via env:
// - SV2_DENY_HEIGHT: absolute L2 height to invalidate (single value)
// - or SV2_DENY_EVERY_N: invalidate every N-th height (CrossFinalized multiple)
// Uses Snapshot.ResolvePayloadHash to compute payload IDs.
type heightChecker struct {
	targetHeight uint64
	proposed     bool
}

func NewHeightCheckerFromEnv() BlockValidityChecker {
	var h heightChecker
	if v := os.Getenv("SV2_DENY_HEIGHT"); v != "" {
		if n, err := strconv.ParseUint(v, 10, 64); err == nil {
			h.targetHeight = n
		}
	}
	if h.targetHeight == 0 {
		return nil
	}
	return &h
}

func (c *heightChecker) Evaluate(ctx context.Context, snap Snapshot) ([]Proposal, error) {
	if c.proposed {
		return nil, nil
	}
	target := c.targetHeight
	if target == 0 || target > snap.CrossFinalized { // only act when target ≤ cross-finalized
		return nil, nil
	}
	to := target - 1
	var props []Proposal
	for chainID := range snap.PerChain {
		var pid string
		if snap.ResolvePayloadHash != nil {
			if hash, err := snap.ResolvePayloadHash(chainID, target); err == nil && hash != "" {
				pid = hash
			}
		}
		props = append(props, Proposal{ChainID: chainID, PayloadID: pid, ToBlock: to, Reason: "height deny"})
	}
	// ensure we only propose once to avoid thrashing
	c.proposed = true
	return props, nil
}
