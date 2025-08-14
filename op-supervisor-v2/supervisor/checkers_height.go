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
    everyN       uint64
}

func NewHeightCheckerFromEnv() BlockValidityChecker {
    var h heightChecker
    if v := os.Getenv("SV2_DENY_HEIGHT"); v != "" {
        if n, err := strconv.ParseUint(v, 10, 64); err == nil {
            h.targetHeight = n
        }
    }
    if v := os.Getenv("SV2_DENY_EVERY_N"); v != "" {
        if n, err := strconv.ParseUint(v, 10, 64); err == nil && n > 0 {
            h.everyN = n
        }
    }
    if h.targetHeight == 0 && h.everyN == 0 {
        return nil
    }
    return &h
}

func (c *heightChecker) Evaluate(ctx context.Context, snap Snapshot) ([]Proposal, error) {
    var target uint64
    if c.targetHeight != 0 {
        target = c.targetHeight
    } else if c.everyN != 0 && snap.CrossFinalized != 0 {
        if snap.CrossFinalized%c.everyN == 0 {
            target = snap.CrossFinalized
        }
    }
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
    return props, nil
}


