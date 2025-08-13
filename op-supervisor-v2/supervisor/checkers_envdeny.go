package supervisor

import (
    "context"
    "os"
    "strings"
)

// envDenyChecker reads SV2_DENY_PAYLOADS (comma-separated payloadIDs) and proposes rollbacks
// to H-1 for any chain at cross-finalized height if its current finalized payload matches.
// This is a trivial testing checker to exercise proposal plumbing.
type envDenyChecker struct{}

func NewEnvDenyChecker() BlockValidityChecker { return &envDenyChecker{} }

func (c *envDenyChecker) Evaluate(ctx context.Context, snap Snapshot) ([]Proposal, error) {
    ids := strings.TrimSpace(os.Getenv("SV2_DENY_PAYLOADS"))
    if ids == "" {
        return nil, nil
    }
    deny := map[string]struct{}{}
    for _, s := range strings.Split(ids, ",") {
        s = strings.TrimSpace(s)
        if s != "" {
            deny[s] = struct{}{}
        }
    }
    if len(deny) == 0 || snap.CrossFinalized == 0 {
        return nil, nil
    }
    // We do not have per-height payload IDs here yet; this checker is best-effort placeholder.
    // It proposes a rollback if any denylist entry exists, using CrossFinalized-1.
    // A real checker would compare payload headers at H against the deny set.
    var props []Proposal
    to := snap.CrossFinalized - 1
    if to == 0 {
        return nil, nil
    }
    for chainID := range snap.PerChain {
        for id := range deny {
            props = append(props, Proposal{ChainID: chainID, PayloadID: id, ToBlock: to, Reason: "env deny"})
        }
    }
    return props, nil
}


