package supervisor

import (
	"context"
)

// Proposal represents a suggested corrective action at or before a given L2 height.
type Proposal struct {
	ChainID uint64
	// PayloadID is the deterministic block header hash that should be denylisted
	PayloadID string
	// ToBlock is the absolute L2 block number to roll back the EL to (commonly H-1)
	ToBlock uint64
	Reason  string
}

// Snapshot is a minimal view of chain state at a cross-finalized height.
// This can be extended later with L1/L2 headers and derived associations.
type Snapshot struct {
	CrossFinalized uint64
	PerChain       map[uint64]ChainSnapshot
    // ResolvePayloadHash resolves the payload hash for a given chain at height H (best-effort)
    ResolvePayloadHash func(chainID uint64, height uint64) (string, error)
}

// ChainSnapshot summarizes per-chain state used by checkers.
type ChainSnapshot struct {
	Finalized uint64
}

// BlockValidityChecker evaluates a snapshot and returns zero or more proposals.
type BlockValidityChecker interface {
	Evaluate(ctx context.Context, snap Snapshot) ([]Proposal, error)
}
