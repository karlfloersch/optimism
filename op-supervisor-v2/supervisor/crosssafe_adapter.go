package supervisor

import (
	"context"
	"fmt"

	"github.com/ethereum/go-ethereum/log"

	"github.com/ethereum-optimism/optimism/op-service/eth"
	"github.com/ethereum-optimism/optimism/op-service/sources"
	"github.com/ethereum-optimism/optimism/op-supervisor/supervisor/backend/cross"
	fromda "github.com/ethereum-optimism/optimism/op-supervisor/supervisor/backend/db/fromda"
	logsdb "github.com/ethereum-optimism/optimism/op-supervisor/supervisor/backend/db/logs"
	"github.com/ethereum-optimism/optimism/op-supervisor/supervisor/backend/depset"
	"github.com/ethereum-optimism/optimism/op-supervisor/supervisor/backend/reads"
	"github.com/ethereum-optimism/optimism/op-supervisor/supervisor/types"
)

// crosssafeAdapter implements cross.CrossSafeDeps backed by v1 DBs.
type crosssafeAdapter struct {
	logger log.Logger

	chainID eth.ChainID

	logs  *logsdb.DB
	local *fromda.DB
	cross *fromda.DB

	reads *reads.Registry

	// data sources
	l1 *sources.L1Client
	l2 *sources.L2Client

	// sv2 hooks
	addDenylist func(chainID uint64, id string) error
	rollback    func(ctx context.Context, chainID uint64, toBlock uint64) error

	// scope
	l1ConfirmDepth uint64

	// supervisor-provided L1 scope label (Safe in prod, Unsafe in tests)
	l1ScopeLabel eth.BlockLabel
}

func (a *crosssafeAdapter) AcquireHandle() reads.Handle { return a.reads.AcquireHandle() }

func (a *crosssafeAdapter) CrossSafe(chainID eth.ChainID) (types.DerivedBlockSealPair, error) {
	return a.cross.Last()
}

// Contains routes to logs DB Contains
func (a *crosssafeAdapter) Contains(chain eth.ChainID, q types.ContainsQuery) (types.BlockSeal, error) {
	return a.logs.Contains(q)
}

// CrossDerivedToSource returns the L1 source seal for a given L2 derived ID using cross DB
func (a *crosssafeAdapter) CrossDerivedToSource(chainID eth.ChainID, derived eth.BlockID) (types.BlockSeal, error) {
	return a.cross.DerivedToFirstSource(derived, types.RevisionAny)
}

func (a *crosssafeAdapter) CandidateCrossSafe(chain eth.ChainID) (types.DerivedBlockRefPair, error) {
	// Determine L1 scope using depth gating: HeadL1 - confirmDepth
	st, err := a.rollupSync()
	if err != nil {
		return types.DerivedBlockRefPair{}, err
	}
	headNum := st.HeadL1.Number
	var scopeNum uint64
	if headNum > a.l1ConfirmDepth {
		scopeNum = headNum - a.l1ConfirmDepth
	} else {
		scopeNum = 0
	}
	l1Ref, err := a.l1.BlockRefByNumber(context.Background(), scopeNum)
	if err != nil {
		return types.DerivedBlockRefPair{}, fmt.Errorf("l1 scope lookup: %w", err)
	}
	// After-derived is current cross-safe derived, or zero
	var after eth.BlockID
	if pair, err := a.cross.Last(); err == nil {
		after = pair.Derived.ID()
	} else {
		after = eth.BlockID{}
	}
	rev, err := a.cross.LastRevision()
	if err != nil {
		// empty: allow any
		rev = types.RevisionAny
	}
	return a.cross.Candidate(l1Ref.ID(), after, rev)
}

func (a *crosssafeAdapter) NextSource(chain eth.ChainID, source eth.BlockID) (eth.BlockRef, error) {
	seal, err := a.cross.NextSource(source)
	if err != nil {
		return eth.BlockRef{}, err
	}
	return seal.WithParent(source)
}

func (a *crosssafeAdapter) PreviousCrossDerived(chain eth.ChainID, derived eth.BlockID) (types.BlockSeal, error) {
	rev, err := a.cross.DerivedToRevision(derived)
	if err != nil {
		return types.BlockSeal{}, err
	}
	return a.cross.PreviousDerived(derived, rev)
}

func (a *crosssafeAdapter) OpenBlock(chain eth.ChainID, blockNum uint64) (eth.BlockRef, uint32, map[uint32]*types.ExecutingMessage, error) {
	return a.logs.OpenBlock(blockNum)
}

func (a *crosssafeAdapter) UpdateCrossSafe(chain eth.ChainID, l1View eth.BlockRef, lastCrossDerived eth.BlockRef) error {
	rev, err := a.local.SourceToRevision(l1View.ID())
	if err != nil {
		return err
	}
	return a.cross.AddDerived(l1View, lastCrossDerived, rev)
}

func (a *crosssafeAdapter) InvalidateLocalSafe(chainID eth.ChainID, candidate types.DerivedBlockRefPair) error {
	// 1) mark invalid in localDB (this rewinds and adds invalidation entry)
	inv := a.reads // Registry implements reads.Invalidator
	if err := a.local.RewindAndInvalidate(inv, candidate); err != nil {
		return err
	}
	// 2) compute deterministic payload header-hash (stand-in PayloadID)
	env, err := a.l2.PayloadByNumber(context.Background(), candidate.Derived.Number)
	if err != nil {
		return fmt.Errorf("payload for denylist: %w", err)
	}
	if actual, ok := env.CheckBlockHash(); ok {
		if a.addDenylist != nil {
			if v, ok := a.chainID.Uint64(); ok {
				_ = a.addDenylist(v, actual.Hex())
			}
		}
	}
	// 3) rollback EL to H-1 to force replacement
	if a.rollback != nil {
		if v, ok := a.chainID.Uint64(); ok {
			return a.rollback(context.Background(), v, candidate.Derived.Number-1)
		}
	}
	return nil
}

// rollupSync fetches op-node sync for scope gating; caller ensures context and client
func (a *crosssafeAdapter) rollupSync() (*eth.SyncStatus, error) {
	// The L1 client does not expose op-node sync; the supervisor already has rollup client per chain.
	// For simplicity here we only gate by L1 head fetched via l1 client best-effort.
	// Use BlockRefByLabel("latest") equivalent: Head number from eth_blockNumber.
	// Fallback: return a synthetic status with HeadL1=SafeL1=FinalizedL1 from recent heads.
	// We keep this minimal to avoid pulling rollup client here; SV2 main loop can pass in the scope number instead if needed later.
	label := a.l1ScopeLabel
	// default to Safe if unspecified (BlockLabel has no zero sentinel in type system)
	if label != eth.Unsafe && label != eth.Safe && label != eth.Finalized {
		label = eth.Safe
	}
	head, err := a.l1.BlockRefByLabel(context.Background(), label)
	if err != nil {
		// fallback to 0 scope to be safe
		return &eth.SyncStatus{HeadL1: head}, nil
	}
	return &eth.SyncStatus{HeadL1: head}, nil
}

// buildLinkChecker constructs a default depset.LinkChecker from an interop config+dep-set.
func buildLinkChecker(cfg depset.LinkerConfig) depset.LinkChecker {
	return depset.LinkerFromConfig(cfg)
}

// runCrossSafeOnce executes one step of cross-safe update using v1 algorithm.
func (a *crosssafeAdapter) runCrossSafeOnce(logger log.Logger, linker depset.LinkChecker) error {
	return cross.CrossSafeUpdate(logger, a.chainID, a, linker)
}
