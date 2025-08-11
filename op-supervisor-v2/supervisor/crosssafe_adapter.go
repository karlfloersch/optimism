package supervisor

import (
	"context"
	"fmt"

	"github.com/ethereum/go-ethereum/log"

	"github.com/ethereum-optimism/optimism/op-node/rollup/derive"
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

	// default DBs for the executing chain (chainID)
	logs  *logsdb.DB
	local *fromda.DB
	cross *fromda.DB

	// cross-chain DB lookups (required to validate remote-chain initiating messages)
	lookupLogs  func(chain eth.ChainID) (*logsdb.DB, error)
	lookupLocal func(chain eth.ChainID) (*fromda.DB, error)
	lookupCross func(chain eth.ChainID) (*fromda.DB, error)

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
	// Cross-safe is per-chain; look up the corresponding DB
	if a.lookupCross != nil && chainID != a.chainID {
		if db, err := a.lookupCross(chainID); err == nil && db != nil {
			return db.Last()
		} else if err != nil {
			return types.DerivedBlockSealPair{}, err
		}
	}
	return a.cross.Last()
}

// Contains routes to logs DB Contains
func (a *crosssafeAdapter) Contains(chain eth.ChainID, q types.ContainsQuery) (types.BlockSeal, error) {
	if chain == a.chainID {
		return a.logs.Contains(q)
	}
	if a.lookupLogs == nil {
		return types.BlockSeal{}, fmt.Errorf("no logs lookup for chain %v", chain)
	}
	db, err := a.lookupLogs(chain)
	if err != nil {
		return types.BlockSeal{}, err
	}
	if db == nil {
		return types.BlockSeal{}, fmt.Errorf("logs DB not found for chain %v", chain)
	}
	return db.Contains(q)
}

// CrossDerivedToSource returns the L1 source seal for a given L2 derived ID using cross DB
func (a *crosssafeAdapter) CrossDerivedToSource(chainID eth.ChainID, derived eth.BlockID) (types.BlockSeal, error) {
	if chainID == a.chainID {
		return a.cross.DerivedToFirstSource(derived, types.RevisionAny)
	}
	if a.lookupCross == nil {
		return types.BlockSeal{}, fmt.Errorf("no cross DB lookup for chain %v", chainID)
	}
	db, err := a.lookupCross(chainID)
	if err != nil {
		return types.BlockSeal{}, err
	}
	if db == nil {
		return types.BlockSeal{}, fmt.Errorf("cross DB not found for chain %v", chainID)
	}
	return db.DerivedToFirstSource(derived, types.RevisionAny)
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
	// Clamp scope to last known source in DBs to avoid "future data" queries
	var lastSourceNum uint64
	if chain == a.chainID {
		if pair, e := a.cross.Last(); e == nil {
			lastSourceNum = pair.Source.Number
		}
		if lastSourceNum == 0 {
			if pair, e := a.local.Last(); e == nil {
				lastSourceNum = pair.Source.Number
			}
		}
	} else {
		if a.lookupCross != nil {
			if db, e := a.lookupCross(chain); e == nil && db != nil {
				if pair, e2 := db.Last(); e2 == nil {
					lastSourceNum = pair.Source.Number
				}
			}
		}
		if lastSourceNum == 0 && a.lookupLocal != nil {
			if db, e := a.lookupLocal(chain); e == nil && db != nil {
				if pair, e2 := db.Last(); e2 == nil {
					lastSourceNum = pair.Source.Number
				}
			}
		}
	}
	if lastSourceNum != 0 && scopeNum > lastSourceNum {
		scopeNum = lastSourceNum
	}
	l1Ref, err := a.l1.BlockRefByNumber(context.Background(), scopeNum)
	if err != nil {
		return types.DerivedBlockRefPair{}, fmt.Errorf("l1 scope lookup: %w", err)
	}
	// After-derived is current cross-safe derived, or zero
	var after eth.BlockID
	if pair, err := a.CrossSafe(chain); err == nil {
		after = pair.Derived.ID()
	} else {
		after = eth.BlockID{}
	}
	var rev types.Revision
	// revision is per-chain
	if chain == a.chainID {
		rev, err = a.cross.LastRevision()
	} else if a.lookupCross != nil {
		if db, e := a.lookupCross(chain); e == nil && db != nil {
			rev, err = db.LastRevision()
		} else {
			err = e
		}
	}
	if err != nil {
		// empty: allow any
		rev = types.RevisionAny
	}
	// try cross DB first
	var db *fromda.DB
	if chain == a.chainID {
		db = a.cross
	} else if a.lookupCross != nil {
		if xdb, e := a.lookupCross(chain); e == nil {
			db = xdb
		} else {
			return types.DerivedBlockRefPair{}, e
		}
	}
	if db != nil {
		if cand, cerr := db.Candidate(l1Ref.ID(), after, rev); cerr == nil {
			return cand, nil
		} // else: fall back to local mapping
	}
	// fallback: use local DB to resolve last-derived for this L1 source
	var localDB *fromda.DB
	if chain == a.chainID {
		localDB = a.local
	} else if a.lookupLocal != nil {
		if ldb, e := a.lookupLocal(chain); e == nil {
			localDB = ldb
		} else {
			return types.DerivedBlockRefPair{}, e
		}
	}
	if localDB == nil {
		return types.DerivedBlockRefPair{}, fmt.Errorf("local DB not found for chain %v", chain)
	}
	seal, err := localDB.SourceToLastDerived(l1Ref.ID())
	if err != nil {
		return types.DerivedBlockRefPair{}, err
	}
	// build full derived BlockRef
	env, e := a.l2.PayloadByNumber(context.Background(), seal.Number)
	if e != nil {
		return types.DerivedBlockRefPair{}, err
	}
	br, derr := derive.PayloadToBlockRef(a.l2.RollupConfig(), env.ExecutionPayload)
	if derr != nil {
		return types.DerivedBlockRefPair{}, derr
	}
	return types.DerivedBlockRefPair{Source: l1Ref, Derived: br.BlockRef()}, nil
}

func (a *crosssafeAdapter) NextSource(chain eth.ChainID, source eth.BlockID) (eth.BlockRef, error) {
	var db *fromda.DB
	var err error
	if chain == a.chainID {
		db = a.cross
	} else if a.lookupCross != nil {
		db, err = a.lookupCross(chain)
	}
	if db == nil {
		if err != nil {
			return eth.BlockRef{}, err
		}
		return eth.BlockRef{}, fmt.Errorf("cross DB not found for chain %v", chain)
	}
	seal, err := db.NextSource(source)
	if err != nil {
		return eth.BlockRef{}, err
	}
	return seal.WithParent(source)
}

func (a *crosssafeAdapter) PreviousCrossDerived(chain eth.ChainID, derived eth.BlockID) (types.BlockSeal, error) {
	var db *fromda.DB
	var err error
	if chain == a.chainID {
		db = a.cross
	} else if a.lookupCross != nil {
		db, err = a.lookupCross(chain)
	}
	if db == nil {
		if err != nil {
			return types.BlockSeal{}, err
		}
		return types.BlockSeal{}, fmt.Errorf("cross DB not found for chain %v", chain)
	}
	rev, err := db.DerivedToRevision(derived)
	if err != nil {
		return types.BlockSeal{}, err
	}
	return db.PreviousDerived(derived, rev)
}

func (a *crosssafeAdapter) OpenBlock(chain eth.ChainID, blockNum uint64) (eth.BlockRef, uint32, map[uint32]*types.ExecutingMessage, error) {
	if chain == a.chainID {
		return a.logs.OpenBlock(blockNum)
	}
	if a.lookupLogs == nil {
		return eth.BlockRef{}, 0, nil, fmt.Errorf("no logs lookup for chain %v", chain)
	}
	db, err := a.lookupLogs(chain)
	if err != nil {
		return eth.BlockRef{}, 0, nil, err
	}
	if db == nil {
		return eth.BlockRef{}, 0, nil, fmt.Errorf("logs DB not found for chain %v", chain)
	}
	return db.OpenBlock(blockNum)
}

func (a *crosssafeAdapter) UpdateCrossSafe(chain eth.ChainID, l1View eth.BlockRef, lastCrossDerived eth.BlockRef) error {
	// resolve local & cross DBs for the specified chain
	var localDB *fromda.DB
	var crossDB *fromda.DB
	var err error
	if chain == a.chainID {
		localDB = a.local
		crossDB = a.cross
	} else if a.lookupLocal != nil && a.lookupCross != nil {
		localDB, err = a.lookupLocal(chain)
		if err != nil {
			return err
		}
		crossDB, err = a.lookupCross(chain)
		if err != nil {
			return err
		}
	}
	if localDB == nil || crossDB == nil {
		return fmt.Errorf("missing DBs for chain %v", chain)
	}
	rev, err := localDB.SourceToRevision(l1View.ID())
	if err != nil {
		return err
	}
	return crossDB.AddDerived(l1View, lastCrossDerived, rev)
}

func (a *crosssafeAdapter) InvalidateLocalSafe(chainID eth.ChainID, candidate types.DerivedBlockRefPair) error {
	// resolve local DB for executing chain
	localDB := a.local
	if chainID != a.chainID && a.lookupLocal != nil {
		if db, err := a.lookupLocal(chainID); err == nil && db != nil {
			localDB = db
		} else if err != nil {
			return err
		}
	}
	// 1) mark invalid in localDB (this rewinds and adds invalidation entry)
	inv := a.reads // Registry implements reads.Invalidator
	if err := localDB.RewindAndInvalidate(inv, candidate); err != nil {
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
