package supervisor

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/ethereum-optimism/optimism/op-node/rollup"
	"github.com/ethereum-optimism/optimism/op-node/rollup/derive"
	opclient "github.com/ethereum-optimism/optimism/op-service/client"
	"github.com/ethereum-optimism/optimism/op-service/eth"
	"github.com/ethereum-optimism/optimism/op-service/sources"
	fromda "github.com/ethereum-optimism/optimism/op-supervisor/supervisor/backend/db/fromda"
	logsdb "github.com/ethereum-optimism/optimism/op-supervisor/supervisor/backend/db/logs"
	"github.com/ethereum-optimism/optimism/op-supervisor/supervisor/backend/reads"
	"github.com/ethereum-optimism/optimism/op-supervisor/supervisor/types"
)

// ensureL1Client lazily initializes the L1 client using the given RPC URL.
func (s *Supervisor) ensureL1Client(ctx context.Context, l1Cli opclient.RPC, l1 *sources.L1Client, l1RPC string, rcfg *rollup.Config) (opclient.RPC, *sources.L1Client) {
	if l1 != nil {
		return l1Cli, l1
	}
	if l1Cli == nil {
		if c, e := opclient.NewRPC(ctx, s.log, l1RPC); e == nil {
			l1Cli = c
		}
	}
	if l1Cli != nil && l1 == nil {
		if l, e := sources.NewL1Client(l1Cli, s.log, nil, sources.L1ClientDefaultConfig(rcfg, true, sources.RPCKindStandard)); e == nil {
			l = l
			return l1Cli, l
		}
	}
	return l1Cli, l1
}

// ingestToLocalSafe computes the ingest range and ingests up to target.
func (s *Supervisor) ingestToLocalSafe(ctx context.Context, h *chainHandle, l1 *sources.L1Client, l2 *sources.L2Client, rcfg *rollup.Config, chainID uint64, target uint64) {
	var startLogs uint64 = 0
	if last, ok := h.logsDB.LatestSealedBlock(); ok {
		startLogs = last.Number + 1
		if last.Number >= target {
			return
		}
	}
	var startLocal uint64 = 0
	if pair, err := h.localDB.Last(); err == nil {
		startLocal = pair.Derived.Number + 1
	}
	start := startLogs
	if startLocal < start {
		start = startLocal
	}
	if start > target {
		start = target
	}
	s.log.Info("ingest: range", "chain", chainID, "start", start, "end", target)
	if err := ingestRange(ctx, l1, l2, h.logsDB, h.localDB, h.crossDB, sources.L2ClientDefaultConfig(rcfg, true), start, target); err != nil {
		s.log.Info("ingest: deferred", "err", err)
	}
}

// seedLocalIfEmpty seeds the first derived mapping from the current target if local DB is empty.
func (s *Supervisor) seedLocalIfEmpty(ctx context.Context, h *chainHandle, l1 *sources.L1Client, l2 *sources.L2Client, rcfg *rollup.Config, chainID uint64, target uint64) {
	if !h.localDB.IsEmpty() {
		return
	}
	env, err := l2.PayloadByNumber(ctx, target)
	if err != nil {
		return
	}
	if br, derr := derive.PayloadToBlockRef(rcfg, env.ExecutionPayload); derr == nil {
		l1Ref, e1 := l1.BlockRefByNumber(ctx, br.L1Origin.Number)
		if e1 == nil && l1Ref.Hash == br.L1Origin.Hash {
			if did, _ := s.ensureDerived(h.localDB, l1Ref, br.BlockRef(), types.RevisionAny); did {
				s.log.Info("seed: local derived from target", "chain", chainID, "l1", l1Ref, "l2", br.BlockRef())
			}
		}
	}
}

// debugIngestHeads logs the heads of logs/local/cross for observability.
func (s *Supervisor) debugIngestHeads(h *chainHandle, chainID uint64) {
	if blk, ok := h.logsDB.LatestSealedBlock(); ok {
		s.log.Info("ingest: logs head", "chain", chainID, "num", blk.Number)
	}
	if pair, err := h.localDB.Last(); err == nil {
		s.log.Info("ingest: local head", "chain", chainID, "l1", pair.Source, "l2", pair.Derived)
	} else {
		s.log.Info("ingest: local head err", "chain", chainID, "err", err)
	}
	if h.crossDB != nil {
		if pair, err := h.crossDB.Last(); err == nil {
			s.log.Info("ingest: cross head", "chain", chainID, "l1", pair.Source, "l2", pair.Derived)
		} else {
			s.log.Info("ingest: cross head err", "chain", chainID, "err", err)
		}
	}
}

// ensureDerived performs a guarded AddDerived write that only appends when advancing.
// Returns true if a write was performed.
func (s *Supervisor) ensureDerived(db *fromda.DB, l1Ref eth.BlockRef, l2Ref eth.BlockRef, rev types.Revision) (bool, error) {
	if db == nil {
		return false, nil
	}
	if pair, err := db.Last(); err == nil {
		if pair.Derived.Number >= l2Ref.Number {
			return false, nil
		}
	}
	if err := db.AddDerived(l1Ref, l2Ref, rev); err != nil {
		return false, err
	}
	return true, nil
}

// bootstrapCrossIfEmpty initializes cross DB from local DB once.
func (s *Supervisor) bootstrapCrossIfEmpty(ctx context.Context, h *chainHandle, l1 *sources.L1Client, l2 *sources.L2Client, rcfg *rollup.Config, chainID uint64) {
	if !h.crossDB.IsEmpty() {
		return
	}
	if pair, err := h.localDB.Last(); err == nil {
		l1Ref, err1 := l1.BlockRefByNumber(ctx, pair.Source.Number)
		env, err2 := l2.PayloadByNumber(ctx, pair.Derived.Number)
		var dref eth.BlockRef
		if err2 == nil {
			if br, derr := derive.PayloadToBlockRef(rcfg, env.ExecutionPayload); derr == nil {
				dref = br.BlockRef()
			}
		}
		if err1 == nil && dref.Number == pair.Derived.Number {
			if did, _ := s.ensureDerived(h.crossDB, l1Ref, dref, types.RevisionAny); did {
				s.log.Info("bootstrap cross DB from local", "chain", chainID, "l1", l1Ref, "l2", dref)
			}
		}
	}
}

// newCrosssafeAdapter builds the adapter with closures to look up per-chain DBs.
func (s *Supervisor) newCrosssafeAdapter(h *chainHandle, l1 *sources.L1Client, l2 *sources.L2Client, rcfg *rollup.Config, chainID uint64) *crosssafeAdapter {
	return &crosssafeAdapter{
		logger:  s.log,
		chainID: eth.ChainIDFromUInt64(chainID),
		logs:    h.logsDB,
		local:   h.localDB,
		cross:   h.crossDB,
		lookupLogs: func(cid eth.ChainID) (*logsdb.DB, error) {
			if v, ok := cid.Uint64(); ok {
				s.mu.Lock()
				h2 := s.chains[v]
				s.mu.Unlock()
				if h2 != nil {
					return h2.logsDB, nil
				}
			}
			return nil, fmt.Errorf("unknown chain %v", cid)
		},
		lookupLocal: func(cid eth.ChainID) (*fromda.DB, error) {
			if v, ok := cid.Uint64(); ok {
				s.mu.Lock()
				h2 := s.chains[v]
				s.mu.Unlock()
				if h2 != nil {
					return h2.localDB, nil
				}
			}
			return nil, fmt.Errorf("unknown chain %v", cid)
		},
		lookupCross: func(cid eth.ChainID) (*fromda.DB, error) {
			if v, ok := cid.Uint64(); ok {
				s.mu.Lock()
				h2 := s.chains[v]
				s.mu.Unlock()
				if h2 != nil {
					return h2.crossDB, nil
				}
			}
			return nil, fmt.Errorf("unknown chain %v", cid)
		},
		reads:          reads.NewRegistry(s.log),
		l1:             l1,
		l2:             l2,
		addDenylist:    func(cid uint64, id string) error { return s.denylist.Add(cid, id) },
		rollback:       s.RollbackChain,
		l1ConfirmDepth: h.embeddedCfg.confirmDepth,
		l1ScopeLabel:   s.getL1ScopeLabel(),
	}
}

// runCrossSafeStep executes one adapter step and logs the outcome.
func (s *Supervisor) runCrossSafeStep(ctx context.Context, adapter *crosssafeAdapter, chainID uint64) {
	s.log.Info("crosssafe: run", "chain", chainID)
	if err := adapter.runCrossSafeOnce(s.log, s.getLinker()); err != nil {
		msg := err.Error()
		if strings.Contains(msg, "future data") || strings.Contains(msg, "past last entry") {
			s.log.Info("crosssafe: waiting for ingest", "chain", chainID, "err", err)
		} else {
			s.log.Warn("crosssafe: error", "chain", chainID, "err", err)
		}
	}
	if cs, err := adapter.cross.Last(); err == nil {
		s.log.Info("crosssafe: head", "chain", chainID, "derived", cs.Derived)
	} else {
		s.log.Warn("crosssafe: last error", "chain", chainID, "err", err)
	}
}

// chainHandle tracks the per-chain state (embedded op-node lifecycle, DBs, and pollers).
type chainHandle struct {
	stateMu sync.Mutex

	// runtime state
	embeddedOpNodeUserRPC string
	stopEmbeddedOpNode    func(ctx context.Context) error
	cancelPoll            context.CancelFunc
	started               time.Time

	// config for restart/rollback
	embeddedCfg *embeddedConfig

	// v1 DBs per chain
	logsDB  *logsdb.DB
	localDB *fromda.DB
	crossDB *fromda.DB
}

// AddChain starts an embedded op-node and polling for the given rollup config and RPCs.
// Returns the L2 chain ID as the handle key.
func (s *Supervisor) AddChain(l1RPC string, beaconAddr string, l2AuthRPC string, l2UserRPC string, jwtSecret [32]byte, rcfg *rollup.Config, interval time.Duration, confirmDepth uint64) (uint64, error) {
	chainID := rcfg.L2ChainID.Uint64()

	// Start embedded op-node
	userRPC, stopFn, err := s.StartEmbeddedOpNode(l1RPC, beaconAddr, l2AuthRPC, jwtSecret, rcfg)
	if err != nil {
		return 0, err
	}

	h := &chainHandle{
		embeddedOpNodeUserRPC: userRPC,
		stopEmbeddedOpNode:    stopFn,
		embeddedCfg: &embeddedConfig{
			l1RPC:        l1RPC,
			beaconAddr:   beaconAddr,
			l2AuthRPC:    l2AuthRPC,
			l2UserRPC:    l2UserRPC,
			jwtSecret:    jwtSecret,
			rcfg:         rcfg,
			interval:     interval,
			confirmDepth: confirmDepth,
		},
		started: time.Now(),
	}

	// Start polling
	ctxPoll, cancel := context.WithCancel(context.Background())
	h.cancelPoll = cancel

	// Dial clients for polling
	opNodeCli, err := opclient.NewRPC(ctxPoll, s.log, userRPC)
	if err != nil {
		cancel()
		_ = stopFn(context.Background())
		return 0, err
	}
	l2Cli, err := opclient.NewRPC(ctxPoll, s.log, l2UserRPC)
	if err != nil {
		cancel()
		_ = stopFn(context.Background())
		return 0, err
	}
	l2, err := sources.NewL2Client(l2Cli, s.log, nil, sources.L2ClientDefaultConfig(rcfg, true))
	if err != nil {
		cancel()
		_ = stopFn(context.Background())
		return 0, err
	}
	roll := sources.NewRollupClient(opNodeCli)

	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		// lazy L1 client for ingest
		l1Cli, _ := opclient.NewRPC(ctxPoll, s.log, h.embeddedCfg.l1RPC)
		var l1 *sources.L1Client
		if l1Cli != nil {
			l1, _ = sources.NewL1Client(l1Cli, s.log, nil, sources.L1ClientDefaultConfig(rcfg, true, sources.RPCKindStandard))
		}
		// open DBs if not already
		if h.logsDB == nil || h.localDB == nil || h.crossDB == nil {
			logs, local, cross, err := s.openChainDBs(s.log, chainID, s.getDataDir())
			if err == nil {
				h.logsDB, h.localDB, h.crossDB = logs, local, cross
			} else {
				s.log.Warn("failed to open sv2 dbs", "err", err)
			}
		}
		// register this chain in the shared linker
		s.registerChainForLinker(eth.ChainIDFromUInt64(chainID))

		for {
			select {
			case <-ctxPoll.Done():
				return
			case <-ticker.C:
				st, err := roll.SyncStatus(ctxPoll)
				if err != nil || st == nil {
					s.log.Warn("poll: sync status error", "err", err)
					continue
				}
				// Ensure L1 client is initialized (may fail early before L1 comes up)
				l1Cli, l1 = s.ensureL1Client(ctxPoll, l1Cli, l1, h.embeddedCfg.l1RPC, rcfg)
				// Ingest strictly up to local-safe; skip until local-safe progresses
				target := st.LocalSafeL2
				// Include cross-safe (from cross DB) for observability
				var crossSafe any
				if h.crossDB != nil {
					if pair, err := h.crossDB.Last(); err == nil {
						crossSafe = pair.Derived
					}
				}
				s.log.Info("poll: heads", "chain", chainID, "unsafe", st.UnsafeL2, "local_safe", st.LocalSafeL2, "safe", st.SafeL2, "finalized", st.FinalizedL2, "cross_safe", crossSafe)
				if target.Number == 0 {
					continue
				}
				// Ingest and optionally seed/debug if there is new work
				if h.logsDB != nil && h.localDB != nil && l1 != nil {
					s.ingestToLocalSafe(ctxPoll, h, l1, l2, rcfg, chainID, target.Number)
					s.seedLocalIfEmpty(ctxPoll, h, l1, l2, rcfg, chainID, target.Number)
					s.debugIngestHeads(h, chainID)
				}
				// bootstrap cross DB once from latest local derived so cross-safe can start progressing
				if h.crossDB != nil && h.localDB != nil {
					s.bootstrapCrossIfEmpty(ctxPoll, h, l1, l2, rcfg, chainID)
				}
				// drive one step of cross-safe update
				if h.logsDB != nil && h.localDB != nil && h.crossDB != nil {
					adapter := s.newCrosssafeAdapter(h, l1, l2, rcfg, chainID)
					s.runCrossSafeStep(ctxPoll, adapter, chainID)
				}
			}
		}
	}()

	// Register handle
	s.mu.Lock()
	if s.chains == nil {
		s.chains = make(map[uint64]*chainHandle)
	}
	s.chains[chainID] = h
	if s.primaryChainID == 0 {
		s.primaryChainID = chainID
	}
	s.mu.Unlock()

	// Finalized runner removed; cross-safe advancement is driven by per-chain pollers

	return chainID, nil
}

// getCrossFinalized returns the DB-backed min cross-safe height (0 if none).
func (s *Supervisor) getCrossFinalized() uint64 { return s.crossFinalizedFromDBOrFallback() }

// maybeStartFinalizedRunner is removed (no-op).
func (s *Supervisor) maybeStartFinalizedRunner() {}

// RemoveChain stops and unregisters a chain by ID.
func (s *Supervisor) RemoveChain(chainID uint64) {
	s.mu.Lock()
	h := s.chains[chainID]
	delete(s.chains, chainID)
	s.mu.Unlock()
	if h == nil {
		return
	}
	h.stateMu.Lock()
	if h.cancelPoll != nil {
		h.cancelPoll()
		h.cancelPoll = nil
	}
	if h.stopEmbeddedOpNode != nil {
		_ = h.stopEmbeddedOpNode(context.Background())
		h.stopEmbeddedOpNode = nil
	}
	h.stateMu.Unlock()
}

// RollbackChain rolls a specific chain back to an absolute block number.
func (s *Supervisor) RollbackChain(ctx context.Context, chainID uint64, toBlock uint64) error {
	s.mu.Lock()
	h := s.chains[chainID]
	s.mu.Unlock()
	if h == nil || h.embeddedCfg == nil {
		return nil
	}
	h.stateMu.Lock()
	defer h.stateMu.Unlock()

	// Stop polling and op-node
	if h.cancelPoll != nil {
		h.cancelPoll()
		h.cancelPoll = nil
	}
	if h.stopEmbeddedOpNode != nil {
		c, cancel := context.WithTimeout(ctx, 5*time.Second)
		_ = h.stopEmbeddedOpNode(c)
		cancel()
	}

	// Roll back EL head to the absolute target via pluggable implementation
	if err := rollbackEL(ctx, h.embeddedCfg.l2UserRPC, toBlock); err != nil {
		return err
	}

	// Restart embedded op-node and polling
	userRPC, stopFn2, err := s.StartEmbeddedOpNode(h.embeddedCfg.l1RPC, h.embeddedCfg.beaconAddr, h.embeddedCfg.l2AuthRPC, h.embeddedCfg.jwtSecret, h.embeddedCfg.rcfg)
	if err != nil {
		return err
	}
	h.embeddedOpNodeUserRPC = userRPC
	h.stopEmbeddedOpNode = stopFn2
	ctxPoll, cancel := context.WithCancel(context.Background())
	h.cancelPoll = cancel

	// Dial clients for polling
	opNodeCli, err := opclient.NewRPC(ctxPoll, s.log, userRPC)
	if err != nil {
		cancel()
		return err
	}
	l2Cli, err := opclient.NewRPC(ctxPoll, s.log, h.embeddedCfg.l2UserRPC)
	if err != nil {
		cancel()
		return err
	}
	l2, err := sources.NewL2Client(l2Cli, s.log, nil, sources.L2ClientDefaultConfig(h.embeddedCfg.rcfg, true))
	if err != nil {
		cancel()
		return err
	}
	roll := sources.NewRollupClient(opNodeCli)

	go func() {
		ticker := time.NewTicker(h.embeddedCfg.interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctxPoll.Done():
				return
			case <-ticker.C:
				st, err := roll.SyncStatus(ctxPoll)
				if err != nil || st == nil {
					s.log.Warn("poll: sync status error", "err", err)
					continue
				}
				localSafe := st.LocalSafeL2
				var crossSafe any
				if h.crossDB != nil {
					if pair, err := h.crossDB.Last(); err == nil {
						crossSafe = pair.Derived
					}
				}
				s.log.Info("poll: heads", "chain", chainID, "unsafe", st.UnsafeL2, "local_safe", localSafe, "safe", st.SafeL2, "finalized", st.FinalizedL2, "cross_safe", crossSafe)
				if localSafe.Number == 0 {
					continue
				}
				if _, _, err := l2.FetchReceiptsByNumber(ctxPoll, localSafe.Number); err != nil {
					s.log.Debug("poll: fetch receipts", "chain", chainID, "num", localSafe.Number, "err", err)
				}
			}
		}
	}()
	return nil
}
