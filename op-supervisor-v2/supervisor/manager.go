package supervisor

import (
    "context"
    "fmt"
    "os"
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

// chainHandle tracks the managed state for a single chain.
type chainHandle struct {
	stateMu sync.Mutex

	// runtime state
	managedOpNodeUserRPC string
	stopManagedOpNode    func(ctx context.Context) error
	cancelPoll           context.CancelFunc
	started              time.Time

	// config for restart/rollback
	managedCfg *managedConfig

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
	userRPC, stopFn, err := s.StartManagedOpNode(l1RPC, beaconAddr, l2AuthRPC, jwtSecret, rcfg)
	if err != nil {
		return 0, err
	}

	h := &chainHandle{
		managedOpNodeUserRPC: userRPC,
		stopManagedOpNode:    stopFn,
		managedCfg: &managedConfig{
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
		l1Cli, _ := opclient.NewRPC(ctxPoll, s.log, h.managedCfg.l1RPC)
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
				if l1 == nil {
					if l1Cli == nil {
						if c, e := opclient.NewRPC(ctxPoll, s.log, h.managedCfg.l1RPC); e == nil {
							l1Cli = c
						}
					}
					if l1Cli != nil && l1 == nil {
						if l, e := sources.NewL1Client(l1Cli, s.log, nil, sources.L1ClientDefaultConfig(rcfg, true, sources.RPCKindStandard)); e == nil {
							l1 = l
						}
					}
				}
				// prefer local-safe when available, but fall back to unsafe in sequencer-only test mode
				target := st.LocalSafeL2
				if target.Number == 0 {
					target = st.UnsafeL2
				}
				s.log.Info("poll: heads", "chain", chainID, "unsafe", st.UnsafeL2, "local_safe", st.LocalSafeL2, "safe", st.SafeL2, "finalized", st.FinalizedL2)
				if target.Number == 0 {
					continue
				}
				// ingest from last logs height+1 up to target.Number
				if h.logsDB != nil && h.localDB != nil && l1 != nil {
					last, ok := h.logsDB.LatestSealedBlock()
					var start uint64 = 0
					if ok {
						if last.Number >= target.Number { // nothing new
							goto crosssafe
						}
						start = last.Number + 1
					}
					s.log.Info("ingest: range", "chain", chainID, "start", start, "end", target.Number)
					if err := ingestRange(ctxPoll, l1, l2, h.logsDB, h.localDB, h.crossDB, sources.L2ClientDefaultConfig(rcfg, true), start, target.Number); err != nil {
						s.log.Warn("ingest error", "err", err)
					}
					// minimal seeding: if local DB is still empty, seed the first derived entry from current unsafe head
					if h.localDB.IsEmpty() {
						// try to map current target (unsafe fallback) to derived refs
						env, err := l2.PayloadByNumber(ctxPoll, target.Number)
						if err == nil {
							if br, derr := derive.PayloadToBlockRef(rcfg, env.ExecutionPayload); derr == nil {
								l1Ref, e1 := l1.BlockRefByNumber(ctxPoll, br.L1Origin.Number)
								if e1 == nil && l1Ref.Hash == br.L1Origin.Hash {
									_ = h.localDB.AddDerived(l1Ref, br.BlockRef(), types.RevisionAny)
									s.log.Info("seed: local derived from target", "chain", chainID, "l1", l1Ref, "l2", br.BlockRef())
								}
							}
						}
					}
					// debug heads after ingest
					if blk, ok2 := h.logsDB.LatestSealedBlock(); ok2 {
						s.log.Info("ingest: logs head", "chain", chainID, "num", blk.Number)
					}
					if pair, err2 := h.localDB.Last(); err2 == nil {
						s.log.Info("ingest: local head", "chain", chainID, "l1", pair.Source, "l2", pair.Derived)
					} else {
						s.log.Info("ingest: local head err", "chain", chainID, "err", err2)
					}
					if h.crossDB != nil {
						if pair, err2 := h.crossDB.Last(); err2 == nil {
							s.log.Info("ingest: cross head", "chain", chainID, "l1", pair.Source, "l2", pair.Derived)
						} else {
							s.log.Info("ingest: cross head err", "chain", chainID, "err", err2)
						}
					}
				}
				// bootstrap cross DB once from latest local derived so cross-safe can start progressing
				if h.crossDB != nil && h.localDB != nil {
					if h.crossDB.IsEmpty() {
						if pair, err := h.localDB.Last(); err == nil {
							// fetch full refs
							l1Ref, err1 := l1.BlockRefByNumber(ctxPoll, pair.Source.Number)
							env, err2 := l2.PayloadByNumber(ctxPoll, pair.Derived.Number)
							var dref eth.BlockRef
							if err2 == nil {
								if br, derr := derive.PayloadToBlockRef(rcfg, env.ExecutionPayload); derr == nil {
									dref = br.BlockRef()
								}
							}
							if err1 == nil && dref.Number == pair.Derived.Number {
								_ = h.crossDB.AddDerived(l1Ref, dref, types.RevisionAny)
								s.log.Info("bootstrap cross DB from local", "chain", chainID, "l1", l1Ref, "l2", dref)
							}
						}
					}
				}
			crosssafe:
				// drive one step of cross-safe update
				if h.logsDB != nil && h.localDB != nil && h.crossDB != nil {
					adapter := &crosssafeAdapter{
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
						l1ConfirmDepth: h.managedCfg.confirmDepth,
						l1ScopeLabel:   s.getL1ScopeLabel(),
					}
					s.log.Info("crosssafe: run", "chain", chainID)
					if err := adapter.runCrossSafeOnce(s.log, s.getLinker()); err != nil {
						s.log.Warn("crosssafe: error", "chain", chainID, "err", err)
					}
					if cs, err := h.crossDB.Last(); err == nil {
						s.log.Info("crosssafe: head", "chain", chainID, "derived", cs.Derived)
					} else {
						s.log.Warn("crosssafe: last error", "chain", chainID, "err", err)
					}
				}
			}
		}
	}()

	// Start finalized runner if this is the first chain registered
	s.maybeStartFinalizedRunner()

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

	return chainID, nil
}

// getCrossFinalized returns the last computed cross-finalized height.
func (s *Supervisor) getCrossFinalized() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.crossFinalized
}

// maybeStartFinalizedRunner starts a background loop that computes the minimum FinalizedL2 height across chains.
// It is intentionally simple and read-only; later we will plug in checkers and denylist/rollback execution.
func (s *Supervisor) maybeStartFinalizedRunner() {
	s.mu.Lock()
	already := s.cancelFinalized != nil
	hasChains := len(s.chains) > 0
	s.mu.Unlock()
	if already || !hasChains {
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	s.mu.Lock()
	s.cancelFinalized = cancel
	s.mu.Unlock()
    go func() {
        ticker := time.NewTicker(s.runnerInterval)
		defer ticker.Stop()
        // feature flag: disable proposals by default until wired
        enableProposals := strings.ToLower(os.Getenv("SV2_ENABLE_CHECKERS")) == "true"
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				var minFinalized uint64
				minFinalized = 0
				// compute min over all chains of FinalizedL2.Number
				s.mu.Lock()
                // also build a minimal snapshot per tick
                snap := Snapshot{PerChain: make(map[uint64]ChainSnapshot)}
				for cid, h := range s.chains {
					_ = cid
					// fetch rollup status best-effort
					h.stateMu.Lock()
					rpc := h.managedOpNodeUserRPC
					h.stateMu.Unlock()
					if rpc == "" {
						continue
					}
					// best-effort dial with timeout
                    func() {
                        ctx2, cancel2 := context.WithTimeout(ctx, 300*time.Millisecond)
                        defer cancel2()
                        st, err := s.fetchSyncStatus(ctx2, rpc)
						if err == nil && st != nil {
							num := st.FinalizedL2.Number
                            if num != 0 {
                                if minFinalized == 0 || num < minFinalized {
                                    minFinalized = num
                                }
                                snap.PerChain[cid] = ChainSnapshot{Finalized: num}
                            }
						}
					}()
				}
				s.mu.Unlock()
				if minFinalized != 0 {
					s.mu.Lock()
					s.crossFinalized = minFinalized
                    snap.CrossFinalized = minFinalized
					s.mu.Unlock()
				}
                if enableProposals && minFinalized != 0 {
                    // Evaluate registered checkers with the current snapshot
                    for _, chk := range s.getCheckers() {
                        if props, err := chk.Evaluate(ctx, snap); err != nil {
                            s.log.Warn("checker error", "err", err)
                        } else if len(props) > 0 {
                            // Execute proposals: add denylist + rollback
                            for _, p := range props {
                                if p.PayloadID != "" {
                                    _ = s.denylist.Add(p.ChainID, p.PayloadID)
                                }
                                if s.rollbackFn != nil && p.ToBlock > 0 && p.ChainID != 0 {
                                    if err := s.rollbackFn(ctx, p.ChainID, p.ToBlock); err != nil {
                                        s.log.Warn("rollback failed", "chain", p.ChainID, "to", p.ToBlock, "err", err)
                                    } else {
                                        s.log.Info("rollback executed", "chain", p.ChainID, "to", p.ToBlock, "reason", p.Reason)
                                    }
                                }
                            }
                        }
                    }
                }
			}
		}
	}()
}

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
	if h.stopManagedOpNode != nil {
		_ = h.stopManagedOpNode(context.Background())
		h.stopManagedOpNode = nil
	}
	h.stateMu.Unlock()
}

// RollbackChain rolls a specific chain back to an absolute block number.
func (s *Supervisor) RollbackChain(ctx context.Context, chainID uint64, toBlock uint64) error {
	s.mu.Lock()
	h := s.chains[chainID]
	s.mu.Unlock()
	if h == nil || h.managedCfg == nil {
		return nil
	}
	h.stateMu.Lock()
	defer h.stateMu.Unlock()

	// Stop polling and op-node
	if h.cancelPoll != nil {
		h.cancelPoll()
		h.cancelPoll = nil
	}
	if h.stopManagedOpNode != nil {
		c, cancel := context.WithTimeout(ctx, 5*time.Second)
		_ = h.stopManagedOpNode(c)
		cancel()
	}

	// Roll back EL head to the absolute target via pluggable implementation
	if err := rollbackEL(ctx, h.managedCfg.l2UserRPC, toBlock); err != nil {
		return err
	}

	// Restart managed op-node and polling
	userRPC, stopFn2, err := s.StartManagedOpNode(h.managedCfg.l1RPC, h.managedCfg.beaconAddr, h.managedCfg.l2AuthRPC, h.managedCfg.jwtSecret, h.managedCfg.rcfg)
	if err != nil {
		return err
	}
	h.managedOpNodeUserRPC = userRPC
	h.stopManagedOpNode = stopFn2
	ctxPoll, cancel := context.WithCancel(context.Background())
	h.cancelPoll = cancel

	// Dial clients for polling
	opNodeCli, err := opclient.NewRPC(ctxPoll, s.log, userRPC)
	if err != nil {
		cancel()
		return err
	}
	l2Cli, err := opclient.NewRPC(ctxPoll, s.log, h.managedCfg.l2UserRPC)
	if err != nil {
		cancel()
		return err
	}
	l2, err := sources.NewL2Client(l2Cli, s.log, nil, sources.L2ClientDefaultConfig(h.managedCfg.rcfg, true))
	if err != nil {
		cancel()
		return err
	}
	roll := sources.NewRollupClient(opNodeCli)

	go func() {
		ticker := time.NewTicker(h.managedCfg.interval)
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
				s.log.Info("poll: heads", "chain", chainID, "unsafe", st.UnsafeL2, "local_safe", localSafe, "safe", st.SafeL2, "finalized", st.FinalizedL2)
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
