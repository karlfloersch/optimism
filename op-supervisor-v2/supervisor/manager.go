package supervisor

import (
	"context"
	"sync"
	"time"

	"github.com/ethereum-optimism/optimism/op-node/rollup"
	opclient "github.com/ethereum-optimism/optimism/op-service/client"
	"github.com/ethereum-optimism/optimism/op-service/eth"
	"github.com/ethereum-optimism/optimism/op-service/sources"
	fromda "github.com/ethereum-optimism/optimism/op-supervisor/supervisor/backend/db/fromda"
	logsdb "github.com/ethereum-optimism/optimism/op-supervisor/supervisor/backend/db/logs"
	"github.com/ethereum-optimism/optimism/op-supervisor/supervisor/backend/reads"
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
			logs, local, cross, err := s.openChainDBs(s.log, chainID, "/tmp/sv2")
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
				localSafe := st.LocalSafeL2
				s.log.Info("poll: heads", "chain", chainID, "unsafe", st.UnsafeL2, "local_safe", localSafe, "safe", st.SafeL2, "finalized", st.FinalizedL2)
				if localSafe.Number == 0 {
					continue
				}
				// ingest from last logs height+1 up to localSafe.Number
				if h.logsDB != nil && h.localDB != nil && l1 != nil {
					last, ok := h.logsDB.LatestSealedBlock()
					var start uint64 = 0
					if ok {
						if last.Number >= localSafe.Number { // nothing new
							goto crosssafe
						}
						start = last.Number + 1
					}
					if err := ingestRange(ctxPoll, l1, l2, h.logsDB, h.localDB, sources.L2ClientDefaultConfig(rcfg, true), start, localSafe.Number); err != nil {
						s.log.Warn("ingest error", "err", err)
					}
				}
			crosssafe:
				// drive one step of cross-safe update
				if h.logsDB != nil && h.localDB != nil && h.crossDB != nil {
					adapter := &crosssafeAdapter{
						logger:         s.log,
						chainID:        eth.ChainIDFromUInt64(chainID),
						logs:           h.logsDB,
						local:          h.localDB,
						cross:          h.crossDB,
						reads:          reads.NewRegistry(s.log),
						l1:             l1,
						l2:             l2,
						addDenylist:    func(cid uint64, id string) error { return s.denylist.Add(cid, id) },
						rollback:       s.RollbackChain,
						l1ConfirmDepth: h.managedCfg.confirmDepth,
					}
					_ = adapter.runCrossSafeOnce(s.log, s.getLinker())
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

	return chainID, nil
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
