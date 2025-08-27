package supervisor

import (
	"context"
	"sync"
	"time"

	"github.com/ethereum-optimism/optimism/op-node/rollup"
	opclient "github.com/ethereum-optimism/optimism/op-service/client"
	"github.com/ethereum-optimism/optimism/op-service/eth"
	"github.com/ethereum-optimism/optimism/op-service/sources"
	"github.com/ethereum-optimism/optimism/op-supervisor-v2/supervisor/virtual_node"
	fromda "github.com/ethereum-optimism/optimism/op-supervisor/supervisor/backend/db/fromda"
	logsdb "github.com/ethereum-optimism/optimism/op-supervisor/supervisor/backend/db/logs"
)

// ChainHandle tracks the per-chain state (embedded op-node lifecycle, DBs, and pollers).
type ChainHandle struct {
	stateMu sync.Mutex

	// runtime state
	embeddedOpNodeUserRPC string
	stopEmbeddedOpNode    func(ctx context.Context) error
	cancelPoll            context.CancelFunc
	started               time.Time

	// config for restart/rollback
	virtualCfg *virtual_node.VirtualNodeConfig

	// v1 DBs per chain
	logsDB  *logsdb.DB
	localDB *fromda.DB
	crossDB *fromda.DB
}

// startChainPolling starts the main polling loop for a chain with full supervisor functionality.
func (s *Supervisor) startChainPolling(ctxPoll context.Context, h *ChainHandle, roll *sources.RollupClient, l2 *sources.L2Client, chainID uint64) {
	ticker := time.NewTicker(h.virtualCfg.Interval)
	defer ticker.Stop()

	// lazy L1 client for ingest
	l1Cli, _ := opclient.NewRPC(ctxPoll, s.log, h.virtualCfg.L1RPC)
	var l1 *sources.L1Client
	if l1Cli != nil {
		l1, _ = sources.NewL1Client(l1Cli, s.log, nil, sources.L1ClientDefaultConfig(h.virtualCfg.Rcfg, true, sources.RPCKindStandard))
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
	s.MarkChainActive(eth.ChainIDFromUInt64(chainID))

	for {
		select {
		case <-ctxPoll.Done():
			return
		case <-ticker.C:
			// Ensure L1 client is initialized (may fail early before L1 comes up)
			l1Cli, l1 = s.EnsureL1Client(ctxPoll, l1Cli, l1, h.virtualCfg.L1RPC, h.virtualCfg.Rcfg)

			// Get sync status from rollup client
			syncStatus, err := roll.SyncStatus(ctxPoll)
			if err != nil || syncStatus == nil {
				s.log.Warn("poll: sync status error", "err", err)
				continue
			}
			s.log.Info("poll: heads",
				"chain", chainID,
				"unsafe", syncStatus.UnsafeL2,
				"local_safe", syncStatus.LocalSafeL2,
				"safe", syncStatus.SafeL2,
				"finalized", syncStatus.FinalizedL2)

			target := syncStatus.LocalSafeL2
			if target.Number == 0 {
				continue
			}
			// Ingest and optionally seed/debug if there is new work
			if h.logsDB != nil && h.localDB != nil && l1 != nil {
				s.IngestToLocalSafe(ctxPoll, h, l1, l2, h.virtualCfg.Rcfg, chainID, target.Number)
				s.SeedLocalIfEmpty(ctxPoll, h, l1, l2, h.virtualCfg.Rcfg, chainID, target.Number)
				s.DebugIngestHeads(h, chainID)
			}
			// bootstrap cross DB once from latest local derived so cross-safe can start progressing
			if h.crossDB != nil && h.localDB != nil {
				s.BootstrapCrossIfEmpty(ctxPoll, h, l1, l2, h.virtualCfg.Rcfg, chainID)
			}
			// drive one step of cross-safe update
			if h.logsDB != nil && h.localDB != nil && h.crossDB != nil {
				adapter := s.newCrosssafeAdapter(h, l1, l2, h.virtualCfg.Rcfg, chainID)
				s.RunCrossSafeStep(ctxPoll, adapter, chainID)
			}
		}
	}
}

// AddChain starts an embedded op-node and polling for the given rollup config and RPCs.
// Returns the L2 chain ID as the handle key.
func (s *Supervisor) AddChain(l1RPC string, beaconAddr string, l2AuthRPC string, l2UserRPC string, jwtSecret [32]byte, rcfg *rollup.Config, interval time.Duration, confirmDepth uint64) (uint64, error) {
	chainID := rcfg.L2ChainID.Uint64()

	// Start embedded op-node
	userRPC, stopFn, err := virtual_node.StartVirtualNode(l1RPC, beaconAddr, l2AuthRPC, jwtSecret, rcfg, s.log)
	if err != nil {
		return 0, err
	}

	h := &ChainHandle{
		embeddedOpNodeUserRPC: userRPC,
		stopEmbeddedOpNode:    stopFn,
		virtualCfg: &virtual_node.VirtualNodeConfig{
			L1RPC:        l1RPC,
			BeaconAddr:   beaconAddr,
			L2AuthRPC:    l2AuthRPC,
			L2UserRPC:    l2UserRPC,
			JwtSecret:    jwtSecret,
			Rcfg:         rcfg,
			Interval:     interval,
			ConfirmDepth: confirmDepth,
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

	go s.startChainPolling(ctxPoll, h, roll, l2, chainID)

	// Register handle
	s.mu.Lock()
	if s.chains == nil {
		s.chains = make(map[uint64]*ChainHandle)
	}
	s.chains[chainID] = h
	s.mu.Unlock()

	// Finalized runner removed; cross-safe advancement is driven by per-chain pollers

	return chainID, nil
}

// getCrossFinalized returns the DB-backed min cross-safe height (0 if none).
func (s *Supervisor) getCrossFinalized() uint64 { return s.crossFinalizedFromDBOrFallback() }

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
	if h == nil || h.virtualCfg == nil {
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
	if err := rollbackEL(ctx, h.virtualCfg.L2UserRPC, toBlock); err != nil {
		return err
	}

	// Restart embedded op-node and polling
	userRPC, stopFn2, err := virtual_node.StartVirtualNode(h.virtualCfg.L1RPC, h.virtualCfg.BeaconAddr, h.virtualCfg.L2AuthRPC, h.virtualCfg.JwtSecret, h.virtualCfg.Rcfg, s.log)
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
	l2Cli, err := opclient.NewRPC(ctxPoll, s.log, h.virtualCfg.L2UserRPC)
	if err != nil {
		cancel()
		return err
	}
	l2, err := sources.NewL2Client(l2Cli, s.log, nil, sources.L2ClientDefaultConfig(h.virtualCfg.Rcfg, true))
	if err != nil {
		cancel()
		return err
	}
	roll := sources.NewRollupClient(opNodeCli)

	go s.startChainPolling(ctxPoll, h, roll, l2, chainID)
	return nil
}
