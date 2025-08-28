package supervisor

import (
	"context"
	"sync"
	"time"

	"github.com/ethereum-optimism/optimism/op-node/rollup"
	"github.com/ethereum-optimism/optimism/op-service/eth"
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

// AddChain starts a chainHandler with virtual node for the given rollup config and RPCs.
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

	// Open per-chain DBs (logs/local/cross). Cross may not be used yet but is harmless to open.
	logsDB, localDB, crossDB, err := s.openChainDBs(s.log, chainID, s.getDataDir())
	if err != nil {
		// Stop the embedded op-node before returning the error
		_ = stopFn(context.Background())
		return 0, err
	}
	h.logsDB = logsDB
	h.localDB = localDB
	h.crossDB = crossDB

	// Register handle
	s.MarkChainActive(eth.ChainIDFromUInt64(chainID))
	s.mu.Lock()
	if s.chains == nil {
		s.chains = make(map[uint64]*ChainHandle)
	}
	s.chains[chainID] = h
	s.mu.Unlock()

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
	//go s.startChainPolling(ctxPoll, h, roll, l2, chainID)
	return nil
}
