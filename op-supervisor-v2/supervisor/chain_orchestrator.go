package supervisor

import (
	"context"
	"sync"
	"time"

	"github.com/ethereum-optimism/optimism/op-supervisor-v2/supervisor/virtual_node"
	logsdb "github.com/ethereum-optimism/optimism/op-supervisor/supervisor/backend/db/logs"
	"github.com/ethereum-optimism/optimism/op-supervisor/supervisor/backend/reads"
)

// ChainContainer tracks the per-chain state (virtual op-node lifecycle, DBs, and pollers).
type ChainContainer struct {
	stateMu sync.Mutex

	// runtime state
	virtualOpNodeUserRPC string
	stopVirtualOpNode    func(ctx context.Context) error
	cancelPoll           context.CancelFunc
	started              time.Time

	// config for restart/rollback
	virtualCfg *virtual_node.VirtualNodeConfig

	// v1 DBs per chain
	logsDB *logsdb.DB
	// Note: localDB and crossDB removed in v2 - they were never written to and always returned empty data
}

// AddChain starts a chainHandler with virtual node for the given config.
// Returns the L2 chain ID as the container key.
func (s *Supervisor) AddChain(vCfg *virtual_node.VirtualNodeConfig) (uint64, error) {
	chainID := vCfg.Rcfg.L2ChainID.Uint64()

	// Create chain-specific logger for supervisor operations
	chainLogger := s.log.New("chain", chainID)
	chainLogger.Info("adding chain to supervisor", "chain_id", chainID)

	// Start virtual op-node
	userRPC, stopFn, err := virtual_node.StartVirtualNode(vCfg, chainLogger)
	if err != nil {
		return 0, err
	}

	container := &ChainContainer{
		virtualOpNodeUserRPC: userRPC,
		stopVirtualOpNode:    stopFn,
		virtualCfg:           vCfg,
		started:              time.Now(),
	}

	// Open logs DB for chain
	logsDB, err := s.openLogsDB(chainLogger, chainID, s.getDataDir())
	if err != nil {
		// Stop the virtual op-node before returning the error
		_ = stopFn(context.Background())
		return 0, err
	}
	container.logsDB = logsDB

	s.mu.Lock()
	if s.chains == nil {
		s.chains = make(map[uint64]*ChainContainer)
	}
	s.chains[chainID] = container
	s.mu.Unlock()

	chainLogger.Info("chain added successfully", "chain_id", chainID, "user_rpc", userRPC)
	return chainID, nil
}

// getCrossFinalized returns the DB-backed min cross-safe height (0 if none).
func (s *Supervisor) getCrossFinalized() uint64 { return s.crossFinalizedFromDBOrFallback() }

// RemoveChain stops and unregisters a chain by ID.
func (s *Supervisor) RemoveChain(chainID uint64) {
	chainLogger := s.log.New("chain", chainID)
	chainLogger.Info("removing chain from supervisor", "chain_id", chainID)

	s.mu.Lock()
	container := s.chains[chainID]
	delete(s.chains, chainID)
	s.mu.Unlock()
	if container == nil {
		chainLogger.Warn("remove chain skipped - container not found", "chain_id", chainID)
		return
	}
	container.stateMu.Lock()
	if container.cancelPoll != nil {
		container.cancelPoll()
		container.cancelPoll = nil
	}
	if container.stopVirtualOpNode != nil {
		_ = container.stopVirtualOpNode(context.Background())
		container.stopVirtualOpNode = nil
	}
	container.stateMu.Unlock()
	chainLogger.Info("chain removed successfully", "chain_id", chainID)
}

// RollbackChain rolls a specific chain back to an absolute block number.
func (s *Supervisor) RollbackChain(ctx context.Context, chainID uint64, toBlock uint64) error {
	chainLogger := s.log.New("chain", chainID)
	chainLogger.Info("rolling back chain", "chain_id", chainID, "to_block", toBlock)

	s.mu.Lock()
	container := s.chains[chainID]
	s.mu.Unlock()
	if container == nil || container.virtualCfg == nil {
		chainLogger.Warn("rollback skipped - missing container or config", "chain_id", chainID)
		return nil
	}
	container.stateMu.Lock()
	defer container.stateMu.Unlock()

	// Stop polling and op-node
	if container.cancelPoll != nil {
		container.cancelPoll()
		container.cancelPoll = nil
	}
	if container.stopVirtualOpNode != nil {
		c, cancel := context.WithTimeout(ctx, 5*time.Second)
		_ = container.stopVirtualOpNode(c)
		cancel()
	}

	// Attempt to record the soon-to-be-invalidated block (toBlock+1) in the denylist
	if container.logsDB != nil && s.denylist != nil {
		invalidNum := toBlock + 1
		if ref, _, _, err := container.logsDB.OpenBlock(invalidNum); err == nil {
			_ = s.denylist.Add(chainID, ref.Time, ref.Hash.Hex())
		}
	}

	// Roll back logsDB to the target block number
	if container.logsDB != nil {
		if ref, _, _, openErr := container.logsDB.OpenBlock(toBlock); openErr == nil {
			inv := reads.NewRegistry(s.log)
			_ = container.logsDB.Rewind(inv, ref.ID())
		}
	}

	// Roll back EL head to the absolute target via pluggable implementation
	if err := rollbackEL(ctx, container.virtualCfg.L2UserRPC, toBlock); err != nil {
		return err
	}

	// Restart virtual op-node and polling
	userRPC, stopFn2, err := virtual_node.StartVirtualNode(container.virtualCfg, chainLogger)
	if err != nil {
		return err
	}
	container.virtualOpNodeUserRPC = userRPC
	container.stopVirtualOpNode = stopFn2
	chainLogger.Info("chain rollback completed", "chain_id", chainID, "to_block", toBlock, "user_rpc", userRPC)
	//go s.startChainPolling(ctxPoll, h, roll, l2, chainID)
	return nil
}
