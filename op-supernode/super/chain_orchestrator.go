package super

import (
	"context"
	"time"

	"github.com/ethereum-optimism/optimism/op-supernode/super/chain"
	"github.com/ethereum-optimism/optimism/op-supervisor/supervisor/backend/reads"
)

// AddChain starts a chainHandler with virtual node for the given config.
// Returns the L2 chain ID as the container key.
func (s *Super) AddChain(vCfg *chain.VirtualNodeConfig) (uint64, error) {
	chainID := vCfg.Rcfg.L2ChainID.Uint64()

	// Start virtual op-node
	userRPC, stopFn, err := chain.StartVirtualNode(vCfg, s.log)
	if err != nil {
		return 0, err
	}

	container := &chain.ChainContainer{
		VirtualOpNodeUserRPC: userRPC,
		StopVirtualOpNode:    stopFn,
		VirtualCfg:           vCfg,
		Started:              time.Now(),
	}

	// Open logs DB for chain
	logsDB, err := s.openLogsDB(s.log, chainID, s.getDataDir())
	if err != nil {
		// Stop the virtual op-node before returning the error
		_ = stopFn(context.Background())
		return 0, err
	}
	container.LogsDB = logsDB

	s.mu.Lock()
	if s.chains == nil {
		s.chains = make(map[uint64]*chain.ChainContainer)
	}
	s.chains[chainID] = container
	s.mu.Unlock()

	return chainID, nil
}

// getCrossFinalized returns the DB-backed min cross-safe height (0 if none).
func (s *Super) getCrossFinalized() uint64 { return s.crossFinalizedFromDBOrFallback() }

// RemoveChain stops and unregisters a chain by ID.
func (s *Super) RemoveChain(chainID uint64) {
	s.mu.Lock()
	container := s.chains[chainID]
	delete(s.chains, chainID)
	s.mu.Unlock()
	if container == nil {
		return
	}
	container.StateMu.Lock()
	if container.CancelPoll != nil {
		container.CancelPoll()
		container.CancelPoll = nil
	}
	if container.StopVirtualOpNode != nil {
		_ = container.StopVirtualOpNode(context.Background())
		container.StopVirtualOpNode = nil
	}
	container.StateMu.Unlock()
}

// RollbackChain rolls a specific chain back to an absolute block number.
func (s *Super) RollbackChain(ctx context.Context, chainID uint64, toBlock uint64) error {
	s.mu.Lock()
	container := s.chains[chainID]
	s.mu.Unlock()
	if container == nil || container.VirtualCfg == nil {
		return nil
	}
	container.StateMu.Lock()
	defer container.StateMu.Unlock()

	// Stop polling and op-node
	if container.CancelPoll != nil {
		container.CancelPoll()
		container.CancelPoll = nil
	}
	if container.StopVirtualOpNode != nil {
		c, cancel := context.WithTimeout(ctx, 5*time.Second)
		_ = container.StopVirtualOpNode(c)
		cancel()
	}

	// Attempt to record the soon-to-be-invalidated block (toBlock+1) in the denylist
	if container.LogsDB != nil && s.denylist != nil {
		invalidNum := toBlock + 1
		if ref, _, _, err := container.LogsDB.OpenBlock(invalidNum); err == nil {
			_ = s.denylist.Add(chainID, ref.Time, ref.Hash.Hex())
		}
	}

	// Roll back logsDB to the target block number
	if container.LogsDB != nil {
		if ref, _, _, openErr := container.LogsDB.OpenBlock(toBlock); openErr == nil {
			inv := reads.NewRegistry(s.log)
			_ = container.LogsDB.Rewind(inv, ref.ID())
		}
	}

	// Roll back EL head to the absolute target via pluggable implementation
	if err := rollbackEL(ctx, container.VirtualCfg.L2UserRPC, toBlock); err != nil {
		return err
	}

	// Restart virtual op-node and polling
	userRPC, stopFn2, err := chain.StartVirtualNode(container.VirtualCfg, s.log)
	if err != nil {
		return err
	}
	container.VirtualOpNodeUserRPC = userRPC
	container.StopVirtualOpNode = stopFn2
	//go s.startChainPolling(ctxPoll, h, roll, l2, chainID)
	return nil
}
