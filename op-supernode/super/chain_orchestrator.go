package super

import (
	"context"
	"time"

	"github.com/ethereum-optimism/optimism/op-supernode/super/activities/cross"
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

	container := chain.NewChainContainer()
	container.VirtualOpNodeUserRPC = userRPC
	container.StopVirtualOpNode = stopFn
	container.VirtualCfg = vCfg
	container.Started = time.Now()

	// Create logs DB for chain via cross service
	if err := s.crossService.AddChainLogsDB(chainID); err != nil {
		// Stop the virtual op-node before returning the error
		_ = stopFn(context.Background())
		return 0, err
	}

	s.mu.Lock()
	if s.chains == nil {
		s.chains = make(cross.ChainDirectory)
	}
	s.chains[chainID] = container
	s.mu.Unlock()

	// Update cross service with new chain directory
	if s.crossService != nil {
		s.crossService.UpdateChains(s.chains)
	}

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

	// Update cross service with new chain directory
	if s.crossService != nil {
		s.crossService.UpdateChains(s.chains)
		// Remove logsDB for this chain
		s.crossService.RemoveChainLogsDB(chainID)
	}

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

	// Roll back logsDB to the target block number via cross service
	if s.crossService != nil {
		if logsDB := s.crossService.GetLogsDB(chainID); logsDB != nil {
			// Attempt to record the soon-to-be-invalidated block (toBlock+1) in the denylist
			invalidNum := toBlock + 1
			if _, _, _, err := logsDB.OpenBlock(invalidNum); err == nil {
				// Access denylist through cross service (we'll need to add a method for this)
				// For now, we'll skip this functionality as it should be handled by the cross service internally
			}

			// Roll back logsDB to the target block number
			if blockRef, _, _, openErr := logsDB.OpenBlock(toBlock); openErr == nil {
				inv := reads.NewRegistry(s.log)
				_ = logsDB.Rewind(inv, blockRef.ID())
			}
		}
	}

	// Roll back EL head to the absolute target via container method
	if err := container.RollbackEL(ctx, toBlock); err != nil {
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
