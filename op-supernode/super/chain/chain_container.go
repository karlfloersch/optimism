package chain

import (
	"context"
	"sync"
	"time"

	logsdb "github.com/ethereum-optimism/optimism/op-supervisor/supervisor/backend/db/logs"
)

// ChainContainer interface defines the basic operations for a chain container
type ChainContainer interface {
	// VirtualNode returns the virtual node RPC endpoint
	VirtualNode() string
}

// AdminChainContainer interface extends ChainContainer with administrative operations
type AdminChainContainer interface {
	ChainContainer
	// Reset resets the chain container state
	Reset() error
	AsChainContainer() ChainContainer
}

// ChainContainerImpl tracks the per-chain state (virtual op-node lifecycle, DBs, and pollers).
type ChainContainerImpl struct {
	StateMu sync.Mutex

	// runtime state
	VirtualOpNodeUserRPC string
	StopVirtualOpNode    func(ctx context.Context) error
	CancelPoll           context.CancelFunc
	Started              time.Time

	// config for restart/rollback
	VirtualCfg *VirtualNodeConfig

	// v1 DBs per chain
	LogsDB *logsdb.DB
	// Note: localDB and crossDB removed in v2 - they were never written to and always returned empty data
}

// VirtualNode returns the virtual node RPC endpoint
func (c *ChainContainerImpl) VirtualNode() string {
	c.StateMu.Lock()
	defer c.StateMu.Unlock()
	return c.VirtualOpNodeUserRPC
}

// Reset resets the chain container state
func (c *ChainContainerImpl) Reset() error {
	c.StateMu.Lock()
	defer c.StateMu.Unlock()

	// Stop any running operations
	if c.CancelPoll != nil {
		c.CancelPoll()
		c.CancelPoll = nil
	}

	if c.StopVirtualOpNode != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = c.StopVirtualOpNode(ctx)
		c.StopVirtualOpNode = nil
	}

	// Reset state
	c.VirtualOpNodeUserRPC = ""
	c.Started = time.Time{}

	return nil
}

func (c *ChainContainerImpl) AsChainContainer() ChainContainer {
	return c
}

// NewChainContainer creates a new chain container that implements both interfaces
func NewChainContainer() *ChainContainerImpl {
	return &ChainContainerImpl{}
}

// Ensure ChainContainerImpl implements both interfaces
var _ ChainContainer = (*ChainContainerImpl)(nil)
var _ AdminChainContainer = (*ChainContainerImpl)(nil)
