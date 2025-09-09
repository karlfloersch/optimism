package chain

import (
	"context"
	"fmt"
	"sync"
	"time"

	opclient "github.com/ethereum-optimism/optimism/op-service/client"
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
	// RollbackEL rolls back the execution layer to a specific block number
	RollbackEL(ctx context.Context, target uint64) error
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

	// Note: LogsDB moved to CrossService in v2 for better separation of concerns
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

// RollbackEL is the high-level EL rollback helper used by the chain container.
// TODO: Replace the debug_setHead-based implementation with an Engine API
// forkchoice update (engine_forkchoiceUpdated) against the authenticated EL RPC
// to support ELs like reth without changing finalized.
func (c *ChainContainerImpl) RollbackEL(ctx context.Context, target uint64) error {
	c.StateMu.Lock()
	defer c.StateMu.Unlock()

	if c.VirtualCfg == nil {
		return fmt.Errorf("no virtual config available for rollback")
	}

	return c.rollbackELWithDebugSetHead(ctx, c.VirtualCfg.L2UserRPC, target)
}

// rollbackELWithDebugSetHead rolls the EL back by N blocks using debug_setHead via the user RPC.
func (c *ChainContainerImpl) rollbackELWithDebugSetHead(ctx context.Context, l2UserRPC string, target uint64) error {
	cli, err := opclient.NewRPC(ctx, nil, l2UserRPC)
	if err != nil {
		return fmt.Errorf("dial l2 user rpc: %w", err)
	}
	defer cli.Close()

	var ok bool
	if err := cli.CallContext(ctx, &ok, "debug_setHead", fmt.Sprintf("0x%x", target)); err != nil {
		return fmt.Errorf("debug_setHead: %w", err)
	}
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
