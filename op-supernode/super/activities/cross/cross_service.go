package cross

import (
	"context"

	"github.com/ethereum-optimism/optimism/op-supernode/super/activities"
	"github.com/ethereum-optimism/optimism/op-supernode/super/chain"
	"github.com/ethereum/go-ethereum/log"
)

// ChainDirectory represents a mapping of chain IDs to chain containers
type ChainDirectory map[uint64]*chain.ChainContainerImpl

// CrossService handles cross-chain operations and coordination
type CrossService struct {
	log    log.Logger
	chains ChainDirectory
}

// NewCrossService creates a new CrossService instance
func NewCrossService(logger log.Logger, chains ChainDirectory) *CrossService {
	return &CrossService{
		log:    logger.New("service", "cross"),
		chains: chains,
	}
}

func (s *CrossService) StartActivity(ctx context.Context) error {
	return nil
}

func (s *CrossService) StopActivity(ctx context.Context) error {
	return nil
}

var _ activities.Activity = (*CrossService)(nil)
