package cross

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"

	"github.com/ethereum/go-ethereum/log"

	opclient "github.com/ethereum-optimism/optimism/op-service/client"
	"github.com/ethereum-optimism/optimism/op-service/eth"
	"github.com/ethereum-optimism/optimism/op-service/sources"
	"github.com/ethereum-optimism/optimism/op-supernode/super/activities"
	"github.com/ethereum-optimism/optimism/op-supernode/super/chain"
)

// ChainDirectory represents a mapping of chain IDs to chain containers
type ChainDirectory map[uint64]*chain.ChainContainerImpl

// CrossService handles cross-chain operations and coordination
type CrossService struct {
	log    log.Logger
	mu     sync.Mutex
	chains ChainDirectory

	// L1 scope label used for cross-safe L1 confirmation depth gating (default: eth.Safe; tests may override to eth.Unsafe)
	l1ScopeLabel eth.BlockLabel

	// per-instance data directory for DBs
	dataDir string

	// denylist
	denylist *DenylistStore

	// crossSafeHistory is the global cross-safe timestamp history (monotonic, non-decreasing)
	crossSafeHistory []crossSafeMD

	// crossSafeHistoryFile is the path to the file where crossSafeHistory is persisted
	crossSafeHistoryFile string

	// testability hooks
	fetchSyncStatus func(ctx context.Context, rpc string) (*eth.SyncStatus, error)
	rollbackFn      func(ctx context.Context, chainID uint64, toBlock uint64) error

	// activity control
	stopCh chan struct{}
	done   chan struct{}
}

// NewCrossService creates a new CrossService instance
func NewCrossService(logger log.Logger, chains ChainDirectory, dataDir string) *CrossService {
	s := &CrossService{
		log:          logger.New("service", "cross"),
		chains:       chains,
		dataDir:      dataDir,
		l1ScopeLabel: defaultScopeLabel(),
		stopCh:       make(chan struct{}),
		done:         make(chan struct{}),
	}

	// default fetcher dials the op-node and returns SyncStatus
	s.fetchSyncStatus = func(ctx context.Context, rpc string) (*eth.SyncStatus, error) {
		cli, err := opclient.NewRPC(ctx, s.log, rpc)
		if err != nil {
			return nil, err
		}
		defer cli.Close()
		roll := sources.NewRollupClient(cli)
		return roll.SyncStatus(ctx)
	}

	// initialize denylist under data dir
	s.denylist = NewDenylistStore(filepath.Join(s.dataDir, "denylist.json"))
	// initialize cross-safe history file path
	s.crossSafeHistoryFile = filepath.Join(s.dataDir, "crossSafeHistory.json")
	// load existing cross-safe history if available
	if err := s.loadCrossSafeHistory(); err != nil {
		s.log.Warn("failed to load cross-safe history", "err", err)
	}

	return s
}

func (s *CrossService) StartActivity(ctx context.Context) error {
	go s.ProgressCrossSafe()
	return nil
}

func (s *CrossService) StopActivity(ctx context.Context) error {
	close(s.stopCh)
	<-s.done
	return nil
}

// Denylisted checks if a block hash is denylisted for a given chain
func (s *CrossService) Denylisted(chainID uint64, id string) bool {
	return s.denylist != nil && id != "" && s.denylist.Has(chainID, id)
}

// Finalized returns the finalized L2 block for a given chain
func (s *CrossService) Finalized(ctx context.Context, chainID uint64) (eth.BlockID, error) {
	s.mu.Lock()
	container := s.chains[chainID]
	s.mu.Unlock()

	if container == nil || container.VirtualOpNodeUserRPC == "" {
		return eth.BlockID{}, fmt.Errorf("chain %d not found or no op-node RPC", chainID)
	}

	st, err := s.fetchSyncStatus(ctx, container.VirtualOpNodeUserRPC)
	if err != nil || st == nil {
		return eth.BlockID{}, fmt.Errorf("failed to fetch sync status: %w", err)
	}

	return st.FinalizedL2.ID(), nil
}

// SetRollbackFn sets the rollback function for testing
func (s *CrossService) SetRollbackFn(fn func(ctx context.Context, chainID uint64, toBlock uint64) error) {
	s.rollbackFn = fn
}

// UpdateChains updates the chain directory
func (s *CrossService) UpdateChains(chains ChainDirectory) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.chains = chains
}

// AddToDenylist adds an entry to the denylist (for testing)
func (s *CrossService) AddToDenylist(chainID uint64, timestamp uint64, hash string) error {
	if s.denylist == nil {
		return fmt.Errorf("denylist not initialized")
	}
	return s.denylist.Add(chainID, timestamp, hash)
}

var _ activities.Activity = (*CrossService)(nil)
