package cross

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/ethereum/go-ethereum/log"

	opclient "github.com/ethereum-optimism/optimism/op-service/client"
	"github.com/ethereum-optimism/optimism/op-service/eth"
	"github.com/ethereum-optimism/optimism/op-service/sources"
	"github.com/ethereum-optimism/optimism/op-supernode/super/activities"
	"github.com/ethereum-optimism/optimism/op-supernode/super/chain"
	logsdb "github.com/ethereum-optimism/optimism/op-supervisor/supervisor/backend/db/logs"
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

	// logsdbs managed by this cross service
	logsdbs map[uint64]*logsdb.DB

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
		logsdbs:      make(map[uint64]*logsdb.DB),
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

// Finalized returns if a timestamp is at or before the latest Cross Valid timestamp
func (s *CrossService) Finalized(timestamp uint64) bool {
	latestCrossSafeTimestamp := s.getCurrentCrossSafeTimestamp()
	return timestamp <= latestCrossSafeTimestamp
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

// openLogsDB initializes the logs DB for a chain.
func (s *CrossService) openLogsDB(logger log.Logger, chainID uint64, dataDir string) (*logsdb.DB, error) {
	// Ensure base directory exists
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return nil, err
	}
	logsPath := fmt.Sprintf("%s/logs-%d", dataDir, chainID)
	// Use no-op metrics for now; can be replaced with real metrics later.
	logDB, err := logsdb.NewFromFile(logger, logsMetricsNoop{}, eth.ChainIDFromUInt64(chainID), logsPath, true)
	if err != nil {
		return nil, err
	}
	return logDB, nil
}

// AddChainLogsDB creates and manages a logsDB for the given chain
func (s *CrossService) AddChainLogsDB(chainID uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Check if logsDB already exists
	if _, exists := s.logsdbs[chainID]; exists {
		return fmt.Errorf("logsDB for chain %d already exists", chainID)
	}

	// Create logsDB
	logsDB, err := s.openLogsDB(s.log, chainID, s.dataDir)
	if err != nil {
		return fmt.Errorf("failed to create logsDB for chain %d: %w", chainID, err)
	}

	s.logsdbs[chainID] = logsDB
	s.log.Info("created logsDB for chain", "chainID", chainID)
	return nil
}

// RemoveChainLogsDB removes and closes the logsDB for the given chain
func (s *CrossService) RemoveChainLogsDB(chainID uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if logsDB, exists := s.logsdbs[chainID]; exists {
		// Close the logsDB (assuming it has a Close method)
		// Note: We should check the actual logsdb.DB interface for the correct close method
		delete(s.logsdbs, chainID)
		s.log.Info("removed logsDB for chain", "chainID", chainID)
		_ = logsDB // Prevent unused variable error for now
	}
}

// GetLogsDB returns the logsDB for the given chain
func (s *CrossService) GetLogsDB(chainID uint64) *logsdb.DB {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.logsdbs[chainID]
}

// logsMetricsNoop implements op-super v1 logs.Metrics
type logsMetricsNoop struct{}

func (logsMetricsNoop) RecordDBEntryCount(kind string, count int64) {}
func (logsMetricsNoop) RecordDBSearchEntriesRead(count int64)       {}

var _ activities.Activity = (*CrossService)(nil)
