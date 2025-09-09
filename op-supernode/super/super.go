package super

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/log"

	"github.com/ethereum-optimism/optimism/op-node/rollup"
	opclient "github.com/ethereum-optimism/optimism/op-service/client"
	"github.com/ethereum-optimism/optimism/op-service/eth"
	"github.com/ethereum-optimism/optimism/op-service/sources"
	"github.com/ethereum-optimism/optimism/op-supernode/super/activities/cross"
	logsdb "github.com/ethereum-optimism/optimism/op-supervisor/supervisor/backend/db/logs"
)

type Super struct {
	log log.Logger
	mu  sync.Mutex

	// if true, HTTP handler exposes an /opnode/ reverse proxy to the virtual op-node user RPC
	enableOpNodeProxy bool

	chains   cross.ChainDirectory
	chainsMu sync.Mutex

	// L1 scope label used for cross-safe L1 confirmation depth gating (default: eth.Safe; tests may override to eth.Unsafe)
	l1ScopeLabel eth.BlockLabel

	// per-instance data directory for DBs
	dataDir string

	// cross-validation activity
	crossService *cross.CrossService

	// testability hooks
	fetchSyncStatus func(ctx context.Context, rpc string) (*eth.SyncStatus, error)
	rollbackFn      func(ctx context.Context, chainID uint64, toBlock uint64) error
}

// ============================================================================
// Package-Level Functions & Constructor
// ============================================================================

// defaultScopeLabel returns the default L1 scope label
// it can be overridden via env SV2_L1_SCOPE
func defaultScopeLabel() eth.BlockLabel {
	switch strings.ToLower(os.Getenv("SV2_L1_SCOPE")) {
	case "unsafe":
		return eth.Unsafe
	case "safe":
		return eth.Safe
	case "finalized":
		return eth.Finalized
	}
	return eth.Safe
}

func NewSuper(l log.Logger) *Super {
	s := &Super{log: l.New("service", "super")}
	// initialize shared linker state
	s.l1ScopeLabel = defaultScopeLabel()

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

	// rollback indirection for tests
	s.rollbackFn = s.RollbackChain

	// unique temp dir per instance (can be overridden via SetDataDir or CLI)
	s.dataDir = fmt.Sprintf("%s/sv2-%d-%d", os.TempDir(), os.Getpid(), time.Now().UnixNano())

	// initialize chains directory
	s.chains = make(cross.ChainDirectory)

	// initialize cross-validation service
	s.crossService = cross.NewCrossService(s.log, s.chains, s.dataDir)
	s.crossService.SetRollbackFn(s.rollbackFn)

	// start the cross-validation activity
	go s.crossService.StartActivity(context.Background())

	return s
}

// openLogsDB initializes the logs DB for a chain.
func (s *Super) openLogsDB(logger log.Logger, chainID uint64, dataDir string) (*logsdb.DB, error) {
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

// ============================================================================
// Configuration & Lifecycle Management
// ============================================================================

// getDataDir returns the base data directory for chain DBs
func (s *Super) getDataDir() string { return s.dataDir }

// SetDataDir overrides the base data directory for chain DBs and cross-validation persistence.
// Should be called before starting any chains or HTTP server.
func (s *Super) SetDataDir(dir string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if dir == "" {
		return
	}
	s.dataDir = dir

	// recreate cross service with new data directory
	if s.crossService != nil {
		s.crossService.StopActivity(context.Background())
	}
	s.crossService = cross.NewCrossService(s.log, s.chains, s.dataDir)
	s.crossService.SetRollbackFn(s.rollbackFn)
}

func (s *Super) getL1ScopeLabel() eth.BlockLabel {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.l1ScopeLabel
}

// SetL1ScopeLabel overrides the L1 scope label (e.g., eth.Unsafe in tests).
func (s *Super) SetL1ScopeLabel(label eth.BlockLabel) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.l1ScopeLabel = label
}

// EnableOpNodeProxy toggles the /opnode/ reverse proxy in the HTTP handler.
func (s *Super) EnableOpNodeProxy(v bool) { s.enableOpNodeProxy = v }

func (s *Super) Stop() {
	// Stop cross service
	if s.crossService != nil {
		s.crossService.StopActivity(context.Background())
	}

	// Stop all chains
	s.mu.Lock()
	chains := make(cross.ChainDirectory)
	for id, container := range s.chains {
		chains[id] = container
	}
	s.mu.Unlock()

	for chainID := range chains {
		s.RemoveChain(chainID)
	}
}

// ============================================================================
// Client Management
// ============================================================================

// EnsureL1Client lazily initializes the L1 client using the given RPC URL.
func (s *Super) EnsureL1Client(ctx context.Context, l1Cli opclient.RPC, l1 *sources.L1Client, l1RPC string, rcfg *rollup.Config) (opclient.RPC, *sources.L1Client) {
	if l1 != nil {
		return l1Cli, l1
	}
	if l1Cli == nil {
		if c, e := opclient.NewRPC(ctx, s.log, l1RPC); e == nil {
			l1Cli = c
		}
	}
	if l1Cli != nil {
		if l1Client, e := sources.NewL1Client(l1Cli, s.log, nil, sources.L1ClientDefaultConfig(rcfg, true, sources.RPCKindStandard)); e == nil {
			l1 = l1Client
		}
	}
	return l1Cli, l1
}

// crossFinalizedFromDBOrFallback returns 0 since cross DBs were removed in v2.
// Kept for API compatibility.
func (s *Super) crossFinalizedFromDBOrFallback() uint64 {
	return 0
}
