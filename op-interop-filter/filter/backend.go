package filter

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/log"

	"github.com/ethereum-optimism/optimism/op-interop-filter/metrics"
	"github.com/ethereum-optimism/optimism/op-service/eth"
	"github.com/ethereum-optimism/optimism/op-supervisor/supervisor/types"
)

var (
	ErrFailsafeEnabled = errors.New("failsafe is enabled")
	ErrNotReady        = errors.New("service not ready, backfill in progress")
	ErrUnknownChain    = errors.New("unknown chain")
)

// Backend coordinates chain ingesters and handles the failsafe state
type Backend struct {
	log     log.Logger
	metrics metrics.Metricer
	cfg     *Config

	chains   map[eth.ChainID]*ChainIngester
	chainsMu sync.RWMutex

	failsafe atomic.Bool
}

// NewBackend creates a new Backend instance
func NewBackend(ctx context.Context, logger log.Logger, m metrics.Metricer, cfg *Config) (*Backend, error) {
	b := &Backend{
		log:     logger,
		metrics: m,
		cfg:     cfg,
		chains:  make(map[eth.ChainID]*ChainIngester),
	}

	// Create chain instances (but don't start them yet)
	for _, l2rpc := range cfg.L2RPCs {
		chainID := eth.ChainIDFromUInt64(l2rpc.ChainID)
		chain, err := NewChainIngester(ctx, logger, m, chainID, l2rpc.RPCURL, cfg)
		if err != nil {
			return nil, fmt.Errorf("failed to create chain %d: %w", l2rpc.ChainID, err)
		}
		b.chains[chainID] = chain
		logger.Info("Created chain ingester", "chainID", l2rpc.ChainID, "rpc", l2rpc.RPCURL)
	}

	return b, nil
}

// Start starts all chain ingesters
func (b *Backend) Start(ctx context.Context) error {
	b.log.Info("Starting backend", "chains", len(b.chains))
	for chainID, chain := range b.chains {
		if err := chain.Start(ctx, b.onReorg); err != nil {
			return fmt.Errorf("failed to start chain %s: %w", chainID, err)
		}
	}
	return nil
}

// Stop stops all chain ingesters
func (b *Backend) Stop(ctx context.Context) error {
	b.log.Info("Stopping backend")
	var result error
	for chainID, chain := range b.chains {
		if err := chain.Stop(ctx); err != nil {
			result = errors.Join(result, fmt.Errorf("failed to stop chain %s: %w", chainID, err))
		}
	}
	return result
}

// onReorg is called when a chain detects a reorg
func (b *Backend) onReorg(chainID eth.ChainID, err error) {
	b.log.Error("Reorg detected, enabling failsafe", "chainID", chainID, "err", err)
	b.failsafe.Store(true)
	b.metrics.RecordFailsafeEnabled(true)
	chainIDUint, _ := chainID.Uint64()
	b.metrics.RecordReorgDetected(chainIDUint)
}

// FailsafeEnabled returns whether failsafe is enabled
func (b *Backend) FailsafeEnabled() bool {
	return b.failsafe.Load()
}

// Ready returns whether all chains have finished backfill
func (b *Backend) Ready() bool {
	b.chainsMu.RLock()
	defer b.chainsMu.RUnlock()
	for _, chain := range b.chains {
		if !chain.Ready() {
			return false
		}
	}
	return true
}

// CheckAccessList validates the given access list entries
func (b *Backend) CheckAccessList(ctx context.Context, inboxEntries []common.Hash,
	minSafety types.SafetyLevel, execDescriptor types.ExecutingDescriptor) error {

	// Check failsafe first
	if b.FailsafeEnabled() {
		b.metrics.RecordCheckAccessList(false)
		return ErrFailsafeEnabled
	}

	// Check if we're ready (all chains backfilled)
	if !b.Ready() {
		b.metrics.RecordCheckAccessList(false)
		return ErrNotReady
	}

	// Parse and validate each access entry
	entries := inboxEntries
	for len(entries) > 0 {
		if err := ctx.Err(); err != nil {
			b.metrics.RecordCheckAccessList(false)
			return fmt.Errorf("context cancelled: %w", err)
		}

		remaining, access, err := types.ParseAccess(entries)
		if err != nil {
			b.metrics.RecordCheckAccessList(false)
			return fmt.Errorf("failed to parse access: %w", err)
		}
		entries = remaining

		// Get chain for this access entry
		b.chainsMu.RLock()
		chain, ok := b.chains[access.ChainID]
		b.chainsMu.RUnlock()
		if !ok {
			b.metrics.RecordCheckAccessList(false)
			return fmt.Errorf("%w: %s", ErrUnknownChain, access.ChainID)
		}

		// Validate via LogsDB
		if err := chain.Contains(access); err != nil {
			b.log.Debug("Access validation failed", "chainID", access.ChainID,
				"blockNum", access.BlockNumber, "logIdx", access.LogIndex, "err", err)
			b.metrics.RecordCheckAccessList(false)
			return types.ErrConflict
		}
	}

	b.metrics.RecordCheckAccessList(true)
	return nil
}
