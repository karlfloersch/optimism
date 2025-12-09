package filter

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/ethereum/go-ethereum/log"

	"github.com/ethereum-optimism/optimism/op-interop-filter/metrics"
	"github.com/ethereum-optimism/optimism/op-service/eth"
	"github.com/ethereum-optimism/optimism/op-supervisor/supervisor/types"
)

var (
	ErrUnknownChain = errors.New("unknown chain")
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
	for _, rpcURL := range cfg.L2RPCs {
		// Query chain ID from RPC
		client, err := ethclient.DialContext(ctx, rpcURL)
		if err != nil {
			return nil, fmt.Errorf("failed to dial RPC %s: %w", rpcURL, err)
		}
		chainIDBig, err := client.ChainID(ctx)
		client.Close()
		if err != nil {
			return nil, fmt.Errorf("failed to get chain ID from %s: %w", rpcURL, err)
		}
		chainIDUint := chainIDBig.Uint64()
		chainID := eth.ChainIDFromUInt64(chainIDUint)

		chain, err := NewChainIngester(ctx, logger, m, chainID, rpcURL, cfg)
		if err != nil {
			return nil, fmt.Errorf("failed to create chain %d: %w", chainIDUint, err)
		}
		b.chains[chainID] = chain
		logger.Info("Created chain ingester", "chainID", chainIDUint, "rpc", rpcURL)
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

// SetFailsafeEnabled enables or disables failsafe mode
func (b *Backend) SetFailsafeEnabled(enabled bool) {
	b.log.Info("Setting failsafe state", "enabled", enabled)
	b.failsafe.Store(enabled)
	b.metrics.RecordFailsafeEnabled(enabled)
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
		return types.ErrFailsafeEnabled
	}

	// Check if we're ready (all chains backfilled)
	if !b.Ready() {
		b.metrics.RecordCheckAccessList(false)
		return types.ErrUninitialized
	}

	// Only support "unsafe" safety level - we don't track safety levels
	if minSafety != types.LocalUnsafe {
		b.metrics.RecordCheckAccessList(false)
		return fmt.Errorf("unsupported safety level %q: only %q is supported", minSafety, types.LocalUnsafe)
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

		// Validate via LogsDB - propagate actual error from LogsDB
		if err := chain.Contains(access); err != nil {
			b.log.Debug("Access validation failed", "chainID", access.ChainID,
				"blockNum", access.BlockNumber, "logIdx", access.LogIndex, "err", err)
			b.metrics.RecordCheckAccessList(false)
			return err
		}
	}

	b.metrics.RecordCheckAccessList(true)
	return nil
}
