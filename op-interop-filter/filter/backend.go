package filter

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/ethereum/go-ethereum/log"

	"github.com/ethereum-optimism/optimism/op-interop-filter/metrics"
	"github.com/ethereum-optimism/optimism/op-service/eth"
	"github.com/ethereum-optimism/optimism/op-supervisor/supervisor/types"
)

// Backend coordinates chain ingesters and handles CheckAccessList requests.
// Failsafe is enabled if manually set OR if any chain ingester has an error.
type Backend struct {
	log     log.Logger
	metrics metrics.Metricer
	cfg     *Config

	// Chain ingesters keyed by chain ID.
	// Immutable after NewBackend returns; safe for concurrent reads.
	chains map[eth.ChainID]*ChainIngester

	// Cross-message validator handles all cross-chain message validation.
	// Runs a validation loop and tracks the cross-validated timestamp.
	crossMessageValidator *CrossValidator

	// Manual failsafe override - when set, failsafe is enabled regardless of chain state.
	// Uses atomic.Bool for thread-safe access from concurrent goroutines.
	manualFailsafe atomic.Bool

	// Context for shutdown
	ctx    context.Context
	cancel context.CancelFunc
}

// NewBackend creates a new Backend instance
func NewBackend(parentCtx context.Context, logger log.Logger, m metrics.Metricer, cfg *Config) (*Backend, error) {
	ctx, cancel := context.WithCancel(parentCtx)

	b := &Backend{
		log:     logger,
		metrics: m,
		cfg:     cfg,
		chains:  make(map[eth.ChainID]*ChainIngester),
		ctx:     ctx,
		cancel:  cancel,
	}

	// Helper to cleanup on error
	cleanup := func() {
		cancel()
		for _, ingester := range b.chains {
			_ = ingester.Stop()
		}
	}

	// Create chain ingesters for each L2 RPC
	for _, rpcURL := range cfg.L2RPCs {
		// Query chain ID from the RPC
		ethClient, err := ethclient.Dial(rpcURL)
		if err != nil {
			cleanup()
			return nil, fmt.Errorf("failed to connect to %s: %w", rpcURL, err)
		}
		chainIDBig, err := ethClient.ChainID(ctx)
		ethClient.Close()
		if err != nil {
			cleanup()
			return nil, fmt.Errorf("failed to query chain ID from %s: %w", rpcURL, err)
		}
		chainID := eth.ChainIDFromBig(chainIDBig)

		// Check for duplicate chain IDs before creating ingester
		if _, exists := b.chains[chainID]; exists {
			cleanup()
			return nil, fmt.Errorf("duplicate chain ID %s: multiple RPCs return the same chain ID", chainID)
		}

		logger.Info("Creating chain ingester", "chain", chainID, "rpc", rpcURL)

		ingester, err := NewChainIngester(
			ctx,
			logger,
			m,
			chainID,
			rpcURL,
			cfg.DataDir,
			cfg.BackfillDuration,
			cfg.PollInterval,
		)
		if err != nil {
			cleanup()
			return nil, fmt.Errorf("failed to create chain ingester for chain %s: %w", chainID, err)
		}

		b.chains[chainID] = ingester
	}

	// Create cross-message validator after all chain ingesters are created
	b.crossMessageValidator = NewCrossValidator(ctx, logger, m, cfg, b.chains)

	logger.Info("Created backend", "chains", len(b.chains))
	return b, nil
}

// Start starts all chain ingesters and the cross-message validator
func (b *Backend) Start(ctx context.Context) error {
	b.log.Info("Starting backend")

	for chainID, ingester := range b.chains {
		if err := ingester.Start(); err != nil {
			return fmt.Errorf("failed to start chain ingester for chain %s: %w", chainID, err)
		}
	}

	// Start cross-message validator after chain ingesters
	if err := b.crossMessageValidator.Start(); err != nil {
		return fmt.Errorf("failed to start cross-message validator: %w", err)
	}

	return nil
}

// Stop stops all chain ingesters and the cross-message validator
func (b *Backend) Stop(ctx context.Context) error {
	b.log.Info("Stopping backend")
	b.cancel()

	var result error

	// Stop cross-message validator first
	if err := b.crossMessageValidator.Stop(); err != nil {
		result = errors.Join(result, fmt.Errorf("failed to stop cross-message validator: %w", err))
	}

	for chainID, ingester := range b.chains {
		if err := ingester.Stop(); err != nil {
			result = errors.Join(result, fmt.Errorf("failed to stop chain ingester for chain %s: %w", chainID, err))
		}
	}

	return result
}

// FailsafeEnabled returns true if failsafe is manually enabled OR any chain has an error.
func (b *Backend) FailsafeEnabled() bool {
	return b.manualFailsafe.Load() || len(b.GetChainErrors()) > 0
}

// SetFailsafeEnabled sets the manual failsafe override.
// When enabled=true, failsafe is enabled regardless of chain state.
// When enabled=false, only clears the manual override (chain errors remain).
// Use ClearChainErrors() to clear chain ingester errors.
func (b *Backend) SetFailsafeEnabled(enabled bool) {
	b.manualFailsafe.Store(enabled)
	b.metrics.RecordFailsafeEnabled(b.FailsafeEnabled())
}

// GetChainErrors returns all chains that are in an error state
func (b *Backend) GetChainErrors() map[eth.ChainID]*IngesterError {
	errors := make(map[eth.ChainID]*IngesterError)
	for chainID, ingester := range b.chains {
		if err := ingester.Error(); err != nil {
			errors[chainID] = err
		}
	}
	return errors
}

// Ready returns true if all chains have completed backfill
func (b *Backend) Ready() bool {
	for _, ingester := range b.chains {
		if !ingester.Ready() {
			return false
		}
	}

	return len(b.chains) > 0
}

// CheckAccessList validates the given access list entries.
func (b *Backend) CheckAccessList(ctx context.Context, inboxEntries []common.Hash,
	minSafety types.SafetyLevel, execDescriptor types.ExecutingDescriptor) error {

	// Check failsafe first (derived from chain error states)
	if b.FailsafeEnabled() {
		b.metrics.RecordCheckAccessList(false)
		return types.ErrFailsafeEnabled
	}

	// Check if all chains are ready
	if !b.Ready() {
		b.metrics.RecordCheckAccessList(false)
		return types.ErrUninitialized
	}

	// We support LocalUnsafe and CrossUnsafe (we don't track derivation for Safe/Finalized)
	if minSafety != types.LocalUnsafe && minSafety != types.CrossUnsafe {
		b.metrics.RecordCheckAccessList(false)
		return fmt.Errorf("unsupported safety level %s: only %s and %s are supported",
			minSafety, types.LocalUnsafe, types.CrossUnsafe)
	}

	// Validate executing chain is one we're tracking
	if _, ok := b.chains[execDescriptor.ChainID]; !ok {
		b.metrics.RecordCheckAccessList(false)
		return fmt.Errorf("executing chain %s: %w", execDescriptor.ChainID, types.ErrUnknownChain)
	}

	// Parse and validate each access entry
	remaining := inboxEntries
	for len(remaining) > 0 {
		var access types.Access
		var err error
		remaining, access, err = types.ParseAccess(remaining)
		if err != nil {
			b.metrics.RecordCheckAccessList(false)
			return fmt.Errorf("failed to parse access entry: %w", err)
		}

		// Validate the access entry using the cross-message validator
		if err := b.crossMessageValidator.ValidateAccessEntry(access, minSafety, execDescriptor); err != nil {
			b.metrics.RecordCheckAccessList(false)
			return err
		}
	}

	b.metrics.RecordCheckAccessList(true)
	return nil
}
