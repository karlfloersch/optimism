package filter

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/ethereum/go-ethereum/log"

	"github.com/ethereum-optimism/optimism/op-interop-filter/metrics"
	"github.com/ethereum-optimism/optimism/op-service/eth"
	"github.com/ethereum-optimism/optimism/op-supervisor/supervisor/types"
)

// Backend coordinates chain ingesters, manages failsafe state, and handles CheckAccessList requests.
type Backend struct {
	log     log.Logger
	metrics metrics.Metricer
	cfg     *Config

	// Chain ingesters keyed by chain ID.
	// Immutable after NewBackend returns; safe for concurrent reads.
	chains map[eth.ChainID]*ChainIngester

	// Failsafe state
	failsafe atomic.Bool

	// Cross-unsafe validated block - highest block number per chain that has been validated
	// Only accessed by the validation loop goroutine, no lock needed.
	validatedUpToBlockNum map[eth.ChainID]uint64

	// Cross-unsafe validated timestamp - highest timestamp where all chains have
	// caught up and executing messages have been validated
	crossUnsafeTimestamp atomic.Uint64

	// Context for shutdown
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// NewBackend creates a new Backend instance
func NewBackend(parentCtx context.Context, logger log.Logger, m metrics.Metricer, cfg *Config) (*Backend, error) {
	ctx, cancel := context.WithCancel(parentCtx)

	b := &Backend{
		log:                   logger,
		metrics:               m,
		cfg:                   cfg,
		chains:                make(map[eth.ChainID]*ChainIngester),
		validatedUpToBlockNum: make(map[eth.ChainID]uint64),
		ctx:                   ctx,
		cancel:                cancel,
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

		// Check for duplicate chain IDs BEFORE creating ingester
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
			b.onReorg,
		)
		if err != nil {
			cleanup()
			return nil, fmt.Errorf("failed to create chain ingester for chain %s: %w", chainID, err)
		}

		b.chains[chainID] = ingester
	}

	logger.Info("Created backend", "chains", len(b.chains))
	return b, nil
}

// Start starts all chain ingesters and the validation loop
func (b *Backend) Start(ctx context.Context) error {
	b.log.Info("Starting backend")

	for chainID, ingester := range b.chains {
		if err := ingester.Start(); err != nil {
			return fmt.Errorf("failed to start chain ingester for chain %s: %w", chainID, err)
		}
	}

	// Start validation loop
	b.wg.Add(1)
	go b.runValidationLoop()

	return nil
}

// Stop stops all chain ingesters and the validation loop
func (b *Backend) Stop(ctx context.Context) error {
	b.log.Info("Stopping backend")
	b.cancel()

	// Wait for validation loop to stop
	b.wg.Wait()

	var result error
	for chainID, ingester := range b.chains {
		if err := ingester.Stop(); err != nil {
			result = errors.Join(result, fmt.Errorf("failed to stop chain ingester for chain %s: %w", chainID, err))
		}
	}

	return result
}

// FailsafeEnabled returns whether failsafe is enabled
func (b *Backend) FailsafeEnabled() bool {
	return b.failsafe.Load()
}

// SetFailsafeEnabled sets the failsafe state
func (b *Backend) SetFailsafeEnabled(enabled bool) {
	b.failsafe.Store(enabled)
	b.metrics.RecordFailsafeEnabled(enabled)
	b.log.Info("Failsafe state changed", "enabled", enabled)
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

	// Check failsafe first
	if b.failsafe.Load() {
		b.metrics.RecordCheckAccessList(false)
		return types.ErrFailsafeEnabled
	}

	// Check if all chains are ready
	if !b.Ready() {
		b.metrics.RecordCheckAccessList(false)
		return types.ErrUninitialized
	}

	// We support LocalUnsafe and CrossUnsafe (we don't track derivation for Safe/Finalized)
	// CrossUnsafe is supported because we perform cross-chain validation in tryValidateCrossUnsafe()
	// and enable failsafe if any validation fails
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

		// Validate the access entry
		if err := b.validateAccess(ctx, access, minSafety, execDescriptor); err != nil {
			b.metrics.RecordCheckAccessList(false)
			return err
		}
	}

	b.metrics.RecordCheckAccessList(true)
	return nil
}

// validateAccess validates a single access entry
// This follows simplified linking rules (no cycle detection):
// 1. initTimestamp < execTimestamp (must be strictly earlier to avoid cycles)
// 2. initTimestamp + MessageExpiryWindow >= execTimestamp (message not expired)
// 3. If Timeout > 0: initTimestamp + MessageExpiryWindow >= execTimestamp + Timeout
// 4. If CrossUnsafe: initTimestamp <= crossUnsafeTimestamp (cross-chain validated)
func (b *Backend) validateAccess(ctx context.Context, access types.Access, minSafety types.SafetyLevel, execDescriptor types.ExecutingDescriptor) error {
	// Check timeout expiry first
	if execDescriptor.Timeout > 0 {
		expiresAt := saturatingAdd(access.Timestamp, b.cfg.MessageExpiryWindow)
		maxExecTimestamp := saturatingAdd(execDescriptor.Timestamp, execDescriptor.Timeout)
		if expiresAt < maxExecTimestamp {
			return fmt.Errorf("initiating message will expire before timeout: init %d + expiry %d = %d < exec %d + timeout %d = %d: %w",
				access.Timestamp, b.cfg.MessageExpiryWindow, expiresAt,
				execDescriptor.Timestamp, execDescriptor.Timeout, maxExecTimestamp, types.ErrConflict)
		}
	}

	// Check cross-unsafe timestamp
	if minSafety == types.CrossUnsafe {
		crossUnsafeTs := b.crossUnsafeTimestamp.Load()
		if access.Timestamp > crossUnsafeTs {
			return fmt.Errorf("message at timestamp %d not yet cross-unsafe validated (current cross-unsafe timestamp: %d): %w",
				access.Timestamp, crossUnsafeTs, types.ErrOutOfScope)
		}
	}

	// Validate core message rules (timestamp, expiry, log exists)
	execMsg := &types.ExecutingMessage{
		ChainID:   access.ChainID,
		BlockNum:  access.BlockNumber,
		LogIdx:    access.LogIndex,
		Timestamp: access.Timestamp,
		Checksum:  access.Checksum,
	}
	return b.validateExecutingMessage(execMsg, execDescriptor.Timestamp)
}

// saturatingAdd adds two uint64 values, returning max uint64 on overflow
func saturatingAdd(a, b uint64) uint64 {
	result := a + b
	if result < a { // overflow
		return ^uint64(0) // max uint64
	}
	return result
}

const validationInterval = 500 * time.Millisecond

// runValidationLoop periodically validates cross-unsafe executing messages
func (b *Backend) runValidationLoop() {
	defer b.wg.Done()

	ticker := time.NewTicker(validationInterval)
	defer ticker.Stop()

	for {
		select {
		case <-b.ctx.Done():
			return
		case <-ticker.C:
			b.tryValidateCrossUnsafe()
		}
	}
}

// tryValidateCrossUnsafe validates executing messages when all chains have caught up.
// Queries chain ingesters directly for their latest state.
func (b *Backend) tryValidateCrossUnsafe() {
	// Don't validate while failsafe is enabled
	if b.failsafe.Load() {
		return
	}

	// Find minimum timestamp across all chains
	minTimestamp, ok := b.getMinChainTimestamp()
	if !ok {
		return // Not all chains ready
	}

	// For each chain, validate executing messages from blocks we haven't validated yet
	for chainID, ingester := range b.chains {
		latestBlock, ok := ingester.LatestBlock()
		if !ok {
			continue
		}

		// Initialize or detect rewind: trust existing blocks, only validate new ones
		// On first encounter (startup) or rewind, set to current latest block
		if _, exists := b.validatedUpToBlockNum[chainID]; !exists ||
			b.validatedUpToBlockNum[chainID] > latestBlock.Number {
			b.validatedUpToBlockNum[chainID] = latestBlock.Number
			continue // Trust existing, will validate from next block onwards
		}

		startBlock := b.validatedUpToBlockNum[chainID] + 1
		endBlock := latestBlock.Number

		if startBlock > endBlock {
			continue // Already validated up to latest
		}

		// Query all blocks in the range
		blocks, err := ingester.GetBlocksInRange(startBlock, endBlock)
		if err != nil {
			b.log.Error("Failed to query blocks", "chain", chainID, "err", err)
			continue
		}

		// Validate each block's executing messages up to minTimestamp
		for _, block := range blocks {
			if block.Timestamp > minTimestamp {
				break // Don't validate blocks past minTimestamp yet
			}

			// Validate any executing messages in this block
			for _, execMsg := range block.ExecMsgs {
				if err := b.validateExecutingMessage(execMsg, block.Timestamp); err != nil {
					b.log.Error("Cross-unsafe validation failed, enabling failsafe",
						"execChain", chainID,
						"sourceChain", execMsg.ChainID,
						"execBlock", block.BlockNum,
						"timestamp", block.Timestamp,
						"err", err)
					b.SetFailsafeEnabled(true)
					return
				}
			}

			// Update validated block pointer
			b.validatedUpToBlockNum[chainID] = block.BlockNum
		}
	}

	// Update cross-unsafe timestamp
	b.crossUnsafeTimestamp.Store(minTimestamp)
	b.metrics.RecordCrossUnsafeValidatedTimestamp(minTimestamp)
}

// getMinChainTimestamp returns the minimum timestamp across all chains.
// Returns false if any chain is not ready yet.
func (b *Backend) getMinChainTimestamp() (uint64, bool) {
	if len(b.chains) == 0 {
		return 0, false
	}

	var minTimestamp uint64
	first := true
	for _, ingester := range b.chains {
		ts, ok := ingester.LatestTimestamp()
		if !ok {
			return 0, false // Chain not ready
		}
		if first || ts < minTimestamp {
			minTimestamp = ts
			first = false
		}
	}
	return minTimestamp, true
}

// validateExecutingMessage validates a single executing message against the source chain
// execTimestamp is the timestamp when the executing message was processed
// Uses the same linking rules as validateAccess (matching op-supervisor)
func (b *Backend) validateExecutingMessage(execMsg *types.ExecutingMessage, execTimestamp uint64) error {
	ingester, ok := b.chains[execMsg.ChainID]
	if !ok {
		return fmt.Errorf("source chain %s: %w", execMsg.ChainID, types.ErrUnknownChain)
	}

	// Initiating message timestamp must be strictly before execution timestamp
	if execMsg.Timestamp >= execTimestamp {
		return fmt.Errorf("initiating message timestamp %d not before execution timestamp %d: %w",
			execMsg.Timestamp, execTimestamp, types.ErrConflict)
	}

	// Check expiry: message expires at initTimestamp + MessageExpiryWindow
	expiresAt := saturatingAdd(execMsg.Timestamp, b.cfg.MessageExpiryWindow)
	if expiresAt < execTimestamp {
		return fmt.Errorf("initiating message expired: init %d + expiry window %d = %d < exec %d: %w",
			execMsg.Timestamp, b.cfg.MessageExpiryWindow, expiresAt, execTimestamp, types.ErrConflict)
	}

	// Check log exists in source chain
	query := types.ContainsQuery{
		Timestamp: execMsg.Timestamp,
		BlockNum:  execMsg.BlockNum,
		LogIdx:    execMsg.LogIdx,
		Checksum:  execMsg.Checksum,
	}

	_, err := ingester.Contains(query)
	return err
}

// onReorg is called when a reorg is detected on a chain
func (b *Backend) onReorg(chainID eth.ChainID) {
	b.log.Warn("Reorg detected, enabling failsafe", "chain", chainID)
	b.SetFailsafeEnabled(true)
}

// GetChainIDs returns the chain IDs of all configured chains
func (b *Backend) GetChainIDs() []eth.ChainID {
	chainIDs := make([]eth.ChainID, 0, len(b.chains))
	for chainID := range b.chains {
		chainIDs = append(chainIDs, chainID)
	}
	return chainIDs
}

// CrossUnsafeTimestamp returns the highest timestamp that has been cross-chain validated.
// Messages at or before this timestamp satisfy CrossUnsafe safety level.
func (b *Backend) CrossUnsafeTimestamp() uint64 {
	return b.crossUnsafeTimestamp.Load()
}
