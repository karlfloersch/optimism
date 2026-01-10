package filter

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum/log"

	"github.com/ethereum-optimism/optimism/op-interop-filter/metrics"
	"github.com/ethereum-optimism/optimism/op-service/eth"
	"github.com/ethereum-optimism/optimism/op-supervisor/supervisor/types"
)

// CrossSafeValidator validates cross-chain executing messages and tracks
// the cross-validated timestamp. It runs a validation loop that checks
// each chain's executing messages against their source chains.
//
// This is separate from ChainIngester which only handles block ingestion.
// The cross-validated timestamp represents the point up to which ALL
// executing messages have been verified.
type CrossSafeValidator struct {
	log     log.Logger
	metrics metrics.Metricer
	cfg     *Config

	// Chain ingesters keyed by chain ID (read-only after construction)
	chains map[eth.ChainID]*ChainIngester

	// Cross-validated timestamp per chain - the timestamp up to which
	// all executing messages on that chain have been validated.
	// Key: chain ID, Value: timestamp
	crossValidatedTs sync.Map // map[eth.ChainID]*atomic.Uint64

	// Global cross-validated timestamp - minimum across all chains
	globalCrossValidatedTs atomic.Uint64

	// Context for shutdown
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// NewCrossSafeValidator creates a new CrossSafeValidator
func NewCrossSafeValidator(
	parentCtx context.Context,
	logger log.Logger,
	m metrics.Metricer,
	cfg *Config,
	chains map[eth.ChainID]*ChainIngester,
) *CrossSafeValidator {
	ctx, cancel := context.WithCancel(parentCtx)

	v := &CrossSafeValidator{
		log:     logger.New("component", "cross-safe-validator"),
		metrics: m,
		cfg:     cfg,
		chains:  chains,
		ctx:     ctx,
		cancel:  cancel,
	}

	// Initialize cross-validated timestamp for each chain
	for chainID := range chains {
		ts := &atomic.Uint64{}
		v.crossValidatedTs.Store(chainID, ts)
	}

	return v
}

// Start starts the validation loop
func (v *CrossSafeValidator) Start() error {
	v.log.Info("Starting cross-safe validator", "chains", len(v.chains))

	v.wg.Add(1)
	go v.runValidationLoop()

	return nil
}

// Stop stops the validation loop
func (v *CrossSafeValidator) Stop() error {
	v.log.Info("Stopping cross-safe validator")
	v.cancel()
	v.wg.Wait()
	return nil
}

// CrossValidatedTimestamp returns the global cross-validated timestamp.
// This is the minimum cross-validated timestamp across all chains.
// Returns 0, false if not all chains have been validated yet.
func (v *CrossSafeValidator) CrossValidatedTimestamp() (uint64, bool) {
	ts := v.globalCrossValidatedTs.Load()
	if ts == 0 {
		return 0, false
	}
	return ts, true
}

// ChainCrossValidatedTimestamp returns the cross-validated timestamp for a specific chain.
func (v *CrossSafeValidator) ChainCrossValidatedTimestamp(chainID eth.ChainID) (uint64, bool) {
	tsPtr, ok := v.crossValidatedTs.Load(chainID)
	if !ok {
		return 0, false
	}
	ts := tsPtr.(*atomic.Uint64).Load()
	if ts == 0 {
		return 0, false
	}
	return ts, true
}

// ValidateExecutingMessage validates a single executing message against its source chain.
// This is called by CheckAccessList for on-demand validation.
func (v *CrossSafeValidator) ValidateExecutingMessage(execMsg *types.ExecutingMessage, inclusionTimestamp uint64) error {
	ingester, ok := v.chains[execMsg.ChainID]
	if !ok {
		return fmt.Errorf("source chain %s: %w", execMsg.ChainID, types.ErrUnknownChain)
	}

	// Validate timing constraints
	if err := ValidateMessageTiming(execMsg.Timestamp, inclusionTimestamp, v.cfg.MessageExpiryWindow); err != nil {
		return err
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

// runValidationLoop periodically validates executing messages on all chains
func (v *CrossSafeValidator) runValidationLoop() {
	defer v.wg.Done()

	ticker := time.NewTicker(v.cfg.ValidationInterval)
	defer ticker.Stop()

	for {
		select {
		case <-v.ctx.Done():
			return
		case <-ticker.C:
			v.validateAllChains()
		}
	}
}

// validateAllChains validates executing messages on all chains up to the
// minimum ingested timestamp (so we know source chains have the data).
func (v *CrossSafeValidator) validateAllChains() {
	// First, get the minimum ingested timestamp across all chains.
	// This is the furthest we can validate (all chains have data up to here).
	minIngestedTs, ok := v.getMinIngestedTimestamp()
	if !ok {
		return // Not all chains ready yet
	}

	// Validate each chain up to minIngestedTs
	for chainID, ingester := range v.chains {
		if ingester.Error() != nil {
			continue // Skip chains in error state
		}

		if err := v.validateChain(chainID, ingester, minIngestedTs); err != nil {
			v.log.Error("Cross-validation failed",
				"chain", chainID,
				"err", err)
			ingester.setError(ErrorValidationFailed, err.Error())
		}
	}

	// Update global cross-validated timestamp
	v.updateGlobalCrossValidatedTimestamp()
}

// validateChain validates all executing messages on a chain from its current
// cross-validated timestamp up to maxTimestamp.
func (v *CrossSafeValidator) validateChain(chainID eth.ChainID, ingester *ChainIngester, maxTimestamp uint64) error {
	// Get current cross-validated timestamp for this chain
	currentTs, _ := v.ChainCrossValidatedTimestamp(chainID)

	// Get blocks from currentTs to maxTimestamp
	blocks, err := v.getBlocksInTimestampRange(ingester, currentTs, maxTimestamp)
	if err != nil {
		return fmt.Errorf("failed to get blocks for validation: %w", err)
	}

	// Validate each block's executing messages
	var newValidatedTs uint64
	for _, block := range blocks {
		for _, execMsg := range block.ExecMsgs {
			// Validate the executing message
			if err := v.ValidateExecutingMessage(execMsg, block.Timestamp); err != nil {
				return fmt.Errorf("validation failed at block %d, log %d: %w",
					block.BlockNum, execMsg.LogIdx, err)
			}
		}
		newValidatedTs = block.Timestamp
	}

	// Update cross-validated timestamp for this chain
	if newValidatedTs > currentTs {
		tsPtr, _ := v.crossValidatedTs.Load(chainID)
		tsPtr.(*atomic.Uint64).Store(newValidatedTs)

		v.log.Debug("Advanced cross-validated timestamp",
			"chain", chainID,
			"previous", currentTs,
			"new", newValidatedTs)
	}

	return nil
}

// getBlocksInTimestampRange returns all blocks with timestamps in (startTs, endTs].
func (v *CrossSafeValidator) getBlocksInTimestampRange(ingester *ChainIngester, startTs, endTs uint64) ([]blockExecMsgs, error) {
	// Get latest block from ingester
	latestBlock, ok := ingester.LatestBlock()
	if !ok {
		return nil, nil // No blocks yet
	}

	// Find the block range to validate
	// We need to find blocks with timestamp > startTs and <= endTs
	var result []blockExecMsgs

	// Start from where we left off - this is a simplified approach
	// In production, we'd want to track the last validated block number
	blocks, err := ingester.GetBlocksInRange(0, latestBlock.Number)
	if err != nil {
		return nil, err
	}

	for _, block := range blocks {
		if block.Timestamp > startTs && block.Timestamp <= endTs {
			result = append(result, block)
		}
	}

	return result, nil
}

// getMinIngestedTimestamp returns the minimum ingested timestamp across all chains.
// Returns false if any chain is not ready.
func (v *CrossSafeValidator) getMinIngestedTimestamp() (uint64, bool) {
	if len(v.chains) == 0 {
		return 0, false
	}

	var minTs uint64
	first := true
	for _, ingester := range v.chains {
		ts, ok := ingester.LatestTimestamp()
		if !ok {
			return 0, false // Chain not ready
		}
		if first || ts < minTs {
			minTs = ts
			first = false
		}
	}
	return minTs, true
}

// updateGlobalCrossValidatedTimestamp updates the global cross-validated timestamp
// to be the minimum across all chains.
func (v *CrossSafeValidator) updateGlobalCrossValidatedTimestamp() {
	if len(v.chains) == 0 {
		return
	}

	var minTs uint64
	first := true
	for chainID := range v.chains {
		ts, ok := v.ChainCrossValidatedTimestamp(chainID)
		if !ok {
			return // Not all chains have validated data yet
		}
		if first || ts < minTs {
			minTs = ts
			first = false
		}
	}

	if minTs > 0 {
		v.globalCrossValidatedTs.Store(minTs)
	}
}
