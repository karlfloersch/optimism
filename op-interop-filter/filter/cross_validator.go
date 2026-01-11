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
	"github.com/ethereum-optimism/optimism/op-service/safemath"
	"github.com/ethereum-optimism/optimism/op-supervisor/supervisor/types"
)

// ValidateMessageTiming validates timing constraints for cross-chain messages.
// Checks:
//   - initMsgTimestamp < inclusionTimestamp (init must be before execution)
//   - initMsgTimestamp + expiryWindow >= inclusionTimestamp (not expired)
func ValidateMessageTiming(initMsgTimestamp, inclusionTimestamp, expiryWindow uint64) error {
	if !(initMsgTimestamp < inclusionTimestamp) {
		return fmt.Errorf("initiating message timestamp %d not before inclusion timestamp %d: %w",
			initMsgTimestamp, inclusionTimestamp, types.ErrConflict)
	}

	expiresAt := safemath.SaturatingAdd(initMsgTimestamp, expiryWindow)
	if expiresAt < inclusionTimestamp {
		return fmt.Errorf("initiating message expired: init %d + expiry window %d = %d < inclusion %d: %w",
			initMsgTimestamp, expiryWindow, expiresAt, inclusionTimestamp, types.ErrConflict)
	}

	return nil
}

// CrossValidator validates cross-chain executing messages and tracks
// the cross-validated timestamp. It runs a validation loop that checks
// each chain's executing messages against their source chains.
//
// This is separate from ChainIngester which only handles block ingestion.
// The cross-validated timestamp represents the point up to which ALL
// executing messages have been verified.
type CrossValidator struct {
	log     log.Logger
	metrics metrics.Metricer
	cfg     *Config

	// Chain ingesters keyed by chain ID (read-only after construction)
	chains map[eth.ChainID]*ChainIngester

	// Cross-validated timestamp per chain - the timestamp up to which
	// all executing messages on that chain have been validated.
	// Key: chain ID, Value: timestamp
	crossValidatedTs sync.Map // map[eth.ChainID]*atomic.Uint64

	// Last validated block number per chain - tracks where we left off
	// to avoid re-validating already validated blocks.
	// Key: chain ID, Value: block number
	lastValidatedBlockNum sync.Map // map[eth.ChainID]*atomic.Uint64

	// Global cross-validated timestamp - minimum across all chains
	globalCrossValidatedTs atomic.Uint64

	// Context for shutdown
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// NewCrossValidator creates a new CrossValidator
func NewCrossValidator(
	parentCtx context.Context,
	logger log.Logger,
	m metrics.Metricer,
	cfg *Config,
	chains map[eth.ChainID]*ChainIngester,
) *CrossValidator {
	ctx, cancel := context.WithCancel(parentCtx)

	v := &CrossValidator{
		log:     logger.New("component", "cross-validator"),
		metrics: m,
		cfg:     cfg,
		chains:  chains,
		ctx:     ctx,
		cancel:  cancel,
	}

	// Initialize cross-validated timestamp and last validated block for each chain
	for chainID := range chains {
		ts := &atomic.Uint64{}
		v.crossValidatedTs.Store(chainID, ts)

		blockNum := &atomic.Uint64{}
		v.lastValidatedBlockNum.Store(chainID, blockNum)
	}

	return v
}

// Start starts the validation loop
func (v *CrossValidator) Start() error {
	v.log.Info("Starting cross-validator", "chains", len(v.chains))

	v.wg.Add(1)
	go v.runValidationLoop()

	return nil
}

// Stop stops the validation loop
func (v *CrossValidator) Stop() error {
	v.log.Info("Stopping cross-validator")
	v.cancel()
	v.wg.Wait()
	return nil
}

// CrossValidatedTimestamp returns the global cross-validated timestamp.
// This is the minimum cross-validated timestamp across all chains.
// Returns 0, false if not all chains have been validated yet.
func (v *CrossValidator) CrossValidatedTimestamp() (uint64, bool) {
	ts := v.globalCrossValidatedTs.Load()
	if ts == 0 {
		return 0, false
	}
	return ts, true
}

// ChainCrossValidatedTimestamp returns the cross-validated timestamp for a specific chain.
func (v *CrossValidator) ChainCrossValidatedTimestamp(chainID eth.ChainID) (uint64, bool) {
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

// ValidateAccessEntry validates a single access list entry against all message validity rules.
// This consolidates all message validation checks:
// 1. initTimestamp < inclusionTimestamp (must be strictly earlier to avoid cycles)
// 2. initTimestamp + MessageExpiryWindow >= inclusionTimestamp (message not expired)
// 3. If Timeout > 0: initTimestamp + MessageExpiryWindow >= inclusionTimestamp + Timeout
// 4. If CrossUnsafe: initTimestamp <= crossValidatedTimestamp (cross-chain validated)
// 5. Log exists in source chain
func (v *CrossValidator) ValidateAccessEntry(access types.Access, minSafety types.SafetyLevel, execDescriptor types.ExecutingDescriptor) error {
	// Check timeout expiry first
	if execDescriptor.Timeout > 0 {
		expiresAt := safemath.SaturatingAdd(access.Timestamp, v.cfg.MessageExpiryWindow)
		maxExecTimestamp := safemath.SaturatingAdd(execDescriptor.Timestamp, execDescriptor.Timeout)
		if expiresAt < maxExecTimestamp {
			return fmt.Errorf("initiating message will expire before timeout: init %d + expiry %d = %d < exec %d + timeout %d = %d: %w",
				access.Timestamp, v.cfg.MessageExpiryWindow, expiresAt,
				execDescriptor.Timestamp, execDescriptor.Timeout, maxExecTimestamp, types.ErrConflict)
		}
	}

	// Check cross-unsafe timestamp - message must be at or before the cross-validated
	// timestamp (the point up to which all executing messages have been verified)
	if minSafety == types.CrossUnsafe {
		crossValidatedTs, ok := v.CrossValidatedTimestamp()
		if !ok {
			return fmt.Errorf("cross-validated timestamp not available: %w", types.ErrOutOfScope)
		}
		if access.Timestamp > crossValidatedTs {
			return fmt.Errorf("message at timestamp %d not yet cross-unsafe validated (current cross-validated timestamp: %d): %w",
				access.Timestamp, crossValidatedTs, types.ErrOutOfScope)
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
	return v.ValidateExecutingMessage(execMsg, execDescriptor.Timestamp)
}

// ValidateExecutingMessage validates a single executing message against its source chain.
// This is called by ValidateAccessEntry and during the validation loop.
func (v *CrossValidator) ValidateExecutingMessage(execMsg *types.ExecutingMessage, inclusionTimestamp uint64) error {
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
func (v *CrossValidator) runValidationLoop() {
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
func (v *CrossValidator) validateAllChains() {
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
		if !ingester.Ready() {
			continue // Skip chains that haven't finished backfill
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
// last validated block up to maxTimestamp.
func (v *CrossValidator) validateChain(chainID eth.ChainID, ingester *ChainIngester, maxTimestamp uint64) error {
	// Get current cross-validated timestamp for this chain
	currentTs, _ := v.ChainCrossValidatedTimestamp(chainID)

	// Get blocks that need validation
	blocks, err := v.getBlocksForValidation(chainID, ingester, maxTimestamp)
	if err != nil {
		return fmt.Errorf("failed to get blocks for validation: %w", err)
	}

	if len(blocks) == 0 {
		return nil // Nothing to validate
	}

	// Validate each block's executing messages
	var newValidatedTs uint64
	var newValidatedBlockNum uint64
	for _, block := range blocks {
		for _, execMsg := range block.ExecMsgs {
			// Validate the executing message
			if err := v.ValidateExecutingMessage(execMsg, block.Timestamp); err != nil {
				return fmt.Errorf("validation failed at block %d, log %d: %w",
					block.BlockNum, execMsg.LogIdx, err)
			}
		}
		newValidatedTs = block.Timestamp
		newValidatedBlockNum = block.BlockNum
	}

	// Update last validated block number for this chain
	if newValidatedBlockNum > 0 {
		blockNumPtr, _ := v.lastValidatedBlockNum.Load(chainID)
		blockNumPtr.(*atomic.Uint64).Store(newValidatedBlockNum)
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

// getBlocksForValidation returns blocks that need to be validated for a chain.
// It starts from the last validated block (or earliest block if never validated)
// and returns all blocks up to the latest block.
func (v *CrossValidator) getBlocksForValidation(chainID eth.ChainID, ingester *ChainIngester, maxTimestamp uint64) ([]blockExecMsgs, error) {
	// Get latest block from ingester
	latestBlock, ok := ingester.LatestBlock()
	if !ok {
		return nil, nil // No blocks yet
	}

	// Get the earliest block number from the ingester
	earliestBlockNum, ok := ingester.EarliestBlockNum()
	if !ok {
		return nil, nil // DB not initialized yet
	}

	// Get last validated block number for this chain
	var startBlockNum uint64
	lastValidatedPtr, ok := v.lastValidatedBlockNum.Load(chainID)
	if ok && lastValidatedPtr.(*atomic.Uint64).Load() > 0 {
		// Start from the block after the last validated one
		startBlockNum = lastValidatedPtr.(*atomic.Uint64).Load() + 1
	} else {
		// First time validating - start from the earliest block
		startBlockNum = earliestBlockNum
	}

	// Don't try to validate blocks before the earliest one in the DB
	if startBlockNum < earliestBlockNum {
		startBlockNum = earliestBlockNum
	}

	// Nothing to validate if we've caught up
	if startBlockNum > latestBlock.Number {
		return nil, nil
	}

	// Get blocks in range
	blocks, err := ingester.GetBlocksInRange(startBlockNum, latestBlock.Number)
	if err != nil {
		return nil, err
	}

	// Filter to only blocks up to maxTimestamp
	var result []blockExecMsgs
	for _, block := range blocks {
		if block.Timestamp <= maxTimestamp {
			result = append(result, block)
		}
	}

	return result, nil
}

// getMinIngestedTimestamp returns the minimum ingested timestamp across all ready chains.
// Returns false if no chains are ready yet.
func (v *CrossValidator) getMinIngestedTimestamp() (uint64, bool) {
	if len(v.chains) == 0 {
		return 0, false
	}

	var minTs uint64
	first := true
	for _, ingester := range v.chains {
		if !ingester.Ready() {
			continue // Skip chains that haven't finished backfill
		}
		ts, ok := ingester.LatestTimestamp()
		if !ok {
			continue // Skip chains without timestamp data
		}
		if first || ts < minTs {
			minTs = ts
			first = false
		}
	}
	if first {
		return 0, false // No ready chains found
	}
	return minTs, true
}

// updateGlobalCrossValidatedTimestamp updates the global cross-validated timestamp
// to be the minimum across all chains.
func (v *CrossValidator) updateGlobalCrossValidatedTimestamp() {
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
