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

// BackgroundCrossValidator validates cross-chain executing messages and tracks
// the cross-validated timestamp. It runs a background validation loop that checks
// each chain's executing messages against their source chains.
type BackgroundCrossValidator struct {
	log     log.Logger
	metrics metrics.Metricer

	messageExpiryWindow uint64
	validationInterval  time.Duration

	// Chain ingesters keyed by chain ID (read-only after construction)
	chains map[eth.ChainID]ChainIngester

	// Cross-validated timestamp per chain
	crossValidatedTs sync.Map // map[eth.ChainID]*atomic.Uint64

	// Last validated block number per chain
	lastValidatedBlockNum sync.Map // map[eth.ChainID]*atomic.Uint64

	// Global cross-validated timestamp - minimum across all chains
	globalCrossValidatedTs atomic.Uint64

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// NewBackgroundCrossValidator creates a new BackgroundCrossValidator
func NewBackgroundCrossValidator(
	parentCtx context.Context,
	logger log.Logger,
	m metrics.Metricer,
	messageExpiryWindow uint64,
	validationInterval time.Duration,
	chains map[eth.ChainID]ChainIngester,
) *BackgroundCrossValidator {
	ctx, cancel := context.WithCancel(parentCtx)

	v := &BackgroundCrossValidator{
		log:                 logger.New("component", "cross-validator"),
		metrics:             m,
		messageExpiryWindow: messageExpiryWindow,
		validationInterval:  validationInterval,
		chains:              chains,
		ctx:                 ctx,
		cancel:              cancel,
	}

	for chainID := range chains {
		ts := &atomic.Uint64{}
		v.crossValidatedTs.Store(chainID, ts)

		blockNum := &atomic.Uint64{}
		v.lastValidatedBlockNum.Store(chainID, blockNum)
	}

	return v
}

// Start starts the validation loop
func (v *BackgroundCrossValidator) Start() error {
	v.log.Info("Starting cross-validator", "chains", len(v.chains))

	v.wg.Add(1)
	go v.runValidationLoop()

	return nil
}

// Stop stops the validation loop
func (v *BackgroundCrossValidator) Stop() error {
	v.log.Info("Stopping cross-validator")
	v.cancel()
	v.wg.Wait()
	return nil
}

// CrossValidatedTimestamp returns the global cross-validated timestamp.
func (v *BackgroundCrossValidator) CrossValidatedTimestamp() (uint64, bool) {
	ts := v.globalCrossValidatedTs.Load()
	if ts == 0 {
		return 0, false
	}
	return ts, true
}

// ChainCrossValidatedTimestamp returns the cross-validated timestamp for a specific chain.
func (v *BackgroundCrossValidator) ChainCrossValidatedTimestamp(chainID eth.ChainID) (uint64, bool) {
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
func (v *BackgroundCrossValidator) ValidateAccessEntry(
	access types.Access,
	minSafety types.SafetyLevel,
	execDescriptor types.ExecutingDescriptor,
) error {
	// Check timeout expiry first
	if execDescriptor.Timeout > 0 {
		expiresAt := safemath.SaturatingAdd(access.Timestamp, v.messageExpiryWindow)
		maxExecTimestamp := safemath.SaturatingAdd(execDescriptor.Timestamp, execDescriptor.Timeout)
		if expiresAt < maxExecTimestamp {
			return fmt.Errorf("initiating message will expire before timeout: "+
				"init %d + expiry %d = %d < exec %d + timeout %d = %d: %w",
				access.Timestamp, v.messageExpiryWindow, expiresAt,
				execDescriptor.Timestamp, execDescriptor.Timeout, maxExecTimestamp,
				types.ErrConflict)
		}
	}

	// Check cross-unsafe timestamp
	if minSafety == types.CrossUnsafe {
		crossValidatedTs, ok := v.CrossValidatedTimestamp()
		if !ok {
			return fmt.Errorf("cross-validated timestamp not available: %w", types.ErrOutOfScope)
		}
		if access.Timestamp > crossValidatedTs {
			return fmt.Errorf("message at timestamp %d not yet cross-unsafe validated "+
				"(current cross-validated timestamp: %d): %w",
				access.Timestamp, crossValidatedTs, types.ErrOutOfScope)
		}
	}

	// Validate core message rules
	execMsg := &types.ExecutingMessage{
		ChainID:   access.ChainID,
		BlockNum:  access.BlockNumber,
		LogIdx:    access.LogIndex,
		Timestamp: access.Timestamp,
		Checksum:  access.Checksum,
	}
	return v.validateExecutingMessage(execMsg, execDescriptor.Timestamp)
}

func (v *BackgroundCrossValidator) validateExecutingMessage(
	execMsg *types.ExecutingMessage,
	inclusionTimestamp uint64,
) error {
	ingester, ok := v.chains[execMsg.ChainID]
	if !ok {
		return fmt.Errorf("source chain %s: %w", execMsg.ChainID, types.ErrUnknownChain)
	}

	if err := ValidateMessageTiming(execMsg.Timestamp, inclusionTimestamp, v.messageExpiryWindow); err != nil {
		return err
	}

	query := types.ContainsQuery{
		Timestamp: execMsg.Timestamp,
		BlockNum:  execMsg.BlockNum,
		LogIdx:    execMsg.LogIdx,
		Checksum:  execMsg.Checksum,
	}
	_, err := ingester.Contains(query)
	return err
}

func (v *BackgroundCrossValidator) runValidationLoop() {
	defer v.wg.Done()

	ticker := time.NewTicker(v.validationInterval)
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

func (v *BackgroundCrossValidator) validateAllChains() {
	// All chains must be ready and error-free
	for _, ingester := range v.chains {
		if ingester.Error() != nil {
			return
		}
		if !ingester.Ready() {
			return
		}
	}

	minIngestedTs, ok := v.getMinIngestedTimestamp()
	if !ok {
		return
	}

	for chainID, ingester := range v.chains {
		if err := v.validateChain(chainID, ingester, minIngestedTs); err != nil {
			v.log.Error("Cross-validation failed",
				"chain", chainID,
				"err", err)
			ingester.SetError(ErrorValidationFailed, err.Error())
			return
		}
	}

	v.updateGlobalCrossValidatedTimestamp()
}

func (v *BackgroundCrossValidator) validateChain(
	chainID eth.ChainID,
	ingester ChainIngester,
	maxTimestamp uint64,
) error {
	currentTs, _ := v.ChainCrossValidatedTimestamp(chainID)

	blocks, err := v.getBlocksForValidation(chainID, ingester, maxTimestamp)
	if err != nil {
		return fmt.Errorf("failed to get blocks for validation: %w", err)
	}

	if len(blocks) == 0 {
		return nil
	}

	var newValidatedTs uint64
	var newValidatedBlockNum uint64
	for _, block := range blocks {
		for _, execMsg := range block.ExecMsgs {
			if err := v.validateExecutingMessage(execMsg, block.Timestamp); err != nil {
				return fmt.Errorf("validation failed at block %d, log %d: %w",
					block.BlockNum, execMsg.LogIdx, err)
			}
		}
		newValidatedTs = block.Timestamp
		newValidatedBlockNum = block.BlockNum
	}

	if newValidatedBlockNum > 0 {
		blockNumPtr, _ := v.lastValidatedBlockNum.Load(chainID)
		blockNumPtr.(*atomic.Uint64).Store(newValidatedBlockNum)
	}

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

func (v *BackgroundCrossValidator) getBlocksForValidation(
	chainID eth.ChainID,
	ingester ChainIngester,
	maxTimestamp uint64,
) ([]BlockExecMsgs, error) {
	latestBlock, ok := ingester.LatestBlock()
	if !ok {
		return nil, nil
	}

	earliestBlockNum, ok := ingester.EarliestBlockNum()
	if !ok {
		return nil, nil
	}

	var startBlockNum uint64
	lastValidatedPtr, ok := v.lastValidatedBlockNum.Load(chainID)
	if ok && lastValidatedPtr.(*atomic.Uint64).Load() > 0 {
		startBlockNum = lastValidatedPtr.(*atomic.Uint64).Load() + 1
	} else {
		startBlockNum = earliestBlockNum
	}

	if startBlockNum < earliestBlockNum {
		startBlockNum = earliestBlockNum
	}

	if startBlockNum > latestBlock.Number {
		return nil, nil
	}

	blocks, err := ingester.GetBlocksInRange(startBlockNum, latestBlock.Number)
	if err != nil {
		return nil, err
	}

	var result []BlockExecMsgs
	for _, block := range blocks {
		if block.Timestamp <= maxTimestamp {
			result = append(result, block)
		}
	}

	return result, nil
}

func (v *BackgroundCrossValidator) getMinIngestedTimestamp() (uint64, bool) {
	if len(v.chains) == 0 {
		return 0, false
	}

	var minTs uint64
	first := true
	for _, ingester := range v.chains {
		if !ingester.Ready() {
			continue
		}
		ts, ok := ingester.LatestTimestamp()
		if !ok {
			continue
		}
		if first || ts < minTs {
			minTs = ts
			first = false
		}
	}
	if first {
		return 0, false
	}
	return minTs, true
}

func (v *BackgroundCrossValidator) updateGlobalCrossValidatedTimestamp() {
	if len(v.chains) == 0 {
		return
	}

	var minTs uint64
	first := true
	for chainID := range v.chains {
		ts, ok := v.ChainCrossValidatedTimestamp(chainID)
		if !ok {
			return
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

// Ensure BackgroundCrossValidator implements CrossValidator
var _ CrossValidator = (*BackgroundCrossValidator)(nil)
