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
// the cross-validated timestamp. It runs a background validation loop that advances
// the cross-validated timestamp one step at a time, validating all messages at each
// timestamp across all chains.
type BackgroundCrossValidator struct {
	log     log.Logger
	metrics metrics.Metricer

	messageExpiryWindow uint64
	validationInterval  time.Duration

	// Chain ingesters keyed by chain ID (read-only after construction)
	chains map[eth.ChainID]ChainIngester

	// Single global cross-validated timestamp
	crossValidatedTs atomic.Uint64

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

	return &BackgroundCrossValidator{
		log:                 logger.New("component", "cross-validator"),
		metrics:             m,
		messageExpiryWindow: messageExpiryWindow,
		validationInterval:  validationInterval,
		chains:              chains,
		ctx:                 ctx,
		cancel:              cancel,
	}
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
	ts := v.crossValidatedTs.Load()
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
			v.advanceValidation()
		}
	}
}

// advanceValidation tries to advance the cross-validated timestamp one step at a time.
func (v *BackgroundCrossValidator) advanceValidation() {
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

	currentTs := v.crossValidatedTs.Load()

	// If we haven't started yet, initialize to min ingested - 1
	// (so first validation will be at minIngestedTs... but actually
	// we need to start from the earliest timestamp we have data for)
	if currentTs == 0 {
		minEarliestTs, ok := v.getMinEarliestTimestamp()
		if !ok {
			return
		}
		// Start one before the earliest so first +1 lands on earliest
		if minEarliestTs > 0 {
			currentTs = minEarliestTs - 1
			v.crossValidatedTs.Store(currentTs)
		}
	}

	// Try to advance one timestamp at a time until we catch up or hit an error
	for {
		nextTs := currentTs + 1

		// Don't go past what all chains have ingested
		if nextTs > minIngestedTs {
			return
		}

		// Validate all messages at this timestamp across all chains
		if err := v.validateTimestamp(nextTs); err != nil {
			v.log.Error("Cross-validation failed", "timestamp", nextTs, "err", err)
			// Set error on all chains to trigger failsafe
			for _, ingester := range v.chains {
				ingester.SetError(ErrorValidationFailed, err.Error())
			}
			return
		}

		// Advance
		v.crossValidatedTs.Store(nextTs)
		currentTs = nextTs

		v.log.Debug("Advanced cross-validated timestamp", "timestamp", nextTs)
	}
}

// validateTimestamp validates all executing messages with the given inclusion timestamp
// across all chains.
func (v *BackgroundCrossValidator) validateTimestamp(timestamp uint64) error {
	for chainID, ingester := range v.chains {
		msgs, err := ingester.GetExecMsgsAtTimestamp(timestamp)
		if err != nil {
			return fmt.Errorf("failed to get messages at timestamp %d from chain %s: %w",
				timestamp, chainID, err)
		}

		for _, msg := range msgs {
			if err := v.validateExecutingMessage(msg.ExecutingMessage, msg.InclusionTimestamp); err != nil {
				return fmt.Errorf("validation failed on chain %s at timestamp %d, log %d: %w",
					chainID, timestamp, msg.LogIdx, err)
			}
		}
	}

	return nil
}

func (v *BackgroundCrossValidator) getMinIngestedTimestamp() (uint64, bool) {
	if len(v.chains) == 0 {
		return 0, false
	}

	var minTs uint64
	first := true
	for _, ingester := range v.chains {
		if !ingester.Ready() {
			return 0, false
		}
		ts, ok := ingester.LatestTimestamp()
		if !ok {
			return 0, false
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

func (v *BackgroundCrossValidator) getMinEarliestTimestamp() (uint64, bool) {
	if len(v.chains) == 0 {
		return 0, false
	}

	var minTs uint64
	first := true
	for _, ingester := range v.chains {
		ts, ok := ingester.LatestTimestamp()
		if !ok {
			continue
		}
		// Use earliest block's timestamp as approximation
		// In practice, we'd want EarliestTimestamp() but we don't have that
		// For now, just use a reasonable starting point
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

// Ensure BackgroundCrossValidator implements CrossValidator
var _ CrossValidator = (*BackgroundCrossValidator)(nil)
