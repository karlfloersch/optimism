package filter

import (
	"fmt"

	"github.com/ethereum-optimism/optimism/op-service/eth"
	"github.com/ethereum-optimism/optimism/op-service/safemath"
	"github.com/ethereum-optimism/optimism/op-supervisor/supervisor/types"
)

// SimpleCrossValidator is a synchronous implementation of CrossValidator.
// It validates messages directly without a background loop.
// This implementation does NOT support same-block message dependencies.
type SimpleCrossValidator struct {
	chains             map[eth.ChainID]ChainIngester
	messageExpiryWindow uint64

	// Cross-validated timestamp - manually set for testing or computed
	crossValidatedTs uint64
}

// NewSimpleCrossValidator creates a new SimpleCrossValidator.
func NewSimpleCrossValidator(
	chains map[eth.ChainID]ChainIngester,
	messageExpiryWindow uint64,
) *SimpleCrossValidator {
	return &SimpleCrossValidator{
		chains:             chains,
		messageExpiryWindow: messageExpiryWindow,
	}
}

// SetCrossValidatedTimestamp sets the cross-validated timestamp.
// This is useful for testing specific scenarios.
func (v *SimpleCrossValidator) SetCrossValidatedTimestamp(ts uint64) {
	v.crossValidatedTs = ts
}

// ValidateAccessEntry implements CrossValidator.
// It validates a single access list entry against all message validity rules:
//  1. initTimestamp < inclusionTimestamp (must be strictly earlier to avoid cycles)
//  2. initTimestamp + MessageExpiryWindow >= inclusionTimestamp (message not expired)
//  3. If Timeout > 0: initTimestamp + MessageExpiryWindow >= inclusionTimestamp + Timeout
//  4. If CrossUnsafe: initTimestamp <= crossValidatedTimestamp
//  5. Log exists in source chain
func (v *SimpleCrossValidator) ValidateAccessEntry(
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
		if v.crossValidatedTs == 0 {
			return fmt.Errorf("cross-validated timestamp not available: %w", types.ErrOutOfScope)
		}
		if access.Timestamp > v.crossValidatedTs {
			return fmt.Errorf("message at timestamp %d not yet cross-unsafe validated "+
				"(current cross-validated timestamp: %d): %w",
				access.Timestamp, v.crossValidatedTs, types.ErrOutOfScope)
		}
	}

	// Validate the executing message (timing + log exists)
	return v.validateExecutingMessage(access, execDescriptor.Timestamp)
}

// validateExecutingMessage validates timing constraints and log existence.
func (v *SimpleCrossValidator) validateExecutingMessage(
	access types.Access,
	inclusionTimestamp uint64,
) error {
	// Get the source chain ingester
	ingester, ok := v.chains[access.ChainID]
	if !ok {
		return fmt.Errorf("source chain %s: %w", access.ChainID, types.ErrUnknownChain)
	}

	// Validate timing constraints
	if err := ValidateMessageTiming(access.Timestamp, inclusionTimestamp, v.messageExpiryWindow); err != nil {
		return err
	}

	// Check log exists in source chain
	query := types.ContainsQuery{
		Timestamp: access.Timestamp,
		BlockNum:  access.BlockNumber,
		LogIdx:    access.LogIndex,
		Checksum:  access.Checksum,
	}
	_, err := ingester.Contains(query)
	return err
}

// CrossValidatedTimestamp implements CrossValidator.
func (v *SimpleCrossValidator) CrossValidatedTimestamp() (uint64, bool) {
	if v.crossValidatedTs == 0 {
		return 0, false
	}
	return v.crossValidatedTs, true
}

// Ensure SimpleCrossValidator implements CrossValidator
var _ CrossValidator = (*SimpleCrossValidator)(nil)
