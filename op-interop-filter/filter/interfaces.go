package filter

import (
	"github.com/ethereum-optimism/optimism/op-service/eth"
	"github.com/ethereum-optimism/optimism/op-supervisor/supervisor/types"
)

// IncludedMessage wraps an executing message with its inclusion context.
// The ExecutingMessage contains the initiating message's data (source chain),
// while InclusionBlockNum/Timestamp indicate when it was executed (this chain).
type IncludedMessage struct {
	*types.ExecutingMessage
	InclusionBlockNum  uint64
	InclusionTimestamp uint64
}

// ChainIngester provides access to chain logs and state.
// Implementations include:
//   - MemoryChainIngester: in-memory for testing
//   - LogsDBChainIngester: RPC + sqlite-based for production
type ChainIngester interface {
	// Contains checks if a log exists in the chain's database.
	Contains(query types.ContainsQuery) (types.BlockSeal, error)

	// LatestBlock returns the latest ingested block.
	LatestBlock() (eth.BlockID, bool)

	// LatestTimestamp returns the timestamp of the latest ingested block.
	LatestTimestamp() (uint64, bool)

	// EarliestBlockNum returns the earliest block number in the database.
	EarliestBlockNum() (uint64, bool)

	// GetExecMsgsAtTimestamp returns executing messages with the given inclusion timestamp.
	GetExecMsgsAtTimestamp(timestamp uint64) ([]IncludedMessage, error)

	// Ready returns true if the ingester has completed initial sync.
	Ready() bool

	// Error returns the current error state, if any.
	Error() *IngesterError

	// SetError sets an error state on the ingester.
	SetError(reason IngesterErrorReason, msg string)

	// ClearError clears the error state.
	ClearError()
}

// CrossValidator validates cross-chain messages.
// Implementations include:
//   - SimpleCrossValidator: synchronous, no background loop
//   - BackgroundCrossValidator: runs background validation loop
type CrossValidator interface {
	// ValidateAccessEntry validates a single access list entry.
	ValidateAccessEntry(access types.Access, minSafety types.SafetyLevel, execDescriptor types.ExecutingDescriptor) error

	// CrossValidatedTimestamp returns the global cross-validated timestamp.
	CrossValidatedTimestamp() (uint64, bool)
}
