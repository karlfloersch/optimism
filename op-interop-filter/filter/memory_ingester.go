package filter

import (
	"sync"

	"github.com/ethereum-optimism/optimism/op-service/eth"
	"github.com/ethereum-optimism/optimism/op-supervisor/supervisor/types"
)

// MemoryChainIngester is an in-memory implementation of ChainIngester for testing.
// It stores logs in a map and provides simple state management.
type MemoryChainIngester struct {
	mu sync.RWMutex

	// Logs stored by their identifying query
	logs map[logKey]types.BlockSeal

	// Blocks with their executing messages
	blocks []BlockExecMsgs

	// State
	ready            bool
	err              *IngesterError
	latestBlock      eth.BlockID
	latestTimestamp  uint64
	earliestBlockNum uint64
}

// logKey uniquely identifies a log entry
type logKey struct {
	Timestamp uint64
	BlockNum  uint64
	LogIdx    uint32
	Checksum  types.MessageChecksum
}

// NewMemoryChainIngester creates a new in-memory chain ingester.
func NewMemoryChainIngester() *MemoryChainIngester {
	return &MemoryChainIngester{
		logs:   make(map[logKey]types.BlockSeal),
		blocks: make([]BlockExecMsgs, 0),
		ready:  true, // Default to ready for simple tests
	}
}

// AddLog adds a log entry to the ingester.
func (m *MemoryChainIngester) AddLog(timestamp, blockNum uint64, logIdx uint32, checksum types.MessageChecksum, seal types.BlockSeal) {
	m.mu.Lock()
	defer m.mu.Unlock()

	key := logKey{
		Timestamp: timestamp,
		BlockNum:  blockNum,
		LogIdx:    logIdx,
		Checksum:  checksum,
	}
	m.logs[key] = seal

	// Update latest block/timestamp if needed
	if blockNum > m.latestBlock.Number {
		m.latestBlock = eth.BlockID{Number: blockNum}
		m.latestTimestamp = timestamp
	}
	if m.earliestBlockNum == 0 || blockNum < m.earliestBlockNum {
		m.earliestBlockNum = blockNum
	}
}

// AddBlock adds a block with its executing messages.
func (m *MemoryChainIngester) AddBlock(block BlockExecMsgs) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.blocks = append(m.blocks, block)

	// Update latest block/timestamp if needed
	if block.BlockNum > m.latestBlock.Number {
		m.latestBlock = eth.BlockID{Number: block.BlockNum}
		m.latestTimestamp = block.Timestamp
	}
	if m.earliestBlockNum == 0 || block.BlockNum < m.earliestBlockNum {
		m.earliestBlockNum = block.BlockNum
	}
}

// SetReady sets the ready state.
func (m *MemoryChainIngester) SetReady(ready bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ready = ready
}

// Contains implements ChainIngester.
func (m *MemoryChainIngester) Contains(query types.ContainsQuery) (types.BlockSeal, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	key := logKey{
		Timestamp: query.Timestamp,
		BlockNum:  query.BlockNum,
		LogIdx:    query.LogIdx,
		Checksum:  query.Checksum,
	}

	seal, ok := m.logs[key]
	if !ok {
		return types.BlockSeal{}, types.ErrConflict
	}
	return seal, nil
}

// LatestBlock implements ChainIngester.
func (m *MemoryChainIngester) LatestBlock() (eth.BlockID, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.latestBlock.Number == 0 {
		return eth.BlockID{}, false
	}
	return m.latestBlock, true
}

// LatestTimestamp implements ChainIngester.
func (m *MemoryChainIngester) LatestTimestamp() (uint64, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.latestTimestamp == 0 {
		return 0, false
	}
	return m.latestTimestamp, true
}

// EarliestBlockNum implements ChainIngester.
func (m *MemoryChainIngester) EarliestBlockNum() (uint64, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.earliestBlockNum == 0 {
		return 0, false
	}
	return m.earliestBlockNum, true
}

// GetBlocksInRange implements ChainIngester.
func (m *MemoryChainIngester) GetBlocksInRange(startBlock, endBlock uint64) ([]BlockExecMsgs, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var result []BlockExecMsgs
	for _, block := range m.blocks {
		if block.BlockNum >= startBlock && block.BlockNum <= endBlock {
			result = append(result, block)
		}
	}
	return result, nil
}

// Ready implements ChainIngester.
func (m *MemoryChainIngester) Ready() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.ready
}

// Error implements ChainIngester.
func (m *MemoryChainIngester) Error() *IngesterError {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.err
}

// SetError implements ChainIngester.
func (m *MemoryChainIngester) SetError(reason IngesterErrorReason, msg string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.err = &IngesterError{
		Reason:  reason,
		Message: msg,
	}
}

// ClearError implements ChainIngester.
func (m *MemoryChainIngester) ClearError() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.err = nil
}

// Ensure MemoryChainIngester implements ChainIngester
var _ ChainIngester = (*MemoryChainIngester)(nil)
