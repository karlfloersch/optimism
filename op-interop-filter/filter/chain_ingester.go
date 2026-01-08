package filter

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	gethTypes "github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/log"

	"github.com/ethereum-optimism/optimism/op-interop-filter/metrics"
	"github.com/ethereum-optimism/optimism/op-service/client"
	"github.com/ethereum-optimism/optimism/op-service/eth"
	"github.com/ethereum-optimism/optimism/op-service/sources"
	"github.com/ethereum-optimism/optimism/op-supervisor/supervisor/backend/db/logs"
	"github.com/ethereum-optimism/optimism/op-supervisor/supervisor/backend/processors"
	"github.com/ethereum-optimism/optimism/op-supervisor/supervisor/types"
)

const (
	pollInterval = 2 * time.Second
)

// blockTimestampFetcher fetches a block's timestamp by block number.
// Returns (timestamp, error). Used for binary search to find blocks by timestamp.
type blockTimestampFetcher func(ctx context.Context, blockNum uint64) (uint64, error)

// findBlockByTimestamp uses binary search to find the first block with timestamp >= targetTimestamp.
// Parameters:
//   - ctx: context for cancellation
//   - targetTimestamp: the timestamp we're looking for
//   - latestBlockNum: the highest block number to search (typically chain head)
//   - fetchTimestamp: function to get a block's timestamp by number
//
// Returns the block number of the first block at or after targetTimestamp.
// If all blocks are after targetTimestamp, returns 1.
// If all blocks are before targetTimestamp, returns latestBlockNum.
func findBlockByTimestamp(
	ctx context.Context,
	targetTimestamp uint64,
	latestBlockNum uint64,
	fetchTimestamp blockTimestampFetcher,
) (uint64, error) {
	if latestBlockNum == 0 {
		return 1, nil
	}

	// Check if target is before the first block
	firstTimestamp, err := fetchTimestamp(ctx, 1)
	if err != nil {
		return 0, fmt.Errorf("failed to fetch block 1: %w", err)
	}
	if targetTimestamp <= firstTimestamp {
		return 1, nil
	}

	// Check if target is after the latest block
	latestTimestamp, err := fetchTimestamp(ctx, latestBlockNum)
	if err != nil {
		return 0, fmt.Errorf("failed to fetch block %d: %w", latestBlockNum, err)
	}
	if targetTimestamp > latestTimestamp {
		return latestBlockNum, nil
	}

	// Binary search for the first block with timestamp >= targetTimestamp
	low, high := uint64(1), latestBlockNum

	for low < high {
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		default:
		}

		mid := low + (high-low)/2

		midTimestamp, err := fetchTimestamp(ctx, mid)
		if err != nil {
			return 0, fmt.Errorf("failed to fetch block %d: %w", mid, err)
		}

		if midTimestamp < targetTimestamp {
			low = mid + 1
		} else {
			high = mid
		}
	}

	return low, nil
}

// reorgCallback is called when a reorg is detected
type reorgCallback func(chainID eth.ChainID)

// ChainIngester handles block ingestion and log storage for a single chain
type ChainIngester struct {
	log     log.Logger
	metrics metrics.Metricer
	chainID eth.ChainID

	rpcClient        client.RPC
	ethClient        *sources.EthClient
	logsDB           *logs.DB
	dataDir          string
	backfillDuration time.Duration

	ready   atomic.Bool
	stopped atomic.Bool

	onReorg reorgCallback

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
	mu     sync.RWMutex // protects logsDB access during rewind
}

// NewChainIngester creates a new ChainIngester for the given chain
func NewChainIngester(
	parentCtx context.Context,
	logger log.Logger,
	m metrics.Metricer,
	chainID eth.ChainID,
	rpcURL string,
	dataDir string,
	backfillDuration time.Duration,
	onReorg reorgCallback,
) (*ChainIngester, error) {
	ctx, cancel := context.WithCancel(parentCtx)

	logger = logger.New("chain", chainID)

	// Create RPC client
	rpcClient, err := client.NewRPC(ctx, logger, rpcURL)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("failed to create RPC client for chain %s: %w", chainID, err)
	}

	// Create eth client
	ethClient, err := sources.NewEthClient(
		rpcClient,
		logger,
		nil, // metrics
		&sources.EthClientConfig{
			ReceiptsCacheSize:     1000,
			TransactionsCacheSize: 1000,
			HeadersCacheSize:      1000,
			PayloadsCacheSize:     100,
			MaxRequestsPerBatch:   20,
			MaxConcurrentRequests: 10,
			TrustRPC:              false,
			MustBePostMerge:       true,
			RPCProviderKind:       sources.RPCKindStandard,
		},
	)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("failed to create eth client for chain %s: %w", chainID, err)
	}

	return &ChainIngester{
		log:              logger,
		metrics:          m,
		chainID:          chainID,
		rpcClient:        rpcClient,
		ethClient:        ethClient,
		dataDir:          dataDir,
		backfillDuration: backfillDuration,
		onReorg:          onReorg,
		ctx:              ctx,
		cancel:           cancel,
	}, nil
}

// Start begins block ingestion
func (c *ChainIngester) Start() error {
	c.log.Info("Starting chain ingester")

	// Initialize LogsDB
	if err := c.initLogsDB(); err != nil {
		return fmt.Errorf("failed to init logs DB: %w", err)
	}

	// Start ingestion goroutine
	c.wg.Add(1)
	go c.runIngestion()

	return nil
}

// Stop gracefully stops the chain ingester
func (c *ChainIngester) Stop() error {
	if !c.stopped.CompareAndSwap(false, true) {
		return nil
	}
	c.log.Info("Stopping chain ingester")
	c.cancel()
	c.wg.Wait()

	c.mu.Lock()
	defer c.mu.Unlock()

	// Close LogsDB
	if c.logsDB != nil {
		if err := c.logsDB.Close(); err != nil {
			return fmt.Errorf("failed to close logs DB: %w", err)
		}
	}

	// Close RPC clients
	if c.ethClient != nil {
		c.ethClient.Close()
	}
	if c.rpcClient != nil {
		c.rpcClient.Close()
	}

	return nil
}

// Ready returns true if backfill is complete
func (c *ChainIngester) Ready() bool {
	return c.ready.Load()
}

// ChainID returns the chain ID
func (c *ChainIngester) ChainID() eth.ChainID {
	return c.chainID
}

// Contains checks if a log exists in the database
func (c *ChainIngester) Contains(query types.ContainsQuery) (types.BlockSeal, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.logsDB == nil {
		return types.BlockSeal{}, types.ErrUninitialized
	}

	return c.logsDB.Contains(query)
}

// LatestBlock returns the latest sealed block
func (c *ChainIngester) LatestBlock() (eth.BlockID, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.logsDB == nil {
		return eth.BlockID{}, false
	}

	return c.logsDB.LatestSealedBlock()
}

// LatestTimestamp returns the timestamp of the latest sealed block
func (c *ChainIngester) LatestTimestamp() (uint64, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.logsDB == nil {
		return 0, false
	}

	latestBlock, ok := c.logsDB.LatestSealedBlock()
	if !ok {
		return 0, false
	}

	seal, err := c.logsDB.FindSealedBlock(latestBlock.Number)
	if err != nil {
		return 0, false
	}

	return seal.Timestamp, true
}

// initLogsDB initializes the logs database
func (c *ChainIngester) initLogsDB() error {
	var dbPath string
	if c.dataDir != "" {
		chainDir := filepath.Join(c.dataDir, fmt.Sprintf("chain-%s", c.chainID))
		if err := os.MkdirAll(chainDir, 0755); err != nil {
			return fmt.Errorf("failed to create chain directory: %w", err)
		}
		dbPath = filepath.Join(chainDir, "logs.db")
	} else {
		// Use temp directory if no data dir specified
		tempDir, err := os.MkdirTemp("", fmt.Sprintf("interop-filter-chain-%s-*", c.chainID))
		if err != nil {
			return fmt.Errorf("failed to create temp directory: %w", err)
		}
		dbPath = filepath.Join(tempDir, "logs.db")
		c.log.Warn("Using temporary directory for logs DB", "path", dbPath)
	}

	db, err := logs.NewFromFile(c.log, &logsDBMetrics{m: c.metrics, chainID: c.chainID}, c.chainID, dbPath, true)
	if err != nil {
		return fmt.Errorf("failed to open logs DB: %w", err)
	}

	c.mu.Lock()
	c.logsDB = db
	c.mu.Unlock()

	c.log.Info("Initialized logs DB", "path", dbPath)
	return nil
}

// runIngestion runs the main ingestion loop
func (c *ChainIngester) runIngestion() {
	defer c.wg.Done()

	if err := c.catchUp(); err != nil {
		if errors.Is(err, context.Canceled) {
			c.log.Info("Catch up canceled")
			return
		}
		c.log.Error("Failed to catch up", "err", err)
		return
	}

	c.ready.Store(true)
	c.log.Info("Caught up, starting live ingestion")
	c.pollLoop()
}

// catchUp syncs from the last known block to the current chain head.
// Returns nil if already caught up or after successful backfill.
func (c *ChainIngester) catchUp() error {
	head, err := c.ethClient.InfoByLabel(c.ctx, eth.Unsafe)
	if err != nil {
		return fmt.Errorf("failed to get current head: %w", err)
	}
	c.log.Info("Current chain head", "block", head.NumberU64(), "hash", head.Hash())

	// Check if DB has existing data (for restarts with persistent storage)
	c.mu.RLock()
	latestSealed, hasSealed := c.logsDB.LatestSealedBlock()
	c.mu.RUnlock()

	var startBlock uint64
	if hasSealed {
		startBlock = latestSealed.Number + 1
		c.log.Info("Resuming from existing DB", "lastSealed", latestSealed.Number, "resumeFrom", startBlock)

		// Already caught up
		if startBlock > head.NumberU64() {
			c.log.Info("DB is up to date")
			return nil
		}
	} else {
		// Fresh start - find the block at our target backfill timestamp using binary search
		backfillSeconds := uint64(c.backfillDuration / time.Second)
		var targetTimestamp uint64
		if head.Time() > backfillSeconds {
			targetTimestamp = head.Time() - backfillSeconds
		} // else targetTimestamp = 0, backfill from genesis

		fetchTimestamp := func(ctx context.Context, blockNum uint64) (uint64, error) {
			info, err := c.ethClient.InfoByNumber(ctx, blockNum)
			if err != nil {
				return 0, err
			}
			return info.Time(), nil
		}

		startBlock, err = findBlockByTimestamp(c.ctx, targetTimestamp, head.NumberU64(), fetchTimestamp)
		if err != nil {
			return fmt.Errorf("failed to find backfill start block: %w", err)
		}

		c.log.Info("Starting fresh backfill", "from", startBlock, "to", head.NumberU64(), "targetTimestamp", targetTimestamp, "blocks", head.NumberU64()-startBlock+1)

		// Initialize the LogsDB with the parent block of the start block
		if startBlock > 0 {
			if err := c.initializeAnchorBlock(startBlock - 1); err != nil {
				return fmt.Errorf("failed to initialize anchor block: %w", err)
			}
		}
	}

	// Backfill to head
	if err := c.backfill(startBlock, head.NumberU64()); err != nil {
		if !errors.Is(err, context.Canceled) {
			c.triggerReorg()
		}
		return err
	}

	return nil
}

// initializeAnchorBlock seals the anchor block in the LogsDB
// This must be done before any logs can be added, as logs reference their parent block
func (c *ChainIngester) initializeAnchorBlock(blockNum uint64) error {
	c.log.Info("Initializing anchor block", "block", blockNum)

	// Fetch the anchor block info
	blockInfo, err := c.ethClient.InfoByNumber(c.ctx, blockNum)
	if err != nil {
		return fmt.Errorf("failed to get anchor block info: %w", err)
	}

	blockID := eth.BlockID{Hash: blockInfo.Hash(), Number: blockInfo.NumberU64()}

	c.mu.Lock()
	defer c.mu.Unlock()

	// Seal the anchor block with no logs
	// For the very first block, use zero hash as parent
	parentHash := blockInfo.ParentHash()
	if err := c.logsDB.SealBlock(parentHash, blockID, blockInfo.Time()); err != nil {
		return fmt.Errorf("failed to seal anchor block: %w", err)
	}

	c.log.Info("Initialized anchor block", "block", blockNum, "hash", blockID.Hash)
	return nil
}

// backfill ingests blocks from startBlock to endBlock
func (c *ChainIngester) backfill(startBlock, endBlock uint64) error {
	totalBlocks := endBlock - startBlock + 1
	lastProgress := 0
	lastLog := time.Now()

	for blockNum := startBlock; blockNum <= endBlock; blockNum++ {
		select {
		case <-c.ctx.Done():
			return c.ctx.Err()
		default:
		}

		if err := c.ingestBlock(blockNum); err != nil {
			return fmt.Errorf("failed to ingest block %d: %w", blockNum, err)
		}

		// Progress reporting
		progress := int((blockNum - startBlock + 1) * 100 / totalBlocks)
		if progress >= lastProgress+10 || time.Since(lastLog) > 10*time.Second {
			c.log.Info("Backfill progress",
				"block", blockNum,
				"total", endBlock,
				"progress", fmt.Sprintf("%d%%", progress))
			lastProgress = progress
			lastLog = time.Now()
			// Record backfill progress metric
			chainIDUint64, _ := c.chainID.Uint64()
			c.metrics.RecordBackfillProgress(chainIDUint64, float64(progress)/100.0)
		}
	}

	return nil
}

// pollLoop polls for new blocks
func (c *ChainIngester) pollLoop() {
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-c.ctx.Done():
			return
		case <-ticker.C:
			if err := c.pollNewBlocks(); err != nil {
				if errors.Is(err, context.Canceled) {
					return
				}
				c.log.Error("Failed to poll new blocks", "err", err)
				// Don't trigger failsafe on transient errors
			}
		}
	}
}

// pollNewBlocks checks for and ingests new blocks
func (c *ChainIngester) pollNewBlocks() error {
	head, err := c.ethClient.InfoByLabel(c.ctx, eth.Unsafe)
	if err != nil {
		return fmt.Errorf("failed to get head: %w", err)
	}

	latestBlock, ok := c.LatestBlock()
	if !ok {
		return fmt.Errorf("no latest block in DB")
	}

	// Check for reorg
	if head.NumberU64() < latestBlock.Number {
		c.log.Warn("Detected reorg: head is behind latest block",
			"head", head.NumberU64(),
			"latest", latestBlock.Number)
		c.triggerReorg()
		return nil
	}

	// Ingest any missing blocks
	for blockNum := latestBlock.Number + 1; blockNum <= head.NumberU64(); blockNum++ {
		if err := c.ingestBlock(blockNum); err != nil {
			return fmt.Errorf("failed to ingest block %d: %w", blockNum, err)
		}
	}

	return nil
}

// ingestBlock ingests a single block
func (c *ChainIngester) ingestBlock(blockNum uint64) error {
	// Fetch block info
	blockInfo, err := c.ethClient.InfoByNumber(c.ctx, blockNum)
	if err != nil {
		return fmt.Errorf("failed to get block info: %w", err)
	}

	// Construct block ID
	blockID := eth.BlockID{Hash: blockInfo.Hash(), Number: blockInfo.NumberU64()}

	// Fetch receipts
	_, receipts, err := c.ethClient.FetchReceipts(c.ctx, blockInfo.Hash())
	if err != nil {
		return fmt.Errorf("failed to get receipts: %w", err)
	}

	// Check for reorg (parent hash mismatch)
	c.mu.RLock()
	latestBlock, hasLatest := c.logsDB.LatestSealedBlock()
	c.mu.RUnlock()

	if hasLatest && blockNum == latestBlock.Number+1 {
		if blockInfo.ParentHash() != latestBlock.Hash {
			c.log.Warn("Detected reorg: parent hash mismatch",
				"block", blockNum,
				"expected_parent", latestBlock.Hash,
				"actual_parent", blockInfo.ParentHash())
			c.triggerReorg()
			return nil
		}
	}

	// Process logs and add to DB under lock
	// We explicitly manage lock/unlock to avoid fragile defer patterns
	result, err := c.processBlockLogs(blockInfo, blockID, receipts, blockNum)
	if err != nil {
		return err
	}

	// Handle reorg detection (callback runs without lock)
	if result.needsReorg {
		c.triggerReorg()
		return nil
	}

	// Update metrics (no lock needed)
	chainIDUint64, _ := c.chainID.Uint64()
	c.metrics.RecordChainHead(chainIDUint64, blockNum)
	c.metrics.RecordBlocksSealed(chainIDUint64, 1)
	c.metrics.RecordLogsAdded(chainIDUint64, int64(result.logCount))

	return nil
}

// blockLogsResult holds the result of processing block logs
type blockLogsResult struct {
	logCount   uint32
	needsReorg bool
}

// processBlockLogs processes all logs in a block and adds them to the DB
// Returns the result containing log count and reorg flag
// Handles locking internally - caller should not hold the lock
func (c *ChainIngester) processBlockLogs(blockInfo eth.BlockInfo, blockID eth.BlockID,
	receipts gethTypes.Receipts, blockNum uint64) (blockLogsResult, error) {

	c.mu.Lock()
	defer c.mu.Unlock()

	var logIndex uint32

	// Get parent block ID for AddLog
	parentBlock := eth.BlockID{Hash: blockInfo.ParentHash(), Number: blockNum - 1}
	if blockNum == 0 {
		parentBlock = eth.BlockID{}
	}

	for _, receipt := range receipts {
		for _, l := range receipt.Logs {
			// Compute log hash
			logHash := processors.LogToLogHash(l)

			// Check if this is an executing message
			// Note: DecodeExecutingMessageLog returns (nil, nil) for non-executing-message logs,
			// but returns an error if a log LOOKS like an executing message but can't be decoded.
			// Per supervisor behavior, we treat decode errors as hard failures.
			execMsg, err := processors.DecodeExecutingMessageLog(l)
			if err != nil {
				return blockLogsResult{}, fmt.Errorf("invalid log %d in block %d: %w", l.Index, blockNum, err)
			}

			// Add log to DB
			if err := c.logsDB.AddLog(logHash, parentBlock, logIndex, execMsg); err != nil {
				// Check for conflict (reorg indicator)
				if errors.Is(err, types.ErrConflict) {
					c.log.Warn("Conflict adding log, detected reorg", "err", err)
					return blockLogsResult{needsReorg: true}, nil
				}
				return blockLogsResult{}, fmt.Errorf("failed to add log: %w", err)
			}
			logIndex++
		}
	}

	// Seal the block
	if err := c.logsDB.SealBlock(blockInfo.ParentHash(), blockID, blockInfo.Time()); err != nil {
		if errors.Is(err, types.ErrConflict) {
			c.log.Warn("Conflict sealing block, detected reorg", "err", err)
			return blockLogsResult{needsReorg: true}, nil
		}
		return blockLogsResult{}, fmt.Errorf("failed to seal block: %w", err)
	}

	return blockLogsResult{logCount: logIndex}, nil
}

// triggerReorg handles reorg detection
func (c *ChainIngester) triggerReorg() {
	c.log.Warn("Reorg detected, triggering failsafe")
	chainIDUint64, _ := c.chainID.Uint64()
	c.metrics.RecordReorgDetected(chainIDUint64)
	if c.onReorg != nil {
		c.onReorg(c.chainID)
	}
}

// blockExecMsgs contains executing messages from a single block
type blockExecMsgs struct {
	BlockNum  uint64
	Timestamp uint64
	ExecMsgs  []*types.ExecutingMessage // May be nil/empty if block has no executing messages
}

// GetBlocksInRange returns block info for all blocks from startBlock to endBlock (inclusive).
// This is used for on-demand cross-unsafe validation.
func (c *ChainIngester) GetBlocksInRange(startBlock, endBlock uint64) ([]blockExecMsgs, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.logsDB == nil {
		return nil, types.ErrUninitialized
	}

	results := make([]blockExecMsgs, 0, endBlock-startBlock+1)
	for blockNum := startBlock; blockNum <= endBlock; blockNum++ {
		ref, _, execMsgs, err := c.logsDB.OpenBlock(blockNum)
		if err != nil {
			return nil, fmt.Errorf("failed to open block %d: %w", blockNum, err)
		}

		// Convert map to slice (may be empty)
		var msgs []*types.ExecutingMessage
		if len(execMsgs) > 0 {
			msgs = make([]*types.ExecutingMessage, 0, len(execMsgs))
			for _, msg := range execMsgs {
				msgs = append(msgs, msg)
			}
		}
		results = append(results, blockExecMsgs{
			BlockNum:  blockNum,
			Timestamp: ref.Time,
			ExecMsgs:  msgs,
		})
	}

	return results, nil
}

// logsDBMetrics implements the logs.Metrics interface
type logsDBMetrics struct {
	m       metrics.Metricer
	chainID eth.ChainID
}

func (l *logsDBMetrics) RecordDBEntryCount(kind string, count int64) {
	// Could add more detailed metrics here if needed
}

func (l *logsDBMetrics) RecordDBSearchEntriesRead(count int64) {
	// Could add more detailed metrics here if needed
}
