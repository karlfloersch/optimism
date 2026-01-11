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

	"github.com/ethereum/go-ethereum/common"
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

// blockTimestampFetcher fetches a block's timestamp by block number.
type blockTimestampFetcher func(ctx context.Context, blockNum uint64) (uint64, error)

// findBlockByTimestamp uses binary search to find the first block with timestamp >= targetTimestamp.
func findBlockByTimestamp(
	ctx context.Context,
	targetTimestamp uint64,
	latestBlockNum uint64,
	fetchTimestamp blockTimestampFetcher,
) (uint64, error) {
	if latestBlockNum == 0 {
		return 1, nil
	}

	firstTimestamp, err := fetchTimestamp(ctx, 1)
	if err != nil {
		return 0, fmt.Errorf("failed to fetch block 1: %w", err)
	}
	if targetTimestamp <= firstTimestamp {
		return 1, nil
	}

	latestTimestamp, err := fetchTimestamp(ctx, latestBlockNum)
	if err != nil {
		return 0, fmt.Errorf("failed to fetch block %d: %w", latestBlockNum, err)
	}
	if targetTimestamp > latestTimestamp {
		return latestBlockNum, nil
	}

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

// LogsDBChainIngester handles block ingestion and log storage for a single chain.
// It uses an RPC client to fetch blocks and a sqlite-based logs database for storage.
type LogsDBChainIngester struct {
	log     log.Logger
	metrics metrics.Metricer
	chainID eth.ChainID

	rpcClient        client.RPC
	ethClient        *sources.EthClient
	logsDB           *logs.DB
	dataDir          string
	backfillDuration time.Duration
	pollInterval     time.Duration

	ready   atomic.Bool
	stopped atomic.Bool

	errorState atomic.Pointer[IngesterError]

	earliestBlockNum atomic.Uint64
	earliestBlockSet atomic.Bool

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
	mu     sync.RWMutex
}

// NewLogsDBChainIngester creates a new LogsDBChainIngester for the given chain.
func NewLogsDBChainIngester(
	parentCtx context.Context,
	logger log.Logger,
	m metrics.Metricer,
	chainID eth.ChainID,
	rpcURL string,
	dataDir string,
	backfillDuration time.Duration,
	pollInterval time.Duration,
) (*LogsDBChainIngester, error) {
	ctx, cancel := context.WithCancel(parentCtx)

	logger = logger.New("chain", chainID)

	rpcClient, err := client.NewRPC(ctx, logger, rpcURL)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("failed to create RPC client for chain %s: %w", chainID, err)
	}

	ethClient, err := sources.NewEthClient(
		rpcClient,
		logger,
		nil,
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
		rpcClient.Close()
		cancel()
		return nil, fmt.Errorf("failed to create eth client for chain %s: %w", chainID, err)
	}

	return &LogsDBChainIngester{
		log:              logger,
		metrics:          m,
		chainID:          chainID,
		rpcClient:        rpcClient,
		ethClient:        ethClient,
		dataDir:          dataDir,
		backfillDuration: backfillDuration,
		pollInterval:     pollInterval,
		ctx:              ctx,
		cancel:           cancel,
	}, nil
}

// Start begins block ingestion
func (c *LogsDBChainIngester) Start() error {
	c.log.Info("Starting chain ingester")

	if err := c.initLogsDB(); err != nil {
		return fmt.Errorf("failed to init logs DB: %w", err)
	}

	c.wg.Add(1)
	go c.runIngestion()

	return nil
}

// Stop gracefully stops the chain ingester
func (c *LogsDBChainIngester) Stop() error {
	if !c.stopped.CompareAndSwap(false, true) {
		return nil
	}
	c.log.Info("Stopping chain ingester")
	c.cancel()
	c.wg.Wait()

	c.mu.Lock()
	defer c.mu.Unlock()

	if c.logsDB != nil {
		if err := c.logsDB.Close(); err != nil {
			return fmt.Errorf("failed to close logs DB: %w", err)
		}
	}

	if c.ethClient != nil {
		c.ethClient.Close()
	}
	if c.rpcClient != nil {
		c.rpcClient.Close()
	}

	return nil
}

// Ready returns true if backfill is complete
func (c *LogsDBChainIngester) Ready() bool {
	return c.ready.Load()
}

// SetError sets the error state, logs the error, and records metrics.
func (c *LogsDBChainIngester) SetError(reason IngesterErrorReason, msg string) {
	err := &IngesterError{
		Reason:    reason,
		Message:   msg,
		Timestamp: time.Now(),
	}
	c.errorState.Store(err)
	c.log.Error("Ingester halted", "reason", reason.String(), "msg", msg)

	chainIDUint64, _ := c.chainID.Uint64()
	if reason == ErrorReorg || reason == ErrorConflict {
		c.metrics.RecordReorgDetected(chainIDUint64)
	}
}

// Error returns the current error state, or nil if no error.
func (c *LogsDBChainIngester) Error() *IngesterError {
	return c.errorState.Load()
}

// ClearError clears the error state.
func (c *LogsDBChainIngester) ClearError() {
	c.errorState.Store(nil)
	c.log.Info("Ingester error state cleared")
}

// Contains checks if a log exists in the database
func (c *LogsDBChainIngester) Contains(query types.ContainsQuery) (types.BlockSeal, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.logsDB == nil {
		return types.BlockSeal{}, types.ErrUninitialized
	}

	return c.logsDB.Contains(query)
}

// LatestBlock returns the latest sealed block
func (c *LogsDBChainIngester) LatestBlock() (eth.BlockID, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.logsDB == nil {
		return eth.BlockID{}, false
	}

	return c.logsDB.LatestSealedBlock()
}

// BlockHashAt returns the hash of the sealed block at the given height.
func (c *LogsDBChainIngester) BlockHashAt(blockNum uint64) (common.Hash, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.logsDB == nil {
		return common.Hash{}, false
	}

	seal, err := c.logsDB.FindSealedBlock(blockNum)
	if err != nil {
		return common.Hash{}, false
	}

	return seal.Hash, true
}

// LatestTimestamp returns the timestamp of the latest sealed block
func (c *LogsDBChainIngester) LatestTimestamp() (uint64, bool) {
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

// EarliestBlockNum returns the earliest block number in the logsDB.
func (c *LogsDBChainIngester) EarliestBlockNum() (uint64, bool) {
	if !c.earliestBlockSet.Load() {
		return 0, false
	}
	return c.earliestBlockNum.Load(), true
}

// GetExecMsgsAtTimestamp returns executing messages with the given inclusion timestamp.
func (c *LogsDBChainIngester) GetExecMsgsAtTimestamp(timestamp uint64) ([]IncludedMessage, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.logsDB == nil {
		return nil, types.ErrUninitialized
	}

	earliest := c.earliestBlockNum.Load()
	latestBlock, ok := c.logsDB.LatestSealedBlock()
	if earliest == 0 || !ok {
		return nil, nil
	}

	var results []IncludedMessage
	for blockNum := earliest; blockNum <= latestBlock.Number; blockNum++ {
		ref, _, execMsgs, err := c.logsDB.OpenBlock(blockNum)
		if err != nil {
			return nil, fmt.Errorf("failed to open block %d: %w", blockNum, err)
		}

		if ref.Time == timestamp {
			for _, msg := range execMsgs {
				results = append(results, IncludedMessage{
					ExecutingMessage:   msg,
					InclusionBlockNum:  blockNum,
					InclusionTimestamp: ref.Time,
				})
			}
		}

		// Timestamps increase, so we can stop early
		if ref.Time > timestamp {
			break
		}
	}

	return results, nil
}

func (c *LogsDBChainIngester) findAndSetEarliestBlock(latestBlock uint64) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	earliest := latestBlock
	for blockNum := latestBlock; blockNum > 0; blockNum-- {
		_, err := c.logsDB.FindSealedBlock(blockNum - 1)
		if err != nil {
			earliest = blockNum
			break
		}
		earliest = blockNum - 1
	}

	c.earliestBlockNum.Store(earliest)
	c.earliestBlockSet.Store(true)
	c.log.Info("Found earliest block in DB", "block", earliest)
}

func (c *LogsDBChainIngester) initLogsDB() error {
	var dbPath string
	if c.dataDir != "" {
		chainDir := filepath.Join(c.dataDir, fmt.Sprintf("chain-%s", c.chainID))
		if err := os.MkdirAll(chainDir, 0755); err != nil {
			return fmt.Errorf("failed to create chain directory: %w", err)
		}
		dbPath = filepath.Join(chainDir, "logs.db")
	} else {
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

func (c *LogsDBChainIngester) runIngestion() {
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

	ticker := time.NewTicker(c.pollInterval)
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
			}
		}
	}
}

func (c *LogsDBChainIngester) catchUp() error {
	head, err := c.ethClient.InfoByLabel(c.ctx, eth.Unsafe)
	if err != nil {
		return fmt.Errorf("failed to get current head: %w", err)
	}
	c.log.Info("Current chain head", "block", head.NumberU64(), "hash", head.Hash())

	startBlock, isFreshStart, err := c.determineStartBlock(head)
	if err != nil {
		return err
	}
	if startBlock == 0 {
		return nil
	}

	if isFreshStart && startBlock > 0 {
		if err := c.sealParentBlock(startBlock - 1); err != nil {
			return fmt.Errorf("failed to seal parent block: %w", err)
		}
	}

	return c.backfill(startBlock, head.NumberU64())
}

func (c *LogsDBChainIngester) determineStartBlock(head eth.BlockInfo) (startBlock uint64, isFreshStart bool, err error) {
	c.mu.RLock()
	latestSealed, hasSealed := c.logsDB.LatestSealedBlock()
	c.mu.RUnlock()

	if hasSealed {
		startBlock = latestSealed.Number + 1
		c.log.Info("Resuming from existing DB", "lastSealed", latestSealed.Number, "resumeFrom", startBlock)

		if !c.earliestBlockSet.Load() {
			c.findAndSetEarliestBlock(latestSealed.Number)
		}

		if startBlock > head.NumberU64() {
			c.log.Info("DB is up to date")
			return 0, false, nil
		}
		return startBlock, false, nil
	}

	startBlock, err = c.findBackfillStartBlock(head)
	if err != nil {
		return 0, false, fmt.Errorf("failed to find backfill start block: %w", err)
	}

	c.log.Info("Starting fresh backfill",
		"from", startBlock,
		"to", head.NumberU64(),
		"blocks", head.NumberU64()-startBlock+1)

	return startBlock, true, nil
}

func (c *LogsDBChainIngester) findBackfillStartBlock(head eth.BlockInfo) (uint64, error) {
	backfillSeconds := uint64(c.backfillDuration / time.Second)
	var targetTimestamp uint64
	if head.Time() > backfillSeconds {
		targetTimestamp = head.Time() - backfillSeconds
	}

	fetchTimestamp := func(ctx context.Context, blockNum uint64) (uint64, error) {
		info, err := c.ethClient.InfoByNumber(ctx, blockNum)
		if err != nil {
			return 0, err
		}
		return info.Time(), nil
	}

	return findBlockByTimestamp(c.ctx, targetTimestamp, head.NumberU64(), fetchTimestamp)
}

func (c *LogsDBChainIngester) sealParentBlock(blockNum uint64) error {
	c.log.Info("Sealing parent block as starting point", "block", blockNum)

	blockInfo, err := c.ethClient.InfoByNumber(c.ctx, blockNum)
	if err != nil {
		return fmt.Errorf("failed to get block info: %w", err)
	}

	blockID := eth.BlockID{Hash: blockInfo.Hash(), Number: blockInfo.NumberU64()}

	c.mu.Lock()
	defer c.mu.Unlock()

	parentHash := blockInfo.ParentHash()
	if err := c.logsDB.SealBlock(parentHash, blockID, blockInfo.Time()); err != nil {
		return fmt.Errorf("failed to seal block: %w", err)
	}

	c.earliestBlockNum.Store(blockNum + 1)
	c.earliestBlockSet.Store(true)

	c.log.Info("Sealed parent block", "block", blockNum, "hash", blockID.Hash)
	return nil
}

func (c *LogsDBChainIngester) backfill(startBlock, endBlock uint64) error {
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

		progress := int((blockNum - startBlock + 1) * 100 / totalBlocks)
		if progress >= lastProgress+10 || time.Since(lastLog) > 10*time.Second {
			c.log.Info("Backfill progress",
				"block", blockNum,
				"total", endBlock,
				"progress", fmt.Sprintf("%d%%", progress))
			lastProgress = progress
			lastLog = time.Now()
			chainIDUint64, _ := c.chainID.Uint64()
			c.metrics.RecordBackfillProgress(chainIDUint64, float64(progress)/100.0)
		}
	}

	return nil
}

func (c *LogsDBChainIngester) pollNewBlocks() error {
	if c.Error() != nil {
		return nil
	}

	head, err := c.ethClient.InfoByLabel(c.ctx, eth.Unsafe)
	if err != nil {
		return fmt.Errorf("failed to get head: %w", err)
	}

	latestBlock, ok := c.LatestBlock()
	if !ok {
		return fmt.Errorf("no latest block in DB")
	}

	if head.NumberU64() < latestBlock.Number {
		dbHash, ok := c.BlockHashAt(head.NumberU64())
		if !ok {
			c.log.Warn("Head moved backward, couldn't verify block hash",
				"head", head.NumberU64(), "latest", latestBlock.Number)
			return nil
		}

		if dbHash == head.Hash() {
			c.log.Debug("Head temporarily behind, same hash - skipping",
				"head", head.NumberU64(), "latest", latestBlock.Number)
			return nil
		}

		c.log.Warn("Detected reorg: different block at same height",
			"height", head.NumberU64(), "db_hash", dbHash, "head_hash", head.Hash())
		c.SetError(ErrorReorg, fmt.Sprintf("reorg at height %d: db has %s, chain has %s",
			head.NumberU64(), dbHash, head.Hash()))
		return nil
	}

	for blockNum := latestBlock.Number + 1; blockNum <= head.NumberU64(); blockNum++ {
		if err := c.ingestBlock(blockNum); err != nil {
			return fmt.Errorf("failed to ingest block %d: %w", blockNum, err)
		}
	}

	return nil
}

func (c *LogsDBChainIngester) ingestBlock(blockNum uint64) error {
	if c.Error() != nil {
		return nil
	}

	blockInfo, err := c.ethClient.InfoByNumber(c.ctx, blockNum)
	if err != nil {
		return fmt.Errorf("failed to get block info: %w", err)
	}

	blockID := eth.BlockID{Hash: blockInfo.Hash(), Number: blockInfo.NumberU64()}

	_, receipts, err := c.ethClient.FetchReceipts(c.ctx, blockInfo.Hash())
	if err != nil {
		return fmt.Errorf("failed to get receipts: %w", err)
	}

	c.mu.RLock()
	latestBlock, hasLatest := c.logsDB.LatestSealedBlock()
	c.mu.RUnlock()

	if hasLatest && blockNum == latestBlock.Number+1 {
		if blockInfo.ParentHash() != latestBlock.Hash {
			c.log.Warn("Detected reorg: parent hash mismatch",
				"block", blockNum,
				"expected_parent", latestBlock.Hash,
				"actual_parent", blockInfo.ParentHash())
			c.SetError(ErrorReorg, fmt.Sprintf("parent hash mismatch at block %d", blockNum))
			return nil
		}
	}

	logCount, err := c.processBlockLogs(blockInfo, blockID, receipts, blockNum)
	if err != nil {
		if errors.Is(err, types.ErrConflict) {
			c.SetError(ErrorConflict, fmt.Sprintf("database conflict at block %d", blockNum))
			return nil
		}
		return err
	}

	chainIDUint64, _ := c.chainID.Uint64()
	c.metrics.RecordChainHead(chainIDUint64, blockNum)
	c.metrics.RecordBlocksSealed(chainIDUint64, 1)
	c.metrics.RecordLogsAdded(chainIDUint64, int64(logCount))

	return nil
}

func (c *LogsDBChainIngester) processBlockLogs(blockInfo eth.BlockInfo, blockID eth.BlockID,
	receipts gethTypes.Receipts, blockNum uint64) (uint32, error) {

	c.mu.Lock()
	defer c.mu.Unlock()

	var logIndex uint32

	parentBlock := eth.BlockID{Hash: blockInfo.ParentHash(), Number: blockNum - 1}
	if blockNum == 0 {
		parentBlock = eth.BlockID{}
	}

	for _, receipt := range receipts {
		for _, l := range receipt.Logs {
			logHash := processors.LogToLogHash(l)

			execMsg, err := processors.DecodeExecutingMessageLog(l)
			if err != nil {
				return 0, fmt.Errorf("invalid log %d in block %d: %w", l.Index, blockNum, err)
			}

			if err := c.logsDB.AddLog(logHash, parentBlock, logIndex, execMsg); err != nil {
				return 0, fmt.Errorf("failed to add log: %w", err)
			}
			logIndex++
		}
	}

	if err := c.logsDB.SealBlock(blockInfo.ParentHash(), blockID, blockInfo.Time()); err != nil {
		return 0, fmt.Errorf("failed to seal block: %w", err)
	}

	return logIndex, nil
}

// logsDBMetrics implements the logs.Metrics interface
type logsDBMetrics struct {
	m       metrics.Metricer
	chainID eth.ChainID
}

func (l *logsDBMetrics) RecordDBEntryCount(kind string, count int64) {}

func (l *logsDBMetrics) RecordDBSearchEntriesRead(count int64) {}

// Ensure LogsDBChainIngester implements ChainIngester
var _ ChainIngester = (*LogsDBChainIngester)(nil)
