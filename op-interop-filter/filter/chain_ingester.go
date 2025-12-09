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
	gethtypes "github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/params"

	"github.com/ethereum-optimism/optimism/op-interop-filter/metrics"
	"github.com/ethereum-optimism/optimism/op-service/client"
	"github.com/ethereum-optimism/optimism/op-service/eth"
	"github.com/ethereum-optimism/optimism/op-service/sources"
	"github.com/ethereum-optimism/optimism/op-supervisor/supervisor/backend/db/logs"
	"github.com/ethereum-optimism/optimism/op-supervisor/supervisor/backend/processors"
	"github.com/ethereum-optimism/optimism/op-supervisor/supervisor/backend/reads"
	suptypes "github.com/ethereum-optimism/optimism/op-supervisor/supervisor/types"
)

const (
	// defaultBlockTime is used to estimate blocks per hour if we can't determine it
	defaultBlockTime = 2 * time.Second
	// httpPollInterval for polling new blocks
	httpPollInterval = 2 * time.Second
)

// ReorgCallback is called when a reorg is detected
type ReorgCallback func(chainID eth.ChainID, err error)

// ChainIngester manages block ingestion and LogsDB for a single chain
type ChainIngester struct {
	log     log.Logger
	metrics metrics.Metricer
	chainID eth.ChainID
	rpcURL  string
	cfg     *Config

	rpcClient client.RPC
	ethClient *sources.EthClient
	logsDB    *logs.DB

	ready   atomic.Bool
	stopped atomic.Bool

	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// NewChainIngester creates a new ChainIngester instance
func NewChainIngester(ctx context.Context, logger log.Logger, m metrics.Metricer,
	chainID eth.ChainID, rpcURL string, cfg *Config) (*ChainIngester, error) {

	c := &ChainIngester{
		log:     logger.New("chainID", chainID),
		metrics: m,
		chainID: chainID,
		rpcURL:  rpcURL,
		cfg:     cfg,
	}
	return c, nil
}

// Start starts the chain ingester
func (c *ChainIngester) Start(ctx context.Context, onReorg ReorgCallback) error {
	c.log.Info("Starting chain ingester")

	// Create RPC client
	rpcClient, err := client.NewRPC(ctx, c.log, c.rpcURL,
		client.WithHttpPollInterval(httpPollInterval))
	if err != nil {
		return fmt.Errorf("failed to create RPC client: %w", err)
	}
	c.rpcClient = rpcClient

	// Create eth client for fetching blocks/receipts
	ethClient, err := sources.NewEthClient(
		rpcClient,
		c.log,
		nil, // no metrics for now
		&sources.EthClientConfig{
			MaxRequestsPerBatch:   20,
			MaxConcurrentRequests: 10,
			TrustRPC:              true,
			MustBePostMerge:       false,
			RPCProviderKind:       sources.RPCKindBasic,
			ReceiptsCacheSize:     100,
			TransactionsCacheSize: 100,
			HeadersCacheSize:      100,
			PayloadsCacheSize:     100,
			BlockRefsCacheSize:    100,
		},
	)
	if err != nil {
		return fmt.Errorf("failed to create eth client: %w", err)
	}
	c.ethClient = ethClient

	// Initialize LogsDB
	if err := c.initLogsDB(); err != nil {
		return fmt.Errorf("failed to init LogsDB: %w", err)
	}

	// Start ingestion in background
	ctx, cancel := context.WithCancel(ctx)
	c.cancel = cancel

	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		c.runIngestion(ctx, onReorg)
	}()

	return nil
}

// Stop stops the chain ingester
func (c *ChainIngester) Stop(ctx context.Context) error {
	if !c.stopped.CompareAndSwap(false, true) {
		return nil
	}
	c.log.Info("Stopping chain ingester")
	if c.cancel != nil {
		c.cancel()
	}
	c.wg.Wait()
	if c.ethClient != nil {
		c.ethClient.Close()
	}
	if c.logsDB != nil {
		c.logsDB.Close()
	}
	return nil
}

// Ready returns whether backfill is complete
func (c *ChainIngester) Ready() bool {
	return c.ready.Load()
}

// Contains validates that a log exists in the LogsDB
func (c *ChainIngester) Contains(access suptypes.Access) error {
	query := suptypes.ContainsQuery{
		Timestamp: access.Timestamp,
		BlockNum:  access.BlockNumber,
		LogIdx:    access.LogIndex,
		Checksum:  access.Checksum,
	}
	_, err := c.logsDB.Contains(query)
	return err
}

func (c *ChainIngester) initLogsDB() error {
	chainIDUint, _ := c.chainID.Uint64()

	var dbPath string
	if c.cfg.DataDir != "" {
		chainDir := filepath.Join(c.cfg.DataDir, fmt.Sprintf("chain-%d", chainIDUint))
		if err := os.MkdirAll(chainDir, 0755); err != nil {
			return fmt.Errorf("failed to create chain dir: %w", err)
		}
		dbPath = filepath.Join(chainDir, "logs.db")
	} else {
		// Use fresh temp directory if no data dir specified
		// Remove any stale data from previous runs
		tmpDir := filepath.Join(os.TempDir(), "op-interop-filter", fmt.Sprintf("chain-%d", chainIDUint))
		if err := os.RemoveAll(tmpDir); err != nil {
			c.log.Warn("Failed to clean temp dir", "path", tmpDir, "err", err)
		}
		if err := os.MkdirAll(tmpDir, 0755); err != nil {
			return fmt.Errorf("failed to create temp dir: %w", err)
		}
		dbPath = filepath.Join(tmpDir, "logs.db")
		c.log.Info("Using temporary directory for LogsDB", "path", tmpDir)
	}

	// Create LogsDB
	dbMetrics := &logsDBMetrics{chainID: chainIDUint, m: c.metrics}
	db, err := logs.NewFromFile(c.log, dbMetrics, c.chainID, dbPath, true)
	if err != nil {
		return err
	}
	c.logsDB = db
	return nil
}

func (c *ChainIngester) runIngestion(ctx context.Context, onReorg ReorgCallback) {
	c.log.Info("Starting block ingestion")

	// Get current head
	head, err := c.ethClient.InfoByLabel(ctx, eth.Unsafe)
	if err != nil {
		c.log.Error("Failed to get head block", "err", err)
		onReorg(c.chainID, fmt.Errorf("failed to get head: %w", err))
		return
	}
	c.log.Info("Current head", "number", head.NumberU64(), "hash", head.Hash())

	// Calculate start block (24 hours back)
	startBlock := c.calculateStartBlock(head)
	c.log.Info("Backfill range", "start", startBlock, "end", head.NumberU64())

	// Initialize LogsDB with starting block
	if err := c.initializeLogsDBFromBlock(ctx, startBlock); err != nil {
		c.log.Error("Failed to initialize LogsDB", "err", err)
		onReorg(c.chainID, fmt.Errorf("failed to initialize LogsDB: %w", err))
		return
	}

	// Record the starting block
	chainIDUint, _ := c.chainID.Uint64()
	c.metrics.RecordLogsDBFirstBlock(chainIDUint, startBlock)

	// Backfill historical blocks
	headNum := head.NumberU64()
	if err := c.backfill(ctx, startBlock, headNum); err != nil {
		c.log.Error("Backfill failed", "err", err)
		onReorg(c.chainID, fmt.Errorf("backfill failed: %w", err))
		return
	}

	c.log.Info("Backfill complete, starting live ingestion")
	c.ready.Store(true)
	c.metrics.RecordChainReady(chainIDUint, true)

	// Subscribe to new blocks, starting from the head we backfilled to
	c.subscribeNewBlocks(ctx, onReorg, headNum)
}

func (c *ChainIngester) calculateStartBlock(head eth.BlockInfo) uint64 {
	// Estimate blocks in backfill period based on configured duration
	backfillBlocks := uint64(c.cfg.BackfillDuration / defaultBlockTime)

	headNum := head.NumberU64()
	if headNum <= backfillBlocks {
		return 1 // Start from block 1 (not genesis)
	}
	return headNum - backfillBlocks
}

func (c *ChainIngester) initializeLogsDBFromBlock(ctx context.Context, blockNum uint64) error {
	// Get the block before our start block to use as the "sealed" starting point
	if blockNum == 0 {
		blockNum = 1
	}
	startBlockNum := blockNum - 1

	block, err := c.ethClient.InfoByNumber(ctx, startBlockNum)
	if err != nil {
		return fmt.Errorf("failed to get start block %d: %w", startBlockNum, err)
	}

	// Seal this block as the starting point for an empty DB
	// When the DB is empty, SealBlock accepts any block to initialize it
	blockID := eth.BlockID{Hash: block.Hash(), Number: startBlockNum}
	return c.logsDB.SealBlock(block.ParentHash(), blockID, block.Time())
}

func (c *ChainIngester) backfill(ctx context.Context, startBlock, endBlock uint64) error {
	total := endBlock - startBlock + 1
	c.log.Info("Starting backfill", "blocks", total, "from", startBlock, "to", endBlock)

	chainIDUint, _ := c.chainID.Uint64()

	// Log every 10% or every 100 blocks, whichever is more frequent
	logInterval := total / 10
	if logInterval < 100 {
		logInterval = 100
	}
	if logInterval > 1000 {
		logInterval = 1000
	}

	lastLogTime := time.Now()

	for blockNum := startBlock; blockNum <= endBlock; blockNum++ {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		if err := c.ingestBlock(ctx, blockNum); err != nil {
			return fmt.Errorf("failed to ingest block %d: %w", blockNum, err)
		}

		// Update progress
		progress := blockNum - startBlock + 1
		c.metrics.RecordBackfillProgress(chainIDUint, progress, total)

		// Log progress periodically (by count or time)
		if progress%logInterval == 0 || time.Since(lastLogTime) > 10*time.Second {
			c.log.Info("Backfill progress",
				"block", blockNum,
				"progress", fmt.Sprintf("%d/%d (%.1f%%)", progress, total, float64(progress)/float64(total)*100))
			lastLogTime = time.Now()
		}
	}

	return nil
}

func (c *ChainIngester) subscribeNewBlocks(ctx context.Context, onReorg ReorgCallback, startBlock uint64) {
	// Simple polling loop for new blocks
	ticker := time.NewTicker(httpPollInterval)
	defer ticker.Stop()

	lastBlock := startBlock
	chainIDUint, _ := c.chainID.Uint64()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			head, err := c.ethClient.InfoByLabel(ctx, eth.Unsafe)
			if err != nil {
				c.log.Warn("Failed to get head", "err", err)
				continue
			}

			headNum := head.NumberU64()
			if headNum <= lastBlock {
				continue
			}

			// Ingest any missed blocks
			for blockNum := lastBlock + 1; blockNum <= headNum; blockNum++ {
				if err := c.ingestBlock(ctx, blockNum); err != nil {
					c.log.Error("Failed to ingest block", "block", blockNum, "err", err)
					// Check if this is a reorg
					if errors.Is(err, suptypes.ErrConflict) {
						onReorg(c.chainID, err)
						return
					}
				}
				c.metrics.RecordChainHead(chainIDUint, blockNum)
			}
			lastBlock = headNum
		}
	}
}

func (c *ChainIngester) ingestBlock(ctx context.Context, blockNum uint64) error {
	// Get block info
	block, err := c.ethClient.InfoByNumber(ctx, blockNum)
	if err != nil {
		return fmt.Errorf("failed to get block: %w", err)
	}

	// Get receipts - returns (BlockInfo, Receipts, error)
	_, receipts, err := c.ethClient.FetchReceipts(ctx, block.Hash())
	if err != nil {
		return fmt.Errorf("failed to get receipts: %w", err)
	}

	// Get parent block for parent hash
	parentBlock := eth.BlockID{
		Hash:   block.ParentHash(),
		Number: blockNum - 1,
	}

	// Add all logs from receipts
	var logIdx uint32
	for _, receipt := range receipts {
		for _, log := range receipt.Logs {
			execMsg := c.parseExecutingMessage(log)
			logHash := logToHash(log)

			if err := c.logsDB.AddLog(logHash, parentBlock, logIdx, execMsg); err != nil {
				return fmt.Errorf("failed to add log %d: %w", logIdx, err)
			}
			logIdx++
		}
	}

	// Seal the block
	blockID := eth.BlockID{Hash: block.Hash(), Number: blockNum}
	if err := c.logsDB.SealBlock(block.ParentHash(), blockID, block.Time()); err != nil {
		return fmt.Errorf("failed to seal block: %w", err)
	}

	// Record LogsDB metrics
	chainIDUint, _ := c.chainID.Uint64()
	c.metrics.RecordLogsDBBlocksSealed(chainIDUint)
	if logIdx > 0 {
		c.metrics.RecordLogsDBLogsAdded(chainIDUint, int(logIdx))
	}

	return nil
}

// parseExecutingMessage checks if a log is an executing message and parses it
func (c *ChainIngester) parseExecutingMessage(log *gethtypes.Log) *suptypes.ExecutingMessage {
	// Check if this is a CrossL2Inbox executing message event
	// Address: 0x4200000000000000000000000000000000000022
	if log.Address != params.InteropCrossL2InboxAddress {
		return nil
	}
	if len(log.Topics) == 0 || log.Topics[0] != suptypes.ExecutingMessageEventTopic {
		return nil
	}

	// Parse the executing message using the processors package
	execMsg, err := processors.DecodeExecutingMessageLog(log)
	if err != nil {
		c.log.Warn("Failed to decode executing message", "err", err)
		return nil
	}
	return execMsg
}

// logToHash computes the log hash used in LogsDB
func logToHash(log *gethtypes.Log) common.Hash {
	return processors.LogToLogHash(log)
}

// logsDBMetrics bridges logs.Metrics interface to our metrics.Metricer
type logsDBMetrics struct {
	chainID uint64
	m       metrics.Metricer
}

func (l *logsDBMetrics) RecordDBEntryCount(kind string, count int64) {
	// We track total entries via our own metric
	l.m.RecordLogsDBEntries(l.chainID, count)
}

func (l *logsDBMetrics) RecordDBSearchEntriesRead(count int64) {
	// This tracks search efficiency, could add later if needed
}

// Rewind truncates the LogsDB to the specified block
func (c *ChainIngester) Rewind(block eth.BlockID) error {
	if c.logsDB == nil {
		return errors.New("LogsDB not initialized")
	}
	c.log.Info("Rewinding LogsDB", "block", block.Number, "hash", block.Hash)
	return c.logsDB.Rewind(&noopInvalidator{}, block)
}

// noopInvalidator implements reads.Invalidator for simple single-threaded use
type noopInvalidator struct{}

func (n *noopInvalidator) TryInvalidate(rule reads.InvalidationRule) (release func(), err error) {
	return func() {}, nil
}
