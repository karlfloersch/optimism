package filter

import (
	"context"
	"errors"
	"fmt"
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

// Chain manages block ingestion and LogsDB for a single chain
type Chain struct {
	log      log.Logger
	metrics  metrics.Metricer
	chainID  eth.ChainID
	rpcURL   string
	cfg      *Config

	rpcClient client.RPC
	ethClient *sources.EthClient
	logsDB    *logs.DB

	ready   atomic.Bool
	stopped atomic.Bool

	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// NewChain creates a new Chain instance
func NewChain(ctx context.Context, logger log.Logger, m metrics.Metricer,
	chainID eth.ChainID, rpcURL string, cfg *Config) (*Chain, error) {

	c := &Chain{
		log:     logger.New("chainID", chainID),
		metrics: m,
		chainID: chainID,
		rpcURL:  rpcURL,
		cfg:     cfg,
	}
	return c, nil
}

// Start starts the chain ingester
func (c *Chain) Start(ctx context.Context, onReorg ReorgCallback) error {
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
			TrustRPC:          true,
			MustBePostMerge:   false,
			RPCProviderKind:   sources.RPCKindBasic,
			ReceiptsCacheSize: 100,
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
func (c *Chain) Stop(ctx context.Context) error {
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
func (c *Chain) Ready() bool {
	return c.ready.Load()
}

// Contains validates that a log exists in the LogsDB
func (c *Chain) Contains(access suptypes.Access) error {
	query := suptypes.ContainsQuery{
		Timestamp: access.Timestamp,
		BlockNum:  access.BlockNumber,
		LogIdx:    access.LogIndex,
		Checksum:  access.Checksum,
	}
	_, err := c.logsDB.Contains(query)
	return err
}

func (c *Chain) initLogsDB() error {
	var dbPath string
	if c.cfg.DataDir != "" {
		chainIDUint, _ := c.chainID.Uint64()
		dbPath = filepath.Join(c.cfg.DataDir, fmt.Sprintf("chain-%d", chainIDUint), "logs.db")
	}

	// Create LogsDB - use file if path specified, otherwise in-memory
	db, err := logs.NewFromFile(c.log, &noopLogsDBMetrics{}, c.chainID, dbPath, true)
	if err != nil {
		return err
	}
	c.logsDB = db
	return nil
}

func (c *Chain) runIngestion(ctx context.Context, onReorg ReorgCallback) {
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

	// Backfill historical blocks
	if err := c.backfill(ctx, startBlock, head.NumberU64()); err != nil {
		c.log.Error("Backfill failed", "err", err)
		onReorg(c.chainID, fmt.Errorf("backfill failed: %w", err))
		return
	}

	chainIDUint, _ := c.chainID.Uint64()
	c.log.Info("Backfill complete, starting live ingestion")
	c.ready.Store(true)
	c.metrics.RecordChainReady(chainIDUint, true)

	// Subscribe to new blocks
	c.subscribeNewBlocks(ctx, onReorg)
}

func (c *Chain) calculateStartBlock(head eth.BlockInfo) uint64 {
	// Estimate blocks in backfill period based on configured duration
	backfillBlocks := uint64(c.cfg.BackfillDuration / defaultBlockTime)

	headNum := head.NumberU64()
	if headNum <= backfillBlocks {
		return 1 // Start from block 1 (not genesis)
	}
	return headNum - backfillBlocks
}

func (c *Chain) initializeLogsDBFromBlock(ctx context.Context, blockNum uint64) error {
	// Get the block before our start block to use as the "sealed" starting point
	if blockNum == 0 {
		blockNum = 1
	}
	startBlockNum := blockNum - 1

	block, err := c.ethClient.InfoByNumber(ctx, startBlockNum)
	if err != nil {
		return fmt.Errorf("failed to get start block %d: %w", startBlockNum, err)
	}

	// Force the LogsDB to start from this block
	// Note: This only works on empty DB
	return c.logsDB.AddLog(
		common.Hash{}, // Will be overwritten
		eth.BlockID{Hash: block.ParentHash(), Number: startBlockNum - 1},
		0,
		nil,
	)
}

func (c *Chain) backfill(ctx context.Context, startBlock, endBlock uint64) error {
	total := endBlock - startBlock + 1
	c.log.Info("Starting backfill", "blocks", total)

	chainIDUint, _ := c.chainID.Uint64()

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

		if progress%1000 == 0 {
			c.log.Info("Backfill progress", "block", blockNum, "progress", fmt.Sprintf("%.1f%%", float64(progress)/float64(total)*100))
		}
	}

	return nil
}

func (c *Chain) subscribeNewBlocks(ctx context.Context, onReorg ReorgCallback) {
	// Simple polling loop for new blocks
	ticker := time.NewTicker(httpPollInterval)
	defer ticker.Stop()

	var lastBlock uint64
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

func (c *Chain) ingestBlock(ctx context.Context, blockNum uint64) error {
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

	return nil
}

// parseExecutingMessage checks if a log is an executing message and parses it
func (c *Chain) parseExecutingMessage(log *gethtypes.Log) *suptypes.ExecutingMessage {
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

// noopLogsDBMetrics implements logs.Metrics interface
type noopLogsDBMetrics struct{}

func (m *noopLogsDBMetrics) RecordDBEntryCount(kind string, count int64) {}
func (m *noopLogsDBMetrics) RecordDBSearchEntriesRead(count int64)       {}
