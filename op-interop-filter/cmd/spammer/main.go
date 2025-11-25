package main

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"time"

	"github.com/ethereum/go-ethereum/common"
	gethtypes "github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/rpc"
	"github.com/urfave/cli/v2"

	opservice "github.com/ethereum-optimism/optimism/op-service"
	"github.com/ethereum-optimism/optimism/op-service/client"
	"github.com/ethereum-optimism/optimism/op-service/cliapp"
	"github.com/ethereum-optimism/optimism/op-service/ctxinterrupt"
	"github.com/ethereum-optimism/optimism/op-service/eth"
	oplog "github.com/ethereum-optimism/optimism/op-service/log"
	"github.com/ethereum-optimism/optimism/op-service/sources"
	suptypes "github.com/ethereum-optimism/optimism/op-supervisor/supervisor/types"
)

var (
	Version   = "v0.0.1"
	GitCommit = ""
	GitDate   = ""
)

var (
	L2RPCFlag = &cli.StringFlag{
		Name:     "l2-rpc",
		Usage:    "L2 RPC endpoint URL",
		Required: true,
		EnvVars:  []string{"L2_RPC"},
	}
	FilterRPCFlag = &cli.StringFlag{
		Name:     "filter-rpc",
		Usage:    "Filter service RPC endpoint URL",
		Required: true,
		EnvVars:  []string{"FILTER_RPC"},
	}
	ChainIDFlag = &cli.Uint64Flag{
		Name:     "chain-id",
		Usage:    "Chain ID to test",
		Required: true,
		EnvVars:  []string{"CHAIN_ID"},
	}
	NumQueriesFlag = &cli.IntFlag{
		Name:    "num-queries",
		Usage:   "Number of queries to run (0 = run forever)",
		Value:   100,
		EnvVars: []string{"NUM_QUERIES"},
	}
	BlockRangeFlag = &cli.IntFlag{
		Name:    "block-range",
		Usage:   "Number of recent blocks to sample from",
		Value:   100,
		EnvVars: []string{"BLOCK_RANGE"},
	}
	QueryIntervalFlag = &cli.StringFlag{
		Name:    "query-interval",
		Usage:   "Interval between queries (e.g., 100ms, 1s)",
		Value:   "500ms",
		EnvVars: []string{"QUERY_INTERVAL"},
	}
)

func main() {
	oplog.SetupDefaults()

	app := cli.NewApp()
	app.Name = "filter-spammer"
	app.Usage = "Spam the interop filter service with queries to validate its behavior"
	app.Version = opservice.FormatVersion(Version, GitCommit, GitDate, "")
	app.Flags = cliapp.ProtectFlags(append([]cli.Flag{
		L2RPCFlag,
		FilterRPCFlag,
		ChainIDFlag,
		NumQueriesFlag,
		BlockRangeFlag,
		QueryIntervalFlag,
	}, oplog.CLIFlags("SPAMMER")...))
	app.Action = run

	ctx := ctxinterrupt.WithSignalWaiterMain(context.Background())
	err := app.RunContext(ctx, os.Args)
	if err != nil {
		log.Crit("Application failed", "err", err)
	}
}

func run(cliCtx *cli.Context) error {
	logger := oplog.NewLogger(os.Stdout, oplog.ReadCLIConfig(cliCtx))

	l2RPC := cliCtx.String(L2RPCFlag.Name)
	filterRPC := cliCtx.String(FilterRPCFlag.Name)
	chainID := cliCtx.Uint64(ChainIDFlag.Name)
	numQueries := cliCtx.Int(NumQueriesFlag.Name)
	blockRange := cliCtx.Int(BlockRangeFlag.Name)
	queryIntervalStr := cliCtx.String(QueryIntervalFlag.Name)

	queryInterval, err := time.ParseDuration(queryIntervalStr)
	if err != nil {
		return fmt.Errorf("invalid query-interval: %w", err)
	}

	ctx := cliCtx.Context

	logger.Info("Starting filter spammer",
		"l2RPC", l2RPC,
		"filterRPC", filterRPC,
		"chainID", chainID,
		"numQueries", numQueries,
		"blockRange", blockRange,
		"queryInterval", queryInterval,
	)

	// Connect to L2 RPC
	l2Client, err := client.NewRPC(ctx, logger, l2RPC)
	if err != nil {
		return fmt.Errorf("failed to connect to L2 RPC: %w", err)
	}
	defer l2Client.Close()

	ethClient, err := sources.NewEthClient(l2Client, logger, nil, &sources.EthClientConfig{
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
	})
	if err != nil {
		return fmt.Errorf("failed to create eth client: %w", err)
	}
	defer ethClient.Close()

	// Connect to filter RPC
	filterClient, err := rpc.DialContext(ctx, filterRPC)
	if err != nil {
		return fmt.Errorf("failed to connect to filter RPC: %w", err)
	}
	defer filterClient.Close()

	// Check filter is ready
	var failsafe bool
	if err := filterClient.CallContext(ctx, &failsafe, "admin_getFailsafeEnabled"); err != nil {
		return fmt.Errorf("failed to check filter failsafe: %w", err)
	}
	if failsafe {
		return errors.New("filter service is in failsafe mode")
	}
	logger.Info("Filter service is ready", "failsafe", failsafe)

	// Get current head
	head, err := ethClient.InfoByLabel(ctx, eth.Unsafe)
	if err != nil {
		return fmt.Errorf("failed to get head: %w", err)
	}
	headNum := head.NumberU64()
	logger.Info("Current L2 head", "block", headNum)

	// Calculate block range to sample from
	startBlock := headNum - uint64(blockRange)
	if startBlock < 1 {
		startBlock = 1
	}

	spammer := &Spammer{
		logger:       logger,
		ethClient:    ethClient,
		filterClient: filterClient,
		chainID:      eth.ChainIDFromUInt64(chainID),
		startBlock:   startBlock,
		endBlock:     headNum,
	}

	// Run queries
	ticker := time.NewTicker(queryInterval)
	defer ticker.Stop()

	validQueries := 0
	invalidQueries := 0
	errorCount := 0

	for i := 0; numQueries == 0 || i < numQueries; i++ {
		select {
		case <-ctx.Done():
			logger.Info("Shutting down", "validQueries", validQueries, "invalidQueries", invalidQueries, "errors", errorCount)
			return nil
		case <-ticker.C:
			// Alternate between valid and invalid queries
			if i%2 == 0 {
				if err := spammer.RunValidQuery(ctx); err != nil {
					logger.Error("Valid query failed unexpectedly", "err", err, "query", i)
					errorCount++
					if errorCount > 10 {
						return fmt.Errorf("too many errors (%d): last error: %w", errorCount, err)
					}
				} else {
					validQueries++
					logger.Debug("Valid query passed", "query", i)
				}
			} else {
				if err := spammer.RunInvalidQuery(ctx); err != nil {
					logger.Error("Invalid query test failed", "err", err, "query", i)
					errorCount++
					if errorCount > 10 {
						return fmt.Errorf("too many errors (%d): last error: %w", errorCount, err)
					}
				} else {
					invalidQueries++
					logger.Debug("Invalid query rejected as expected", "query", i)
				}
			}

			if (i+1)%20 == 0 {
				logger.Info("Progress", "queries", i+1, "valid", validQueries, "invalid", invalidQueries, "errors", errorCount)
			}
		}
	}

	logger.Info("Spammer completed successfully",
		"validQueries", validQueries,
		"invalidQueries", invalidQueries,
		"errors", errorCount,
	)

	if errorCount > 0 {
		return fmt.Errorf("completed with %d errors", errorCount)
	}
	return nil
}

// Spammer handles the spam testing logic
type Spammer struct {
	logger       log.Logger
	ethClient    *sources.EthClient
	filterClient *rpc.Client
	chainID      eth.ChainID
	startBlock   uint64
	endBlock     uint64
}

// RunValidQuery fetches a random log and verifies the filter accepts it
func (s *Spammer) RunValidQuery(ctx context.Context) error {
	// Pick a random block
	blockNum := s.startBlock + uint64(rand.Int63n(int64(s.endBlock-s.startBlock+1)))

	// Get block info
	block, err := s.ethClient.InfoByNumber(ctx, blockNum)
	if err != nil {
		return fmt.Errorf("failed to get block %d: %w", blockNum, err)
	}

	// Get receipts
	_, receipts, err := s.ethClient.FetchReceipts(ctx, block.Hash())
	if err != nil {
		return fmt.Errorf("failed to get receipts for block %d: %w", blockNum, err)
	}

	// Find a block with logs
	var foundLog bool
	for _, receipt := range receipts {
		if len(receipt.Logs) == 0 {
			continue
		}

		// Pick a random log from this receipt
		logIdx := rand.Intn(len(receipt.Logs))
		log := receipt.Logs[logIdx]

		// Compute the log hash
		logHash := LogToLogHash(log)

		// Create access entry
		access := suptypes.ChecksumArgs{
			BlockNumber: blockNum,
			LogIndex:    uint32(log.Index),
			Timestamp:   block.Time(),
			ChainID:     s.chainID,
			LogHash:     logHash,
		}.Access()

		// Encode and query
		entries := suptypes.EncodeAccessList([]suptypes.Access{access})

		execDesc := suptypes.ExecutingDescriptor{
			ChainID:   s.chainID,
			Timestamp: block.Time(),
		}

		err := s.filterClient.CallContext(ctx, nil, "supervisor_checkAccessList", entries, suptypes.LocalUnsafe, execDesc)
		if err != nil {
			return fmt.Errorf("valid query rejected: block=%d logIdx=%d err=%w", blockNum, log.Index, err)
		}
		foundLog = true
		break
	}

	if !foundLog {
		// Block had no logs, try again with a different block
		return s.RunValidQuery(ctx)
	}

	return nil
}

// RunInvalidQuery creates an invalid query and verifies the filter rejects it
func (s *Spammer) RunInvalidQuery(ctx context.Context) error {
	// Pick a random block
	blockNum := s.startBlock + uint64(rand.Int63n(int64(s.endBlock-s.startBlock+1)))

	// Get block info
	block, err := s.ethClient.InfoByNumber(ctx, blockNum)
	if err != nil {
		return fmt.Errorf("failed to get block %d: %w", blockNum, err)
	}

	// Get receipts
	_, receipts, err := s.ethClient.FetchReceipts(ctx, block.Hash())
	if err != nil {
		return fmt.Errorf("failed to get receipts for block %d: %w", blockNum, err)
	}

	// Find a block with logs
	var foundLog bool
	for _, receipt := range receipts {
		if len(receipt.Logs) == 0 {
			continue
		}

		// Pick a random log from this receipt
		logIdx := rand.Intn(len(receipt.Logs))
		log := receipt.Logs[logIdx]

		// Compute the log hash
		logHash := LogToLogHash(log)

		// Create access entry with WRONG checksum (flip a byte)
		access := suptypes.ChecksumArgs{
			BlockNumber: blockNum,
			LogIndex:    uint32(log.Index),
			Timestamp:   block.Time(),
			ChainID:     s.chainID,
			LogHash:     logHash,
		}.Access()

		// Corrupt the checksum (flip byte at index 10, preserving prefix byte at 0)
		access.Checksum[10] ^= 0xFF

		// Encode and query
		entries := suptypes.EncodeAccessList([]suptypes.Access{access})

		execDesc := suptypes.ExecutingDescriptor{
			ChainID:   s.chainID,
			Timestamp: block.Time(),
		}

		err := s.filterClient.CallContext(ctx, nil, "supervisor_checkAccessList", entries, suptypes.LocalUnsafe, execDesc)
		if err == nil {
			return fmt.Errorf("invalid query was accepted: block=%d logIdx=%d", blockNum, log.Index)
		}
		// Error expected - query was correctly rejected
		foundLog = true
		break
	}

	if !foundLog {
		// Block had no logs, try again with a different block
		return s.RunInvalidQuery(ctx)
	}

	return nil
}

// LogToLogHash computes the log hash used in LogsDB
// This matches processors.LogToLogHash
func LogToLogHash(l *gethtypes.Log) common.Hash {
	// Compute payload hash from topics and data
	msg := make([]byte, 0)
	for _, topic := range l.Topics {
		msg = append(msg, topic.Bytes()...)
	}
	msg = append(msg, l.Data...)
	payloadHash := crypto.Keccak256Hash(msg)

	// Compute log hash
	return suptypes.PayloadHashToLogHash(payloadHash, l.Address)
}
