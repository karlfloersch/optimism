package supervisor

import (
	"context"
	"fmt"
	"os"

	ethTypes "github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/log"

	"github.com/ethereum-optimism/optimism/op-node/rollup/derive"
	"github.com/ethereum-optimism/optimism/op-service/eth"
	"github.com/ethereum-optimism/optimism/op-service/sources"
	logsdb "github.com/ethereum-optimism/optimism/op-supervisor/supervisor/backend/db/logs"
	"github.com/ethereum-optimism/optimism/op-supervisor/supervisor/backend/processors"
	"github.com/ethereum-optimism/optimism/op-supervisor/supervisor/types"
)

// openLogsDB initializes the logs DB for a chain.
func (s *Supervisor) openLogsDB(logger log.Logger, chainID uint64, dataDir string) (*logsdb.DB, error) {
	// Ensure base directory exists
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return nil, err
	}
	logsPath := fmt.Sprintf("%s/logs-%d", dataDir, chainID)
	// Use no-op metrics for now; can be replaced with real metrics later.
	logDB, err := logsdb.NewFromFile(logger, logsMetricsNoop{}, eth.ChainIDFromUInt64(chainID), logsPath, true)
	if err != nil {
		return nil, err
	}
	return logDB, nil
}

// ingestRange fetches payload, receipts and appends logs for [start,end] (inclusive).
func ingestRange(ctx context.Context, l2 *sources.L2Client, logs *logsdb.DB, start, end uint64) error {
	for n := start; n <= end; n++ {
		env, err := l2.PayloadByNumber(ctx, n)
		if err != nil {
			return err
		}
		ref, err := derive.PayloadToBlockRef(l2.RollupConfig(), env.ExecutionPayload)
		if err != nil {
			return err
		}
		// Fetch tx receipts to obtain logs per tx
		info, receipts, err := l2.FetchReceiptsByNumber(ctx, n)
		if err != nil {
			return err
		}
		// Collect logs flat in block order
		var allLogs []*ethTypes.Log
		for _, r := range receipts {
			for _, lg := range r.Logs {
				allLogs = append(allLogs, lg)
			}
		}
		// Write logs to DB
		// Identify parent block by number-1
		var parent eth.BlockID
		if n > 0 {
			parent = eth.BlockID{Hash: ref.ParentHash, Number: n - 1}
		}
		for i, lg := range allLogs {
			// Try to decode ExecutingMessage; may be nil for non-exec logs
			var exec *types.ExecutingMessage
			if m, err := processors.DecodeExecutingMessageLog(lg); err == nil && m != nil {
				exec = m
			}
			if err := logs.AddLog(processors.LogToLogHash(lg), parent, uint32(i), exec); err != nil {
				return err
			}

		}
		// Seal block in logs DB
		if err := logs.SealBlock(ref.ParentHash, eth.ToBlockID(info), ref.Time); err != nil {
			return err
		}
	}
	return nil
}
