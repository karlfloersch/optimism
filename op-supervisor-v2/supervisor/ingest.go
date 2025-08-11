package supervisor

import (
	"context"
	"fmt"

	ethTypes "github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/log"

	"github.com/ethereum-optimism/optimism/op-node/rollup/derive"
	"github.com/ethereum-optimism/optimism/op-service/eth"
	"github.com/ethereum-optimism/optimism/op-service/sources"
	fromda "github.com/ethereum-optimism/optimism/op-supervisor/supervisor/backend/db/fromda"
	logsdb "github.com/ethereum-optimism/optimism/op-supervisor/supervisor/backend/db/logs"
	"github.com/ethereum-optimism/optimism/op-supervisor/supervisor/backend/processors"
	"github.com/ethereum-optimism/optimism/op-supervisor/supervisor/types"
)

// openChainDBs initializes the v1 DBs for a chain.
func (s *Supervisor) openChainDBs(logger log.Logger, chainID uint64, dataDir string) (*logsdb.DB, *fromda.DB, *fromda.DB, error) {
	// Use no-op metrics for now; can be replaced with real metrics later.
	logDB, err := logsdb.NewFromFile(logger, logsMetricsNoop{}, eth.ChainIDFromUInt64(chainID), fmt.Sprintf("%s/logs-%d", dataDir, chainID), true)
	if err != nil {
		return nil, nil, nil, err
	}
	localDB, err := fromda.NewFromFile(logger.New("db", "local"), fromda.AdaptMetrics(chainMetricsNoop{}, "local_derived"), fmt.Sprintf("%s/local-%d", dataDir, chainID))
	if err != nil {
		return nil, nil, nil, err
	}
	crossDB, err := fromda.NewFromFile(logger.New("db", "cross"), fromda.AdaptMetrics(chainMetricsNoop{}, "cross_derived"), fmt.Sprintf("%s/cross-%d", dataDir, chainID))
	if err != nil {
		return nil, nil, nil, err
	}
	return logDB, localDB, crossDB, nil
}

// ingestRange fetches payload, receipts and appends logs + local-safe link (and optionally cross-safe mirror) for [start,end] (inclusive).
func ingestRange(ctx context.Context, l1 *sources.L1Client, l2 *sources.L2Client, logs *logsdb.DB, local *fromda.DB, cross *fromda.DB, rollupCfg *sources.L2ClientConfig, start, end uint64) error {
	for n := start; n <= end; n++ {
		// debug
		// Note: keep logs lightweight to avoid noise; this helps confirm ingest actually runs.
		// fmt.Printf("[sv2] ingest block %d\n", n)
		env, err := l2.PayloadByNumber(ctx, n)
		if err != nil {
			return err
		}
		ref, err := derive.PayloadToBlockRef(rollupCfg.RollupCfg, env.ExecutionPayload)
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
		var execIdx uint32
		for i, lg := range allLogs {
			// Try to decode ExecutingMessage; may be nil for non-exec logs
			var exec *types.ExecutingMessage
			if m, err := processors.DecodeExecutingMessageLog(lg); err == nil && m != nil {
				exec = m
			}
			if err := logs.AddLog(processors.LogToLogHash(lg), parent, uint32(i), exec); err != nil {
				return err
			}
			execIdx = uint32(i)
			_ = execIdx
		}
		// Seal block in logs DB
		if err := logs.SealBlock(ref.ParentHash, eth.ToBlockID(info), ref.Time); err != nil {
			return err
		}
		// Add local-safe link
		l1Source := ref.L1Origin
		// Rehydrate full L1 block ref to include parent/time
		l1Ref, err := l1.BlockRefByNumber(ctx, l1Source.Number)
		if err != nil {
			return err
		}
		if l1Ref.Hash != l1Source.Hash {
			return fmt.Errorf("l1 reference mismatch at %d", l1Source.Number)
		}
		derivedRef := ref.BlockRef()
		if err := local.AddDerived(l1Ref, derivedRef, types.RevisionAny); err != nil {
			return err
		}
		if cross != nil {
			_ = cross.AddDerived(l1Ref, derivedRef, types.RevisionAny)
		}
	}
	return nil
}
