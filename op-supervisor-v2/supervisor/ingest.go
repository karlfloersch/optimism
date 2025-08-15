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
	fromda "github.com/ethereum-optimism/optimism/op-supervisor/supervisor/backend/db/fromda"
	logsdb "github.com/ethereum-optimism/optimism/op-supervisor/supervisor/backend/db/logs"
	"github.com/ethereum-optimism/optimism/op-supervisor/supervisor/backend/processors"
	"github.com/ethereum-optimism/optimism/op-supervisor/supervisor/types"
)

// openChainDBs initializes the v1 DBs for a chain.
func (s *Supervisor) openChainDBs(logger log.Logger, chainID uint64, dataDir string) (*logsdb.DB, *fromda.DB, *fromda.DB, error) {
	// Ensure base directory exists
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return nil, nil, nil, err
	}
	logsPath := fmt.Sprintf("%s/logs-%d", dataDir, chainID)
	localPath := fmt.Sprintf("%s/local-%d", dataDir, chainID)
	crossPath := fmt.Sprintf("%s/cross-%d", dataDir, chainID)
	// Use no-op metrics for now; can be replaced with real metrics later.
	logDB, err := logsdb.NewFromFile(logger, logsMetricsNoop{}, eth.ChainIDFromUInt64(chainID), logsPath, true)
	if err != nil {
		return nil, nil, nil, err
	}
	localDB, err := fromda.NewFromFile(logger.New("db", "local"), fromda.AdaptMetrics(chainMetricsNoop{}, "local_derived"), localPath)
	if err != nil {
		return nil, nil, nil, err
	}
	crossDB, err := fromda.NewFromFile(logger.New("db", "cross"), fromda.AdaptMetrics(chainMetricsNoop{}, "cross_derived"), crossPath)
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
		if err := addDerivedWithDiagonalSplit(ctx, l2, local, rollupCfg, l1Ref, derivedRef); err != nil {
			return err
		}
		if cross != nil {
			if err := addDerivedWithDiagonalSplit(ctx, l2, cross, rollupCfg, l1Ref, derivedRef); err != nil {
				return err
			}
		}
	}
	return nil
}

// ingestLocalOnlyRange appends only local-safe links for [start,end] (inclusive), without writing logs.
func ingestLocalOnlyRange(ctx context.Context, l1 *sources.L1Client, l2 *sources.L2Client, local *fromda.DB, rollupCfg *sources.L2ClientConfig, start, end uint64) error {
	for n := start; n <= end; n++ {
		env, err := l2.PayloadByNumber(ctx, n)
		if err != nil {
			return err
		}
		ref, err := derive.PayloadToBlockRef(rollupCfg.RollupCfg, env.ExecutionPayload)
		if err != nil {
			return err
		}
		l1Source := ref.L1Origin
		l1Ref, err := l1.BlockRefByNumber(ctx, l1Source.Number)
		if err != nil {
			return err
		}
		if l1Ref.Hash != l1Source.Hash {
			return fmt.Errorf("l1 reference mismatch at %d", l1Source.Number)
		}
		if err := addDerivedWithDiagonalSplit(ctx, l2, local, rollupCfg, l1Ref, ref.BlockRef()); err != nil {
			return err
		}
	}
	return nil
}

// addDerivedWithDiagonalSplit appends a (source,derived) link to the given DB,
// splitting a diagonal increment (source+1, derived+1) into two ordered writes:
// (source+1, derived) followed by (source+1, derived+1).
func addDerivedWithDiagonalSplit(ctx context.Context, l2 *sources.L2Client, db *fromda.DB, rollupCfg *sources.L2ClientConfig, l1Ref eth.BlockRef, derivedRef eth.BlockRef) error {
	if last, err := db.Last(); err == nil {
		if last.Source.Number+1 == l1Ref.Number && last.Derived.Number+1 == derivedRef.Number {
			prevEnv, perr := l2.PayloadByNumber(ctx, last.Derived.Number)
			if perr != nil {
				return perr
			}
			prevRef, derr := derive.PayloadToBlockRef(rollupCfg.RollupCfg, prevEnv.ExecutionPayload)
			if derr != nil {
				return derr
			}
			if err := db.AddDerived(l1Ref, prevRef.BlockRef(), types.RevisionAny); err != nil {
				return err
			}
		}
	}
	return db.AddDerived(l1Ref, derivedRef, types.RevisionAny)
}
