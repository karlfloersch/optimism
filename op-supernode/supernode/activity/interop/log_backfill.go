package interop

import (
	"context"
	"fmt"
	"sync"

	"github.com/ethereum-optimism/optimism/op-service/eth"
	cc "github.com/ethereum-optimism/optimism/op-supernode/supernode/chain_container"
)

// resolveFirstVerifiableTimestamp returns the next timestamp that normal
// interop verification should process.
//
// Startup has three cases:
//   - with an initialized verifiedDB, resume at LastTimestamp+1;
//   - with an empty verifiedDB and SafeDB coverage before activation, begin
//     at activation;
//   - with an empty verifiedDB and SafeDB coverage after activation, latch a
//     cold-start handoff from the newest first SafeDB-covered timestamp across
//     all chains.
//
// Starting after the first SafeDB entry leaves the entry itself available as
// the frontier block when persisting the first verified timestamp.
func (i *Interop) resolveFirstVerifiableTimestamp(ctx context.Context) (uint64, error) {
	if lastTS, initialized := i.verifiedDBLastTimestamp(); initialized {
		return lastTS + 1, nil
	}
	if i.firstVerifiableSet {
		return i.firstVerifiable, nil
	}
	if len(i.chains) == 0 {
		return i.activationTimestamp, nil
	}

	safeDBHandoffTime, err := i.safeDBHandoffTimestamp(ctx)
	if err != nil {
		return 0, err
	}
	first := i.activationTimestamp
	if safeDBHandoffTime >= i.activationTimestamp {
		first = safeDBHandoffTime + 1
	}
	i.firstVerifiable = first
	i.firstVerifiableSet = true
	return first, nil
}

type firstSafeHeadReader interface {
	FirstSafeHead(ctx context.Context) (eth.BlockID, eth.BlockID, error)
}

// safeDBHandoffTimestamp returns the newest first SafeDB-covered timestamp
// across all chains. Starting verification after this timestamp guarantees that
// every chain has at least one SafeDB-backed frontier block.
func (i *Interop) safeDBHandoffTimestamp(ctx context.Context) (uint64, error) {
	handoffTime := uint64(0)
	for _, chain := range i.chains {
		reader, ok := chain.(firstSafeHeadReader)
		if !ok {
			return 0, fmt.Errorf("chain %s: first safe head reader unavailable", chain.ID())
		}
		l1, l2, err := reader.FirstSafeHead(ctx)
		if err != nil {
			return 0, fmt.Errorf("chain %s: first safe head: %w", chain.ID(), err)
		}
		if l2 == (eth.BlockID{}) {
			return 0, fmt.Errorf("chain %s: first safe head not yet available", chain.ID())
		}
		ts, err := chain.BlockNumberToTimestamp(ctx, l2.Number)
		if err != nil {
			return 0, fmt.Errorf("chain %s: first safe head timestamp: %w", chain.ID(), err)
		}
		i.log.Debug("first verifiable timestamp: SafeDB handoff",
			"chain", chain.ID(), "l1", l1, "l2", l2, "timestamp", ts)
		handoffTime = max(handoffTime, ts)
	}
	return handoffTime, nil
}

// shouldRunStartupLogBackfill keeps log backfill on the cold-start path only.
// If verifiedDB already has results, normal verification resumes from
// LastTimestamp+1 and the log backfill phase is skipped.
func (i *Interop) shouldRunStartupLogBackfill() bool {
	if i.logBackfillDepth <= 0 || len(i.chains) == 0 {
		return false
	}
	_, initialized := i.verifiedDBLastTimestamp()
	return !initialized
}

func (i *Interop) verifiedDBLastTimestamp() (uint64, bool) {
	if i.verifiedDB == nil {
		return 0, false
	}
	return i.verifiedDB.LastTimestamp()
}

func (i *Interop) runLogBackfill() (uint64, error) {
	if !i.shouldRunStartupLogBackfill() {
		return 0, nil
	}

	firstVerifiable, err := i.readyFirstVerifiableTimestamp(i.ctx)
	if err != nil {
		return 0, err
	}
	startTime, endTime, ok := i.logBackfillTimeRange(firstVerifiable)
	if !ok {
		return 0, nil
	}

	errCh := make(chan error, len(i.chains))
	wg := sync.WaitGroup{}
	wg.Add(len(i.chains))
	for _, chain := range i.chains {
		go func(chain cc.ChainContainer) {
			defer wg.Done()
			chainStartTime := startTime
			// if we can identify the genesis time, use it to clamp the start time
			// if we can't, we'd either fail now or later when trying to use the value
			if genesisTime, err := chain.BlockNumberToTimestamp(i.ctx, 0); err == nil &&
				genesisTime > startTime {
				chainStartTime = genesisTime
			}
			startNum, err := chain.TimestampToBlockNumber(i.ctx, chainStartTime)
			if err != nil {
				errCh <- fmt.Errorf("chain %s: timestamp to block number for start %d: %w", chain.ID(), chainStartTime, err)
				i.log.Error("log backfill: timestamp to block number for start", "chain", chain.ID(), "err", err)
				return
			}
			endNum, err := chain.TimestampToBlockNumber(i.ctx, endTime)
			if err != nil {
				errCh <- fmt.Errorf("chain %s: timestamp to block number for end %d: %w", chain.ID(), endTime, err)
				i.log.Error("log backfill: timestamp to block number for end", "chain", chain.ID(), "err", err)
				return
			}
			i.log.Info("log backfill: sealing logs",
				"chain", chain.ID(), "from", startNum, "to", endNum)
			if err := i.backfillChain(i.ctx, chain.ID(), chain, startNum, endNum); err != nil {
				errCh <- fmt.Errorf("chain %s: backfill: %w", chain.ID(), err)
				return
			}
		}(chain)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		return 0, err
	}
	return endTime, nil
}

// logBackfillTimeRange returns the inclusive timestamp range to seal before
// normal verification starts.
func (i *Interop) logBackfillTimeRange(firstVerifiable uint64) (startTime, endTime uint64, ok bool) {
	if firstVerifiable <= i.activationTimestamp {
		return 0, 0, false
	}
	endTime = firstVerifiable - 1
	depthSec := uint64(i.logBackfillDepth.Seconds())
	if endTime >= depthSec {
		startTime = endTime - depthSec
	}
	startTime = max(startTime, i.activationTimestamp)
	return startTime, endTime, true
}

func (i *Interop) backfillChain(ctx context.Context, cid eth.ChainID, chain cc.ChainContainer, startNum, endNum uint64) error {
	db := i.logsDBs[cid]
	// This is a startup best-effort repair for pre-existing logsDB reorg drift,
	// separate from the normal interop observation/apply loop. It does not close
	// the window where an L2 reorg lands after reconciliation/backfill and before
	// normal interop persists its first frontier block. In that case the write path
	// fails with ErrParentHashMismatch or ErrStaleLogsDB instead of appending
	// inconsistent logs.
	if err := i.reconcileLogsDBTail(ctx, cid, chain, db); err != nil {
		return err
	}
	if latest, has := db.LatestSealedBlock(); has {
		startNum = latest.Number + 1
	}
	if startNum > endNum {
		return nil
	}
	totalBlocks := endNum - startNum + 1
	for num := startNum; num <= endNum; num++ {
		out, err := chain.OutputV0AtBlockNumber(ctx, num)
		if err != nil {
			return fmt.Errorf("chain %s: output at block %d: %w", cid, num, err)
		}
		bid := eth.BlockID{Hash: out.BlockHash, Number: num}
		blockInfo, receipts, err := chain.FetchReceipts(ctx, bid)
		if err != nil {
			return fmt.Errorf("chain %s: fetch receipts %d: %w", cid, num, err)
		}

		if err := i.sealBlockDataIntoLogsDB(cid, bid, blockInfo, receipts, blockInfo.Time(), true); err != nil {
			return err
		}

		if totalBlocks > 0 {
			progress := float64(num-startNum+1) / float64(totalBlocks)
			i.metrics.LogBackfillProgress.WithLabelValues(cid.String()).Set(progress)
		}
	}
	return nil
}

// reconcileLogsDBTail trims tail blocks whose hash no longer matches canonical,
// so backfill resumes from a block that is still in force. Without this, an L2
// reorg that occurs while supernode is offline leaves the tail diverged and the
// first seal on resume loops forever on ErrParentHashMismatch.
func (i *Interop) reconcileLogsDBTail(ctx context.Context, cid eth.ChainID, chain cc.ChainContainer, db LogsDB) error {
	latest, has := db.LatestSealedBlock()
	if !has {
		return nil
	}
	latestOut, err := chain.OutputV0AtBlockNumber(ctx, latest.Number)
	if err != nil {
		return fmt.Errorf("chain %s: output at block %d during logsDB reconcile: %w", cid, latest.Number, err)
	}
	if latestOut.BlockHash == latest.Hash {
		return nil
	}

	first, err := db.FirstSealedBlock()
	if err != nil {
		return fmt.Errorf("chain %s: first sealed block during reconcile: %w", cid, err)
	}

	// Walk back from latest.Number-1 looking for the deepest sealed block whose
	// hash still matches canonical. latest itself is already known to diverge.
	for n := latest.Number; n > first.Number; {
		n--
		seal, err := db.FindSealedBlock(n)
		if err != nil {
			return fmt.Errorf("chain %s: find sealed block %d during reconcile: %w", cid, n, err)
		}
		out, err := chain.OutputV0AtBlockNumber(ctx, n)
		if err != nil {
			return fmt.Errorf("chain %s: output at block %d during reconcile: %w", cid, n, err)
		}
		if seal.Hash != out.BlockHash {
			continue
		}
		i.log.Warn("rewinding logsDB to last canonical block",
			"chain", cid, "rewindTo", n, "trimmedTipNumber", latest.Number,
			"trimmedTipStored", latest.Hash, "trimmedTipCanonical", latestOut.BlockHash)
		if err := db.Rewind(eth.BlockID{Number: n, Hash: seal.Hash}); err != nil {
			return fmt.Errorf("chain %s: rewind logsDB during reconcile: %w", cid, err)
		}
		return nil
	}

	i.log.Warn("entire logsDB diverges from canonical; clearing",
		"chain", cid, "firstSealed", first.Number, "latestSealed", latest.Number)
	if err := db.Clear(); err != nil {
		return fmt.Errorf("chain %s: clear logsDB during reconcile: %w", cid, err)
	}
	return nil
}
