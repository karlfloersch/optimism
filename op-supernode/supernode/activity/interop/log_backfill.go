package interop

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/ethereum-optimism/optimism/op-service/eth"
	cc "github.com/ethereum-optimism/optimism/op-supernode/supernode/chain_container"
)

// LogBackfillLowerBound returns T_lo = max(T_act, crossSafeTs - D_log) in unix seconds (L2).
// crossSafeTs is the minimum cross-safe timestamp across all chains — the
// earliest point where cross-validation will resume after startup.
// Never ingest logs for timestamps before activation.
func LogBackfillLowerBound(crossSafeTs, activationTimestampUnix uint64, logBackfillDepth time.Duration) uint64 {
	if logBackfillDepth <= 0 {
		return crossSafeTs
	}
	sub := uint64(logBackfillDepth / time.Second)
	var raw uint64
	if crossSafeTs >= sub {
		raw = crossSafeTs - sub
	} else {
		raw = 0
	}
	if raw < activationTimestampUnix {
		return activationTimestampUnix
	}
	return raw
}

func sortedChainIDs(chains map[eth.ChainID]cc.ChainContainer) []eth.ChainID {
	out := make([]eth.ChainID, 0, len(chains))
	for id := range chains {
		out = append(out, id)
	}
	sort.Slice(out, func(a, b int) bool { return out[a].Cmp(out[b]) < 0 })
	return out
}

// runLogBackfill seals logs for each chain from T_lo through LocalSafe and
// advances activationTimestamp past the backfilled range so the main loop
// starts verification after the pre-ingested data.
//
// T_lo is computed from the minimum cross-safe (SafeL2) timestamp across all
// chains, since that is the earliest point where cross-validation will resume.
func (i *Interop) runLogBackfill() error {
	if i.logBackfillDepth <= 0 {
		return nil
	}

	ctx := i.ctx
	sortedIDs := sortedChainIDs(i.chains)

	// First pass: gather the minimum cross-safe timestamp across all chains.
	// SafeL2 is the cross-safe head post-interop.
	type chainInfo struct {
		crossSafeTime uint64
		localSafeNum  uint64
		localSafeTime uint64
	}
	info := make(map[eth.ChainID]chainInfo, len(sortedIDs))
	var minCrossSafeTime uint64
	first := true
	for _, cid := range sortedIDs {
		ss, err := i.chains[cid].SyncStatus(ctx)
		if err != nil {
			return fmt.Errorf("chain %s: sync status: %w", cid, err)
		}
		ci := chainInfo{
			crossSafeTime: ss.SafeL2.Time,
			localSafeNum:  ss.LocalSafeL2.Number,
			localSafeTime: ss.LocalSafeL2.Time,
		}
		info[cid] = ci
		if first || ci.crossSafeTime < minCrossSafeTime {
			minCrossSafeTime = ci.crossSafeTime
			first = false
		}
	}
	if first {
		return nil
	}

	Tlo := LogBackfillLowerBound(minCrossSafeTime, i.activationTimestamp, i.logBackfillDepth)
	i.log.Info("log backfill: computed lower bound",
		"minCrossSafeTime", minCrossSafeTime, "T_lo", Tlo, "depth", i.logBackfillDepth)

	// Second pass: backfill each chain from T_lo to its LocalSafe.
	var minLocalSafeTime uint64
	firstLocal := true
	for _, cid := range sortedIDs {
		ci := info[cid]
		chain := i.chains[cid]

		if firstLocal || ci.localSafeTime < minLocalSafeTime {
			minLocalSafeTime = ci.localSafeTime
			firstLocal = false
		}

		startNum, err := chain.TimestampToBlockNumber(ctx, Tlo)
		if err != nil {
			startNum = 0
		}
		if startNum > ci.localSafeNum {
			i.log.Info("log backfill: chain already past lower bound, skipping",
				"chain", cid, "startNum", startNum, "localSafeNum", ci.localSafeNum)
			continue
		}

		i.log.Info("log backfill: sealing logs",
			"chain", cid, "from", startNum, "to", ci.localSafeNum)

		if err := i.backfillChain(ctx, cid, chain, startNum, ci.localSafeNum); err != nil {
			return err
		}
	}

	if !firstLocal && minLocalSafeTime+1 > i.activationTimestamp {
		i.log.Info("advancing activation past backfilled range",
			"oldActivation", i.activationTimestamp, "newActivation", minLocalSafeTime+1)
		i.activationTimestamp = minLocalSafeTime + 1
	}
	i.log.Info("interop log backfill complete", "activationTimestamp", i.activationTimestamp)
	return nil
}

func (i *Interop) backfillChain(ctx context.Context, cid eth.ChainID, chain cc.ChainContainer, startNum, endNum uint64) error {
	db := i.logsDBs[cid]
	if latest, has := db.LatestSealedBlock(); has {
		startNum = latest.Number + 1
	}
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
	}
	return nil
}
