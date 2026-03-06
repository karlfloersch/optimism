package interop

import (
	"errors"

	"github.com/ethereum-optimism/optimism/op-service/eth"
	"github.com/ethereum/go-ethereum"
)

func (i *Interop) latestValidatedResult() (VerifiedResult, bool) {
	i.mu.RLock()
	ts, ok := i.validatedTimestamp()
	i.mu.RUnlock()
	if !ok {
		return VerifiedResult{}, false
	}
	result, err := i.verifiedDB.Get(ts)
	if err != nil {
		return VerifiedResult{}, false
	}
	return result, true
}

func (i *Interop) repairAcceptedState(lastTimestamp uint64) (bool, error) {
	var keep *VerifiedResult
	for ts := lastTimestamp; ; ts-- {
		stored, err := i.verifiedDB.Get(ts)
		if err != nil {
			if ts == 0 {
				break
			}
			continue
		}
		current, err := i.collectSnapshotAtTimestamp(ts)
		if err != nil {
			if errors.Is(err, ethereum.NotFound) {
				if ts == 0 {
					break
				}
				if ts == i.activationTimestamp {
					break
				}
				continue
			}
			return false, err
		}
		consistent, err := i.checker.AcceptedResultConsistent(i.ctx, stored, current)
		if err != nil {
			return false, err
		}
		if consistent {
			keep = &stored
			break
		}
		if ts == i.activationTimestamp {
			break
		}
		if ts == 0 {
			break
		}
	}

	if keep != nil && keep.Timestamp == lastTimestamp {
		i.mu.Lock()
		i.setValidatedBoundary(lastTimestamp, true)
		i.mu.Unlock()
		return false, nil
	}

	i.mu.Lock()
	defer i.mu.Unlock()

	if keep == nil {
		if _, err := i.verifiedDB.Rewind(0); err != nil {
			return false, err
		}
		for chainID, db := range i.logsDBs {
			if err := db.Clear(&noopInvalidator{}); err != nil {
				i.log.Error("failed to clear logsDB after accepted-state repair", "chainID", chainID, "err", err)
			}
		}
		i.currentL1 = eth.BlockID{}
		i.clearValidatedBoundary()
		return true, nil
	}

	if _, err := i.verifiedDB.RewindAfter(keep.Timestamp); err != nil {
		return false, err
	}
	for chainID, head := range keep.L2Heads {
		db, ok := i.logsDBs[chainID]
		if !ok {
			continue
		}
		i.rewindLogsDBToHead(chainID, db, head)
	}
	i.currentL1 = eth.BlockID{}
	i.setValidatedBoundary(keep.Timestamp, true)
	return true, nil
}

func (i *Interop) rewindLogsDBToHead(chainID eth.ChainID, db LogsDB, target eth.BlockID) {
	firstBlock, err := db.FirstSealedBlock()
	if err != nil {
		if clearErr := db.Clear(&noopInvalidator{}); clearErr != nil {
			i.log.Error("failed to clear logsDB", "chainID", chainID, "err", clearErr)
		}
		return
	}
	if firstBlock.Number > target.Number {
		if err := db.Clear(&noopInvalidator{}); err != nil {
			i.log.Error("failed to clear logsDB", "chainID", chainID, "err", err)
		}
		return
	}
	if err := db.Rewind(&noopInvalidator{}, target); err != nil {
		i.log.Warn("rewind conflict, clearing logsDB", "chainID", chainID, "target", target, "err", err)
		if clearErr := db.Clear(&noopInvalidator{}); clearErr != nil {
			i.log.Error("failed to clear logsDB after rewind conflict", "chainID", chainID, "err", clearErr)
		}
	}
}
