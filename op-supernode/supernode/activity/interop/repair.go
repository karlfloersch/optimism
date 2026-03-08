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
	keep, err := i.findKeptAcceptedResult(lastTimestamp)
	if err != nil {
		return false, err
	}

	if keep != nil && keep.Timestamp == lastTimestamp {
		i.mu.Lock()
		i.setValidatedBoundary(lastTimestamp, true)
		i.mu.Unlock()
		return false, nil
	}

	repairTS := i.repairResetTimestamp(keep)

	affectedChains := make([]eth.ChainID, 0, len(i.chains))
	for chainID, chain := range i.chains {
		if keep == nil {
			if err := chain.ClearDenyList(); err != nil {
				return false, err
			}
			affectedChains = append(affectedChains, chainID)
		} else {
			removed, err := chain.PruneDenyListAfter(repairTS)
			if err != nil {
				return false, err
			}
			if removed {
				affectedChains = append(affectedChains, chainID)
			}
		}
	}

	i.mu.Lock()
	if err := i.rewindAcceptedStateLocked(keep); err != nil {
		i.mu.Unlock()
		return false, err
	}
	i.mu.Unlock()

	for _, chainID := range affectedChains {
		chain, ok := i.chains[chainID]
		if !ok {
			continue
		}
		if err := chain.RewindEngine(i.ctx, repairTS, eth.BlockRef{}); err != nil {
			return false, err
		}
	}

	return true, nil
}

func (i *Interop) findKeptAcceptedResult(lastTimestamp uint64) (*VerifiedResult, error) {
	for ts := lastTimestamp; ; ts-- {
		stored, err := i.verifiedDB.Get(ts)
		if err != nil {
			if ts == 0 {
				return nil, nil
			}
			continue
		}
		current, err := i.collectSnapshotAtTimestamp(ts)
		if err != nil {
			if errors.Is(err, ethereum.NotFound) {
				if ts == 0 || ts == i.activationTimestamp {
					return nil, nil
				}
				continue
			}
			return nil, err
		}
		consistent, err := i.checker.AcceptedResultConsistent(i.ctx, stored, current)
		if err != nil {
			return nil, err
		}
		if consistent {
			return &stored, nil
		}
		if ts == 0 || ts == i.activationTimestamp {
			return nil, nil
		}
	}
}

func (i *Interop) repairResetTimestamp(keep *VerifiedResult) uint64 {
	if keep != nil {
		return keep.Timestamp
	}
	if i.activationTimestamp > 0 {
		return i.activationTimestamp - 1
	}
	return 0
}

func (i *Interop) rewindAcceptedStateLocked(keep *VerifiedResult) error {
	if keep == nil {
		if _, err := i.verifiedDB.Rewind(0); err != nil {
			return err
		}
		for chainID, db := range i.logsDBs {
			if err := db.Clear(&noopInvalidator{}); err != nil {
				i.log.Error("failed to clear logsDB after accepted-state repair", "chainID", chainID, "err", err)
			}
		}
		i.currentL1 = eth.BlockID{}
		i.clearValidatedBoundary()
		return nil
	}

	if _, err := i.verifiedDB.RewindAfter(keep.Timestamp); err != nil {
		return err
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
	return nil
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
