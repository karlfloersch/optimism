package interop

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/ethereum-optimism/optimism/op-service/eth"
	cc "github.com/ethereum-optimism/optimism/op-supernode/supernode/chain_container"
)

type denyEntryMetadata struct {
	Result Result          `json:"result"`
	Base   *VerifiedResult `json:"base,omitempty"`
}

func (i *Interop) denyEntryMetadata(chainID eth.ChainID, result Result) ([]byte, error) {
	meta := denyEntryMetadata{Result: result}

	chain, ok := i.chains[chainID]
	if !ok {
		return nil, fmt.Errorf("chain %s not found", chainID)
	}
	baseTS := result.Timestamp
	if blockTime := chain.BlockTime(); blockTime > 0 && baseTS >= blockTime {
		baseTS -= blockTime
	} else if baseTS > 0 {
		baseTS--
	}
	if baseTS >= i.activationTimestamp {
		base, err := i.verifiedDB.Get(baseTS)
		if err != nil {
			if !errors.Is(err, ErrNotFound) {
				return nil, err
			}
		} else {
			meta.Base = &base
		}
	}

	return json.Marshal(meta)
}

func (i *Interop) ValidateDeniedEntry(chainID eth.ChainID, entry cc.DenyEntry) (bool, error) {
	if len(entry.Result) == 0 {
		return true, nil
	}

	var meta denyEntryMetadata
	if err := json.Unmarshal(entry.Result, &meta); err != nil {
		return false, fmt.Errorf("failed to decode deny entry metadata: %w", err)
	}
	// Backward-compatible fallback for older entries that only stored Result.
	if meta.Result.IsEmpty() && meta.Base == nil {
		if err := json.Unmarshal(entry.Result, &meta.Result); err != nil {
			return false, fmt.Errorf("failed to decode deny entry result: %w", err)
		}
	}
	invalidHead, ok := meta.Result.InvalidHeads[chainID]
	if !ok || invalidHead.Hash != entry.PayloadHash {
		return false, nil
	}
	if len(meta.Result.L2Heads) == 0 {
		return true, nil
	}
	if meta.Base == nil {
		return true, nil
	}

	i.mu.RLock()
	validatedTS, hasValidated := i.validatedTimestamp()
	i.mu.RUnlock()
	if !hasValidated || validatedTS < meta.Base.Timestamp {
		return false, nil
	}

	base, err := i.verifiedDB.Get(meta.Base.Timestamp)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			// The accepted boundary may already have moved back to this timestamp
			// while verifiedDB is still being repopulated after the rewind. In that
			// short gap, keep the deny entry active.
			return true, nil
		}
		return false, err
	}
	valid, err := i.checker.AcceptedResultConsistent(i.ctx, *meta.Base, base)
	return valid, err
}
