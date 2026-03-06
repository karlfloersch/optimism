package interop

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/ethereum-optimism/optimism/op-service/eth"
	cc "github.com/ethereum-optimism/optimism/op-supernode/supernode/chain_container"
	"github.com/ethereum/go-ethereum"
)

func (i *Interop) denyEntryMetadata(result Result) ([]byte, error) {
	return json.Marshal(result)
}

func (i *Interop) ValidateDeniedEntry(chainID eth.ChainID, entry cc.DenyEntry) (bool, error) {
	if len(entry.Result) == 0 {
		return true, nil
	}

	var stored Result
	if err := json.Unmarshal(entry.Result, &stored); err != nil {
		return false, fmt.Errorf("failed to decode deny entry result: %w", err)
	}
	invalidHead, ok := stored.InvalidHeads[chainID]
	if !ok || invalidHead.Hash != entry.PayloadHash {
		return false, nil
	}
	if len(stored.L2Heads) == 0 {
		return true, nil
	}

	current, err := i.collectSnapshotAtTimestamp(stored.Timestamp)
	if err != nil {
		if errors.Is(err, ethereum.NotFound) {
			return false, nil
		}
		return false, err
	}
	return i.checker.AcceptedResultConsistent(i.ctx, stored.ToVerifiedResult(), current)
}
