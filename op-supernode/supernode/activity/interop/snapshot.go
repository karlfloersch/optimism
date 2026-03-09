package interop

import (
	"errors"
	"fmt"

	"github.com/ethereum-optimism/optimism/op-service/eth"
	"github.com/ethereum/go-ethereum"
)

var errSnapshotMoved = errors.New("snapshot moved during collection")

func (i *Interop) setValidatedBoundary(ts uint64, valid bool) {
	i.validated = AcceptedBoundary{Timestamp: ts, Valid: valid}
}

func (i *Interop) clearValidatedBoundary() {
	i.validated = AcceptedBoundary{}
}

func (i *Interop) validatedTimestamp() (uint64, bool) {
	return i.validated.Timestamp, i.validated.Valid
}

func (i *Interop) collectSnapshot(ts uint64, blocksAtTimestamp map[eth.ChainID]eth.BlockID) (VerifiedResult, error) {
	if blocksAtTimestamp == nil {
		var err error
		blocksAtTimestamp, err = i.checkChainsReady(ts)
		if err != nil {
			return VerifiedResult{}, err
		}
	}
	result := VerifiedResult{
		Timestamp: ts,
		L1Heads:   make(map[eth.ChainID]eth.BlockID, len(blocksAtTimestamp)),
		L2Heads:   cloneBlockMap(blocksAtTimestamp),
	}
	for chainID, block := range blocksAtTimestamp {
		chain, ok := i.chains[chainID]
		if !ok {
			return VerifiedResult{}, fmt.Errorf("chain %s not found", chainID)
		}
		optimisticL2, l1Block, err := chain.OptimisticAt(i.ctx, ts)
		if err != nil {
			return VerifiedResult{}, fmt.Errorf("chain %s: failed to get L1 head for block %s: %w", chainID, block, err)
		}
		if optimisticL2 != block {
			return VerifiedResult{}, fmt.Errorf("chain %s: %w (frozen_l2=%s optimistic_l2=%s timestamp=%d)", chainID, errSnapshotMoved, block, optimisticL2, ts)
		}
		result.L1Heads[chainID] = l1Block
	}
	result.L1Inclusion = maxL1Head(result.L1Heads)
	return result, nil
}

func (i *Interop) collectSnapshotAtTimestamp(ts uint64) (VerifiedResult, error) {
	snapshot, err := i.collectSnapshot(ts, nil)
	if err != nil {
		if errorsIsNotFound(err) {
			return VerifiedResult{}, ethereum.NotFound
		}
		return VerifiedResult{}, err
	}
	return snapshot, nil
}

func cloneBlockMap(in map[eth.ChainID]eth.BlockID) map[eth.ChainID]eth.BlockID {
	if len(in) == 0 {
		return nil
	}
	out := make(map[eth.ChainID]eth.BlockID, len(in))
	for chainID, block := range in {
		out[chainID] = block
	}
	return out
}

type DebugSnapshot struct {
	Timestamp   uint64
	L1Inclusion eth.BlockID
	L1Heads     map[eth.ChainID]eth.BlockID
	L2Heads     map[eth.ChainID]eth.BlockID
}

type DebugState struct {
	Accepted *DebugSnapshot
	Frontier *DebugSnapshot
	NextTS   uint64
}

func toDebugSnapshot(in VerifiedResult) *DebugSnapshot {
	return &DebugSnapshot{
		Timestamp:   in.Timestamp,
		L1Inclusion: in.L1Inclusion,
		L1Heads:     cloneBlockMap(in.L1Heads),
		L2Heads:     cloneBlockMap(in.L2Heads),
	}
}

func (i *Interop) DebugState() (*DebugState, error) {
	var accepted *DebugSnapshot
	if result, ok := i.latestValidatedResult(); ok {
		accepted = toDebugSnapshot(result)
	}

	nextTS := i.activationTimestamp
	if lastTimestamp, initialized := i.verifiedDB.LastTimestamp(); initialized {
		nextTS = lastTimestamp + 1
	}

	frontier, err := i.collectSnapshotAtTimestamp(nextTS)
	if err != nil {
		if errorsIsNotFound(err) {
			return &DebugState{
				Accepted: accepted,
				NextTS:   nextTS,
			}, nil
		}
		return nil, err
	}
	return &DebugState{
		Accepted: accepted,
		Frontier: toDebugSnapshot(frontier),
		NextTS:   nextTS,
	}, nil
}

func errorsIsNotFound(err error) bool {
	return errors.Is(err, ethereum.NotFound)
}
