package interop

import (
	"errors"
	"fmt"

	"github.com/ethereum-optimism/optimism/op-service/eth"
	interopengine "github.com/ethereum-optimism/optimism/op-supernode/supernode/activity/interop/engine"
	"github.com/ethereum/go-ethereum"
)

func (i *Interop) observeAcceptedSnapshot(ts *uint64) (interopengine.SnapshotAvailability[interopengine.AcceptedSnapshot], error) {
	if ts == nil {
		return interopengine.SnapshotAvailability[interopengine.AcceptedSnapshot]{
			Present: false,
			Reason:  interopengine.AvailabilityPreActivation,
		}, nil
	}

	frontier, err := i.observeFrontierSnapshot(*ts)
	if err != nil {
		return interopengine.SnapshotAvailability[interopengine.AcceptedSnapshot]{}, err
	}
	if !frontier.Present {
		return interopengine.SnapshotAvailability[interopengine.AcceptedSnapshot]{
			Present: false,
			Reason:  frontier.Reason,
		}, nil
	}

	return interopengine.SnapshotAvailability[interopengine.AcceptedSnapshot]{
		Present: true,
		Reason:  interopengine.AvailabilityPresent,
		Value: interopengine.AcceptedSnapshot{
			Timestamp:   frontier.Value.Timestamp,
			L1Inclusion: frontier.Value.L1Inclusion,
			L1Heads:     frontier.Value.L1Heads,
			L2Heads:     frontier.Value.L2Heads,
		},
	}, nil
}

func (i *Interop) observeFrontierSnapshot(ts uint64) (interopengine.SnapshotAvailability[interopengine.FrontierSnapshot], error) {
	blocksAtTimestamp, err := i.checkChainsReady(ts)
	if err != nil {
		if errors.Is(err, ethereum.NotFound) {
			return interopengine.SnapshotAvailability[interopengine.FrontierSnapshot]{
				Present: false,
				Reason:  interopengine.AvailabilityNotReady,
			}, nil
		}
		return interopengine.SnapshotAvailability[interopengine.FrontierSnapshot]{}, err
	}

	l1Heads := make(map[eth.ChainID]eth.BlockID, len(blocksAtTimestamp))
	for chainID, expectedBlock := range blocksAtTimestamp {
		chain, ok := i.chains[chainID]
		if !ok {
			return interopengine.SnapshotAvailability[interopengine.FrontierSnapshot]{}, fmt.Errorf("chain %s missing during frontier observation", chainID)
		}
		optimisticL2, l1Head, err := chain.OptimisticAt(i.ctx, ts)
		if err != nil {
			if errors.Is(err, ethereum.NotFound) {
				return interopengine.SnapshotAvailability[interopengine.FrontierSnapshot]{
					Present: false,
					Reason:  interopengine.AvailabilityNotReady,
				}, nil
			}
			return interopengine.SnapshotAvailability[interopengine.FrontierSnapshot]{}, err
		}
		if optimisticL2 != expectedBlock {
			i.log.Warn("frontier observation conflict",
				"chain", chainID,
				"timestamp", ts,
				"localSafe", expectedBlock,
				"optimistic", optimisticL2,
			)
			return interopengine.SnapshotAvailability[interopengine.FrontierSnapshot]{
				Present: false,
				Reason:  interopengine.AvailabilityConflict,
			}, nil
		}
		l1Heads[chainID] = l1Head
	}

	return interopengine.SnapshotAvailability[interopengine.FrontierSnapshot]{
		Present: true,
		Reason:  interopengine.AvailabilityPresent,
		Value: interopengine.FrontierSnapshot{
			Timestamp:   ts,
			L1Inclusion: maxBlockID(l1Heads),
			L1Heads:     l1Heads,
			L2Heads:     blocksAtTimestamp,
		},
	}, nil
}

func maxBlockID(heads map[eth.ChainID]eth.BlockID) eth.BlockID {
	var maxHead eth.BlockID
	first := true
	for _, head := range heads {
		if first || head.Number > maxHead.Number {
			maxHead = head
			first = false
		}
	}
	return maxHead
}
