package interop

import (
	"fmt"

	"github.com/ethereum-optimism/optimism/op-service/eth"
	interopengine "github.com/ethereum-optimism/optimism/op-supernode/supernode/activity/interop/engine"
	interopstore "github.com/ethereum-optimism/optimism/op-supernode/supernode/activity/interop/store"
)

type stateStoreBridge struct {
	activationTimestamp uint64
	verifiedDB          *VerifiedDB
	store               *interopstore.Store
}

func importVerifiedDBIfNeeded(activationTimestamp uint64, verifiedDB *VerifiedDB, store *interopstore.Store) error {
	s := stateStoreBridge{
		activationTimestamp: activationTimestamp,
		verifiedDB:          verifiedDB,
		store:               store,
	}
	state, err := s.store.Load()
	if err != nil {
		return err
	}
	if !stateStoreEmpty(state) {
		return nil
	}
	_, err = s.importVerifiedDB()
	return err
}

func stateStoreEmpty(state interopengine.InteropState) bool {
	return state.Accepted == nil &&
		len(state.AcceptedHistory) == 0 &&
		len(state.DeniedByTS) == 0 &&
		state.LastValidatedTS == nil &&
		len(state.PendingEffects) == 0
}

func (s *stateStoreBridge) importVerifiedDB() (interopengine.InteropState, error) {
	lastTS, initialized := s.verifiedDB.LastTimestamp()
	if !initialized {
		return interopengine.InteropState{}, nil
	}
	history := make(map[uint64]interopengine.AcceptedSnapshot, lastTS-s.activationTimestamp+1)
	for ts := s.activationTimestamp; ts <= lastTS; ts++ {
		result, err := s.verifiedDB.Get(ts)
		if err != nil {
			return interopengine.InteropState{}, fmt.Errorf("import verified result at timestamp %d: %w", ts, err)
		}
		history[ts] = acceptedSnapshotFromVerifiedResult(result)
	}
	accepted := history[lastTS]
	validatedTS := lastTS
	state := interopengine.InteropState{
		Accepted:        &accepted,
		AcceptedHistory: history,
		DeniedByTS:      map[uint64][]interopengine.DeniedDecision{},
		LastValidatedTS: &validatedTS,
	}
	if err := s.store.Commit(state); err != nil {
		return interopengine.InteropState{}, fmt.Errorf("persist imported state: %w", err)
	}
	return state, nil
}

func acceptedSnapshotFromVerifiedResult(result VerifiedResult) interopengine.AcceptedSnapshot {
	// Legacy verified results do not persist per-chain L1 heads. When importing
	// legacy state, seed each chain with the committed L1 inclusion so the new
	// state store can round-trip the accepted prefix while the live controller
	// remains unused. Once the controller is authoritative, snapshots will be
	// collected with exact per-chain L1 heads.
	l1Heads := make(map[eth.ChainID]eth.BlockID, len(result.L2Heads))
	for chainID := range result.L2Heads {
		l1Heads[chainID] = result.L1Inclusion
	}
	return interopengine.AcceptedSnapshot{
		Timestamp:   result.Timestamp,
		L1Inclusion: result.L1Inclusion,
		L1Heads:     l1Heads,
		L2Heads:     result.L2Heads,
	}
}
