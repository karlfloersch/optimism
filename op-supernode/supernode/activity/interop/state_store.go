package interop

import (
	"fmt"

	"github.com/ethereum-optimism/optimism/op-service/eth"
	interopengine "github.com/ethereum-optimism/optimism/op-supernode/supernode/activity/interop/engine"
	interopstore "github.com/ethereum-optimism/optimism/op-supernode/supernode/activity/interop/store"
)

type legacyStateStore struct {
	activationTimestamp uint64
	verifiedDB          *VerifiedDB
	store               *interopstore.Store
}

func newLegacyStateStore(activationTimestamp uint64, verifiedDB *VerifiedDB, store *interopstore.Store) *legacyStateStore {
	return &legacyStateStore{
		activationTimestamp: activationTimestamp,
		verifiedDB:          verifiedDB,
		store:               store,
	}
}

func (s *legacyStateStore) Load() (interopengine.InteropState, error) {
	state, err := s.store.Load()
	if err != nil {
		return interopengine.InteropState{}, err
	}
	if !stateStoreEmpty(state) {
		return state, nil
	}
	return s.importVerifiedDB()
}

func (s *legacyStateStore) Commit(state interopengine.InteropState) error {
	if err := s.store.Commit(state); err != nil {
		return err
	}
	return s.mirrorVerifiedDB(state)
}

func stateStoreEmpty(state interopengine.InteropState) bool {
	return state.Accepted == nil &&
		len(state.AcceptedHistory) == 0 &&
		len(state.DeniedByTS) == 0 &&
		state.LastValidatedTS == nil &&
		len(state.PendingEffects) == 0
}

func (s *legacyStateStore) importVerifiedDB() (interopengine.InteropState, error) {
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

func (s *legacyStateStore) mirrorVerifiedDB(state interopengine.InteropState) error {
	if state.Accepted == nil {
		if s.activationTimestamp == 0 {
			_, err := s.verifiedDB.Rewind(0)
			return err
		}
		_, err := s.verifiedDB.Rewind(s.activationTimestamp)
		return err
	}

	_, err := s.verifiedDB.Rewind(s.activationTimestamp)
	if err != nil {
		return err
	}
	for ts := s.activationTimestamp; ts <= state.Accepted.Timestamp; ts++ {
		snapshot, ok := state.AcceptedHistory[ts]
		if !ok {
			return fmt.Errorf("accepted history missing timestamp %d during verifiedDB mirror", ts)
		}
		if err := s.verifiedDB.Commit(verifiedResultFromAcceptedSnapshot(snapshot)); err != nil {
			return fmt.Errorf("mirror verified result at timestamp %d: %w", ts, err)
		}
	}
	return nil
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

func verifiedResultFromAcceptedSnapshot(snapshot interopengine.AcceptedSnapshot) VerifiedResult {
	return VerifiedResult{
		Timestamp:   snapshot.Timestamp,
		L1Inclusion: snapshot.L1Inclusion,
		L2Heads:     snapshot.L2Heads,
	}
}
