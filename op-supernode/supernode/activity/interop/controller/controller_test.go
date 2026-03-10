package controller

import (
	"context"
	"testing"

	"github.com/ethereum-optimism/optimism/op-service/eth"
	interopengine "github.com/ethereum-optimism/optimism/op-supernode/supernode/activity/interop/engine"
	"github.com/ethereum/go-ethereum/common"
	"github.com/stretchr/testify/require"
)

type stubStore struct {
	state   interopengine.InteropState
	commits []interopengine.InteropState
}

func (s *stubStore) Load() (interopengine.InteropState, error) {
	return interopengine.CopyState(s.state), nil
}

func (s *stubStore) Commit(state interopengine.InteropState) error {
	s.state = interopengine.CopyState(state)
	s.commits = append(s.commits, interopengine.CopyState(state))
	return nil
}

type stubSource struct {
	observation interopengine.RoundObservation
	calls       int
}

func (s *stubSource) ObserveRound(context.Context, *uint64, uint64) (interopengine.RoundObservation, error) {
	s.calls++
	return s.observation, nil
}

type stubResolver struct {
	evidence FrontierEvidence
	calls    int
}

func (s *stubResolver) ResolveFrontier(context.Context, interopengine.FrontierSnapshot) (FrontierEvidence, error) {
	s.calls++
	return s.evidence, nil
}

type stubChecker struct {
	same  bool
	err   error
	calls int
}

func (s *stubChecker) SameL1Chain(context.Context, []eth.BlockID) (bool, error) {
	s.calls++
	return s.same, s.err
}

type stubVerifier struct {
	result interopengine.VerificationResult
	calls  int
}

func (s *stubVerifier) Verify(context.Context, interopengine.RoundObservation, FrontierEvidence) (interopengine.VerificationResult, error) {
	s.calls++
	return s.result, nil
}

type stubRunner struct {
	calls   int
	batches [][]interopengine.PendingEffect
}

func (s *stubRunner) Run(_ context.Context, pending []interopengine.PendingEffect) error {
	s.calls++
	cloned := make([]interopengine.PendingEffect, len(pending))
	copy(cloned, pending)
	s.batches = append(s.batches, cloned)
	return nil
}

func TestControllerDrainsPendingEffectsBeforeStepping(t *testing.T) {
	t.Parallel()

	engine, err := interopengine.New(interopengine.Config{ActivationTimestamp: 100})
	require.NoError(t, err)

	store := &stubStore{
		state: interopengine.InteropState{
			PendingEffects: []interopengine.PendingEffect{{
				ID:     interopengine.EffectID(interopengine.PruneDeniedDecisions{AfterTimestamp: 99}),
				Effect: interopengine.PruneDeniedDecisions{AfterTimestamp: 99},
			}},
		},
	}
	source := &stubSource{
		observation: interopengine.RoundObservation{
			AcceptedTS: 0,
			Accepted: interopengine.SnapshotAvailability[interopengine.AcceptedSnapshot]{
				Present: false,
				Reason:  interopengine.AvailabilityPreActivation,
			},
			FrontierTS: 100,
			Frontier: interopengine.SnapshotAvailability[interopengine.FrontierSnapshot]{
				Present: false,
				Reason:  interopengine.AvailabilityNotReady,
			},
		},
	}
	runner := &stubRunner{}
	controller := New(100, engine, store, source, &stubChecker{same: true}, &stubResolver{}, &stubVerifier{}, runner)

	result, err := controller.Step(context.Background())
	require.NoError(t, err)
	require.Equal(t, interopengine.OutcomeWait, result.Outcome)
	require.Equal(t, 1, runner.calls)
	require.Len(t, store.commits, 2)
	require.Empty(t, store.commits[0].PendingEffects)
	require.Empty(t, store.commits[1].PendingEffects)
	require.Equal(t, 1, source.calls)
}

func TestControllerAdvancesAndExecutesStepEffects(t *testing.T) {
	t.Parallel()

	engine, err := interopengine.New(interopengine.Config{ActivationTimestamp: 100})
	require.NoError(t, err)

	source := &stubSource{
		observation: interopengine.RoundObservation{
			AcceptedTS: 0,
			Accepted: interopengine.SnapshotAvailability[interopengine.AcceptedSnapshot]{
				Present: false,
				Reason:  interopengine.AvailabilityPreActivation,
			},
			FrontierTS: 100,
			Frontier: interopengine.SnapshotAvailability[interopengine.FrontierSnapshot]{
				Present: true,
				Reason:  interopengine.AvailabilityPresent,
				Value: interopengine.FrontierSnapshot{
					Timestamp:   100,
					L1Inclusion: eth.BlockID{Hash: common.HexToHash("0x11"), Number: 5},
					L1Heads: map[eth.ChainID]eth.BlockID{
						eth.ChainIDFromUInt64(10): {Hash: common.HexToHash("0x11"), Number: 5},
					},
					L2Heads: map[eth.ChainID]eth.BlockID{
						eth.ChainIDFromUInt64(10): {Hash: common.HexToHash("0x22"), Number: 100},
					},
				},
			},
		},
	}
	resolver := &stubResolver{evidence: FrontierEvidence{Timestamp: 100}}
	verifier := &stubVerifier{
		result: interopengine.VerificationResult{
			Timestamp: 100,
			Status:    interopengine.VerificationValid,
		},
	}
	store := &stubStore{state: interopengine.InteropState{}}
	runner := &stubRunner{}
	controller := New(100, engine, store, source, &stubChecker{same: true}, resolver, verifier, runner)

	result, err := controller.Step(context.Background())
	require.NoError(t, err)
	require.Equal(t, interopengine.OutcomeAdvance, result.Outcome)
	require.Equal(t, 1, resolver.calls)
	require.Equal(t, 1, verifier.calls)
	require.Equal(t, 0, runner.calls)
	require.Len(t, store.commits, 1)
	require.NotNil(t, store.state.Accepted)
}

func TestControllerSkipsResolverAndVerifierWhenFrontierNotReady(t *testing.T) {
	t.Parallel()

	engine, err := interopengine.New(interopengine.Config{ActivationTimestamp: 100})
	require.NoError(t, err)

	store := &stubStore{state: interopengine.InteropState{}}
	source := &stubSource{
		observation: interopengine.RoundObservation{
			AcceptedTS: 0,
			Accepted: interopengine.SnapshotAvailability[interopengine.AcceptedSnapshot]{
				Present: false,
				Reason:  interopengine.AvailabilityPreActivation,
			},
			FrontierTS: 100,
			Frontier: interopengine.SnapshotAvailability[interopengine.FrontierSnapshot]{
				Present: false,
				Reason:  interopengine.AvailabilityNotReady,
			},
		},
	}
	resolver := &stubResolver{}
	verifier := &stubVerifier{}
	runner := &stubRunner{}
	controller := New(100, engine, store, source, &stubChecker{same: true}, resolver, verifier, runner)

	result, err := controller.Step(context.Background())
	require.NoError(t, err)
	require.Equal(t, interopengine.OutcomeWait, result.Outcome)
	require.Equal(t, 0, resolver.calls)
	require.Equal(t, 0, verifier.calls)
}

func TestControllerRunsInvalidationEffectsForInvalidFrontier(t *testing.T) {
	t.Parallel()

	engine, err := interopengine.New(interopengine.Config{ActivationTimestamp: 100})
	require.NoError(t, err)

	chainA := eth.ChainIDFromUInt64(10)
	source := &stubSource{
		observation: interopengine.RoundObservation{
			AcceptedTS: 0,
			Accepted: interopengine.SnapshotAvailability[interopengine.AcceptedSnapshot]{
				Present: false,
				Reason:  interopengine.AvailabilityPreActivation,
			},
			FrontierTS: 100,
			Frontier: interopengine.SnapshotAvailability[interopengine.FrontierSnapshot]{
				Present: true,
				Reason:  interopengine.AvailabilityPresent,
				Value: interopengine.FrontierSnapshot{
					Timestamp:   100,
					L1Inclusion: eth.BlockID{Hash: common.HexToHash("0x11"), Number: 5},
					L1Heads: map[eth.ChainID]eth.BlockID{
						chainA: {Hash: common.HexToHash("0x11"), Number: 5},
					},
					L2Heads: map[eth.ChainID]eth.BlockID{
						chainA: {Hash: common.HexToHash("0x22"), Number: 100},
					},
				},
			},
		},
	}
	resolver := &stubResolver{evidence: FrontierEvidence{Timestamp: 100}}
	verifier := &stubVerifier{
		result: interopengine.VerificationResult{
			Timestamp: 100,
			Status:    interopengine.VerificationInvalid,
			InvalidHeads: map[eth.ChainID]eth.BlockID{
				chainA: {Hash: common.HexToHash("0x22"), Number: 100},
			},
		},
	}
	store := &stubStore{state: interopengine.InteropState{}}
	runner := &stubRunner{}
	controller := New(100, engine, store, source, &stubChecker{same: true}, resolver, verifier, runner)

	result, err := controller.Step(context.Background())
	require.NoError(t, err)
	require.Equal(t, interopengine.OutcomeNoOp, result.Outcome)
	require.Equal(t, 1, runner.calls)
	require.Len(t, runner.batches, 1)
	require.Len(t, runner.batches[0], 1)
	require.Equal(t, interopengine.InvalidateChainHead{
		ChainID:   chainA,
		Timestamp: 100,
		Block:     eth.BlockID{Hash: common.HexToHash("0x22"), Number: 100},
	}, runner.batches[0][0].Effect)
	require.Len(t, store.commits, 2)
	require.Len(t, store.commits[0].PendingEffects, 1)
	require.Empty(t, store.commits[1].PendingEffects)
}

func TestControllerTreatsMixedAcceptedAndFrontierL1AsConflict(t *testing.T) {
	t.Parallel()

	engine, err := interopengine.New(interopengine.Config{ActivationTimestamp: 100})
	require.NoError(t, err)

	chainA := eth.ChainIDFromUInt64(10)
	acceptedTS := uint64(100)
	store := &stubStore{
		state: interopengine.InteropState{
			Accepted: &interopengine.AcceptedSnapshot{
				Timestamp:   100,
				L1Inclusion: eth.BlockID{Hash: common.HexToHash("0x11"), Number: 5},
				L1Heads: map[eth.ChainID]eth.BlockID{
					chainA: {Hash: common.HexToHash("0x11"), Number: 5},
				},
				L2Heads: map[eth.ChainID]eth.BlockID{
					chainA: {Hash: common.HexToHash("0x22"), Number: 100},
				},
			},
			AcceptedHistory: map[uint64]interopengine.AcceptedSnapshot{
				100: {
					Timestamp:   100,
					L1Inclusion: eth.BlockID{Hash: common.HexToHash("0x11"), Number: 5},
					L1Heads: map[eth.ChainID]eth.BlockID{
						chainA: {Hash: common.HexToHash("0x11"), Number: 5},
					},
					L2Heads: map[eth.ChainID]eth.BlockID{
						chainA: {Hash: common.HexToHash("0x22"), Number: 100},
					},
				},
			},
			LastValidatedTS: &acceptedTS,
		},
	}
	source := &stubSource{
		observation: interopengine.RoundObservation{
			AcceptedTS: 100,
			Accepted: interopengine.SnapshotAvailability[interopengine.AcceptedSnapshot]{
				Present: true,
				Reason:  interopengine.AvailabilityPresent,
				Value:   *store.state.Accepted,
			},
			FrontierTS: 101,
			Frontier: interopengine.SnapshotAvailability[interopengine.FrontierSnapshot]{
				Present: true,
				Reason:  interopengine.AvailabilityPresent,
				Value: interopengine.FrontierSnapshot{
					Timestamp:   101,
					L1Inclusion: eth.BlockID{Hash: common.HexToHash("0x33"), Number: 6},
					L1Heads: map[eth.ChainID]eth.BlockID{
						chainA: {Hash: common.HexToHash("0x33"), Number: 6},
					},
					L2Heads: map[eth.ChainID]eth.BlockID{
						chainA: {Hash: common.HexToHash("0x44"), Number: 101},
					},
				},
			},
		},
	}
	resolver := &stubResolver{}
	verifier := &stubVerifier{}
	controller := New(100, engine, store, source, &stubChecker{same: false}, resolver, verifier, &stubRunner{})

	result, err := controller.Step(context.Background())
	require.NoError(t, err)
	require.Equal(t, interopengine.OutcomeConflict, result.Outcome)
	require.Zero(t, resolver.calls)
	require.Zero(t, verifier.calls)
}
