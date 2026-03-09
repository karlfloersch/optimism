package engine

import (
	"testing"

	"github.com/ethereum-optimism/optimism/op-service/eth"
	"github.com/ethereum/go-ethereum/common"
	"github.com/stretchr/testify/require"
)

func TestStepAdvancesFromActivation(t *testing.T) {
	t.Parallel()

	engine, err := New(Config{ActivationTimestamp: 100})
	require.NoError(t, err)

	chainA := eth.ChainIDFromUInt64(10)
	frontier := FrontierSnapshot{
		Timestamp:   100,
		L1Inclusion: eth.BlockID{Hash: common.HexToHash("0x11"), Number: 5},
		L1Heads: map[eth.ChainID]eth.BlockID{
			chainA: {Hash: common.HexToHash("0x11"), Number: 5},
		},
		L2Heads: map[eth.ChainID]eth.BlockID{
			chainA: {Hash: common.HexToHash("0x22"), Number: 100},
		},
	}

	result, err := engine.Step(InteropState{}, StepInput{
		Observation: RoundObservation{
			AcceptedTS: 0,
			Accepted: SnapshotAvailability[AcceptedSnapshot]{
				Present: false,
				Reason:  AvailabilityPreActivation,
			},
			FrontierTS: 100,
			Frontier: SnapshotAvailability[FrontierSnapshot]{
				Present: true,
				Reason:  AvailabilityPresent,
				Value:   frontier,
			},
		},
		Verification: VerificationResult{
			Timestamp: 100,
			Status:    VerificationValid,
		},
	})
	require.NoError(t, err)
	require.Equal(t, OutcomeAdvance, result.Outcome)
	require.NotNil(t, result.NewState.Accepted)
	require.Equal(t, uint64(100), result.NewState.Accepted.Timestamp)
	require.NotNil(t, result.NewState.LastValidatedTS)
	require.Equal(t, uint64(100), *result.NewState.LastValidatedTS)
	require.Contains(t, result.NewState.AcceptedHistory, uint64(100))
}

func TestStepStoresDeniedDecisionForInvalidFrontier(t *testing.T) {
	t.Parallel()

	engine, err := New(Config{ActivationTimestamp: 100})
	require.NoError(t, err)

	chainA := eth.ChainIDFromUInt64(10)
	chainB := eth.ChainIDFromUInt64(20)
	accepted := &AcceptedSnapshot{
		Timestamp:   100,
		L1Inclusion: eth.BlockID{Hash: common.HexToHash("0x11"), Number: 5},
		L1Heads: map[eth.ChainID]eth.BlockID{
			chainA: {Hash: common.HexToHash("0x11"), Number: 5},
		},
		L2Heads: map[eth.ChainID]eth.BlockID{
			chainA: {Hash: common.HexToHash("0x22"), Number: 100},
		},
	}
	validatedTS := uint64(100)
	frontier := FrontierSnapshot{
		Timestamp:   101,
		L1Inclusion: eth.BlockID{Hash: common.HexToHash("0x33"), Number: 6},
		L1Heads: map[eth.ChainID]eth.BlockID{
			chainA: {Hash: common.HexToHash("0x33"), Number: 6},
			chainB: {Hash: common.HexToHash("0x33"), Number: 6},
		},
		L2Heads: map[eth.ChainID]eth.BlockID{
			chainA: {Hash: common.HexToHash("0x44"), Number: 101},
			chainB: {Hash: common.HexToHash("0x55"), Number: 201},
		},
	}

	result, err := engine.Step(InteropState{
		Accepted:        accepted,
		AcceptedHistory: map[uint64]AcceptedSnapshot{100: *accepted},
		LastValidatedTS: &validatedTS,
		DeniedByTS:      map[uint64][]DeniedDecision{},
	}, StepInput{
		Observation: RoundObservation{
			AcceptedTS: 100,
			Accepted: SnapshotAvailability[AcceptedSnapshot]{
				Present: true,
				Reason:  AvailabilityPresent,
				Value:   *accepted,
			},
			FrontierTS: 101,
			Frontier: SnapshotAvailability[FrontierSnapshot]{
				Present: true,
				Reason:  AvailabilityPresent,
				Value:   frontier,
			},
		},
		Verification: VerificationResult{
			Timestamp: 101,
			Status:    VerificationInvalid,
			InvalidHeads: map[eth.ChainID]eth.BlockID{
				chainB: {Hash: common.HexToHash("0x55"), Number: 201},
			},
		},
	})
	require.NoError(t, err)
	require.Equal(t, OutcomeNoOp, result.Outcome)
	require.Len(t, result.NewState.DeniedByTS[101], 1)
	require.Equal(t, frontier, result.NewState.DeniedByTS[101][0].DeniedFrontier)
	require.Len(t, result.Effects, 1)
	require.Equal(t, InvalidateChainHead{
		ChainID:   chainB,
		Timestamp: 101,
		Block:     eth.BlockID{Hash: common.HexToHash("0x55"), Number: 201},
	}, result.Effects[0])
}

func TestStepWaitsWhenFrontierNotReady(t *testing.T) {
	t.Parallel()

	engine, err := New(Config{ActivationTimestamp: 100})
	require.NoError(t, err)

	accepted := &AcceptedSnapshot{
		Timestamp:   100,
		L1Inclusion: eth.BlockID{Hash: common.HexToHash("0x11"), Number: 5},
		L1Heads: map[eth.ChainID]eth.BlockID{
			eth.ChainIDFromUInt64(10): {Hash: common.HexToHash("0x11"), Number: 5},
		},
		L2Heads: map[eth.ChainID]eth.BlockID{
			eth.ChainIDFromUInt64(10): {Hash: common.HexToHash("0x22"), Number: 100},
		},
	}
	validatedTS := uint64(100)

	result, err := engine.Step(InteropState{
		Accepted:        accepted,
		AcceptedHistory: map[uint64]AcceptedSnapshot{100: *accepted},
		LastValidatedTS: &validatedTS,
		DeniedByTS:      map[uint64][]DeniedDecision{},
	}, StepInput{
		Observation: RoundObservation{
			AcceptedTS: 100,
			Accepted: SnapshotAvailability[AcceptedSnapshot]{
				Present: true,
				Reason:  AvailabilityPresent,
				Value:   *accepted,
			},
			FrontierTS: 101,
			Frontier: SnapshotAvailability[FrontierSnapshot]{
				Present: false,
				Reason:  AvailabilityNotReady,
			},
		},
		Verification: VerificationResult{
			Timestamp: 101,
			Status:    VerificationNotReady,
		},
	})
	require.NoError(t, err)
	require.Equal(t, OutcomeWait, result.Outcome)
}

func TestStepPrunesStaleFrontierDeniedDecisionsBeforeWaiting(t *testing.T) {
	t.Parallel()

	engine, err := New(Config{ActivationTimestamp: 100})
	require.NoError(t, err)

	chainA := eth.ChainIDFromUInt64(10)
	accepted := &AcceptedSnapshot{
		Timestamp:   100,
		L1Inclusion: eth.BlockID{Hash: common.HexToHash("0x11"), Number: 5},
		L1Heads: map[eth.ChainID]eth.BlockID{
			chainA: {Hash: common.HexToHash("0x11"), Number: 5},
		},
		L2Heads: map[eth.ChainID]eth.BlockID{
			chainA: {Hash: common.HexToHash("0x22"), Number: 100},
		},
	}
	currentFrontier := FrontierSnapshot{
		Timestamp:   101,
		L1Inclusion: eth.BlockID{Hash: common.HexToHash("0x33"), Number: 6},
		L1Heads: map[eth.ChainID]eth.BlockID{
			chainA: {Hash: common.HexToHash("0x33"), Number: 6},
		},
		L2Heads: map[eth.ChainID]eth.BlockID{
			chainA: {Hash: common.HexToHash("0x44"), Number: 101},
		},
	}
	staleFrontier := currentFrontier
	staleFrontier.L1Inclusion = eth.BlockID{Hash: common.HexToHash("0x55"), Number: 7}
	staleFrontier.L1Heads = map[eth.ChainID]eth.BlockID{
		chainA: {Hash: common.HexToHash("0x55"), Number: 7},
	}
	validatedTS := uint64(100)

	result, err := engine.Step(InteropState{
		Accepted:        accepted,
		AcceptedHistory: map[uint64]AcceptedSnapshot{100: *accepted},
		LastValidatedTS: &validatedTS,
		DeniedByTS: map[uint64][]DeniedDecision{
			101: {{
				Timestamp:      101,
				DeniedFrontier: staleFrontier,
				InvalidHeads: map[eth.ChainID]eth.BlockID{
					chainA: {Hash: common.HexToHash("0x44"), Number: 101},
				},
			}},
		},
	}, StepInput{
		Observation: RoundObservation{
			AcceptedTS: 100,
			Accepted: SnapshotAvailability[AcceptedSnapshot]{
				Present: true,
				Reason:  AvailabilityPresent,
				Value:   *accepted,
			},
			FrontierTS: 101,
			Frontier: SnapshotAvailability[FrontierSnapshot]{
				Present: true,
				Reason:  AvailabilityPresent,
				Value:   currentFrontier,
			},
		},
		Verification: VerificationResult{
			Timestamp: 101,
			Status:    VerificationNotReady,
		},
	})
	require.NoError(t, err)
	require.Equal(t, OutcomeWait, result.Outcome)
	require.NotContains(t, result.NewState.DeniedByTS, uint64(101))
	require.Equal(t, []Effect{PruneFrontierDeniedDecisions{Timestamp: 101}}, result.Effects)
}

func TestStepRejectsAcceptedDriftUntilHistoryExists(t *testing.T) {
	t.Parallel()

	engine, err := New(Config{ActivationTimestamp: 100})
	require.NoError(t, err)

	chainA := eth.ChainIDFromUInt64(10)
	stateAccepted := &AcceptedSnapshot{
		Timestamp:   100,
		L1Inclusion: eth.BlockID{Hash: common.HexToHash("0x11"), Number: 5},
		L1Heads: map[eth.ChainID]eth.BlockID{
			chainA: {Hash: common.HexToHash("0x11"), Number: 5},
		},
		L2Heads: map[eth.ChainID]eth.BlockID{
			chainA: {Hash: common.HexToHash("0x22"), Number: 100},
		},
	}
	observedAccepted := *stateAccepted
	observedAccepted.L1Heads = map[eth.ChainID]eth.BlockID{
		chainA: stateAccepted.L1Heads[chainA],
	}
	observedAccepted.L2Heads = map[eth.ChainID]eth.BlockID{
		chainA: {Hash: common.HexToHash("0x99"), Number: 100},
	}
	validatedTS := uint64(100)

	_, err = engine.Step(InteropState{
		Accepted: stateAccepted,
		AcceptedHistory: map[uint64]AcceptedSnapshot{
			100: *stateAccepted,
		},
		LastValidatedTS: &validatedTS,
		DeniedByTS:      map[uint64][]DeniedDecision{},
	}, StepInput{
		Observation: RoundObservation{
			AcceptedTS: 100,
			Accepted: SnapshotAvailability[AcceptedSnapshot]{
				Present: true,
				Reason:  AvailabilityPresent,
				Value:   observedAccepted,
			},
			FrontierTS: 101,
			Frontier: SnapshotAvailability[FrontierSnapshot]{
				Present: false,
				Reason:  AvailabilityNotReady,
			},
		},
		Verification: VerificationResult{
			Timestamp: 101,
			Status:    VerificationNotReady,
		},
	})
	require.NoError(t, err)
}

func TestStepRewindsOneTimestampAndPrunesFutureDenies(t *testing.T) {
	t.Parallel()

	engine, err := New(Config{ActivationTimestamp: 100})
	require.NoError(t, err)

	chainA := eth.ChainIDFromUInt64(10)
	accepted100 := AcceptedSnapshot{
		Timestamp:   100,
		L1Inclusion: eth.BlockID{Hash: common.HexToHash("0x11"), Number: 5},
		L1Heads: map[eth.ChainID]eth.BlockID{
			chainA: {Hash: common.HexToHash("0x11"), Number: 5},
		},
		L2Heads: map[eth.ChainID]eth.BlockID{
			chainA: {Hash: common.HexToHash("0x22"), Number: 100},
		},
	}
	accepted101 := AcceptedSnapshot{
		Timestamp:   101,
		L1Inclusion: eth.BlockID{Hash: common.HexToHash("0x33"), Number: 6},
		L1Heads: map[eth.ChainID]eth.BlockID{
			chainA: {Hash: common.HexToHash("0x33"), Number: 6},
		},
		L2Heads: map[eth.ChainID]eth.BlockID{
			chainA: {Hash: common.HexToHash("0x44"), Number: 101},
		},
	}
	validatedTS := uint64(101)
	observedAccepted := accepted101
	observedAccepted.L2Heads = map[eth.ChainID]eth.BlockID{
		chainA: {Hash: common.HexToHash("0x99"), Number: 101},
	}

	result, err := engine.Step(InteropState{
		Accepted: &accepted101,
		AcceptedHistory: map[uint64]AcceptedSnapshot{
			100: accepted100,
			101: accepted101,
		},
		LastValidatedTS: &validatedTS,
		DeniedByTS: map[uint64][]DeniedDecision{
			101: {{
				Timestamp:      101,
				DeniedFrontier: FrontierSnapshot{Timestamp: 101},
				InvalidHeads: map[eth.ChainID]eth.BlockID{
					chainA: {Hash: common.HexToHash("0x44"), Number: 101},
				},
			}},
			102: {{
				Timestamp:      102,
				DeniedFrontier: FrontierSnapshot{Timestamp: 102},
				InvalidHeads: map[eth.ChainID]eth.BlockID{
					chainA: {Hash: common.HexToHash("0x55"), Number: 102},
				},
			}},
		},
	}, StepInput{
		Observation: RoundObservation{
			AcceptedTS: 101,
			Accepted: SnapshotAvailability[AcceptedSnapshot]{
				Present: true,
				Reason:  AvailabilityPresent,
				Value:   observedAccepted,
			},
			FrontierTS: 102,
			Frontier: SnapshotAvailability[FrontierSnapshot]{
				Present: false,
				Reason:  AvailabilityNotReady,
			},
		},
		Verification: VerificationResult{
			Timestamp: 102,
			Status:    VerificationNotReady,
		},
	})
	require.NoError(t, err)
	require.Equal(t, OutcomeRewind, result.Outcome)
	require.NotNil(t, result.NewState.Accepted)
	require.Equal(t, uint64(100), result.NewState.Accepted.Timestamp)
	require.Equal(t, uint64(100), *result.NewState.LastValidatedTS)
	require.NotContains(t, result.NewState.DeniedByTS, uint64(101))
	require.NotContains(t, result.NewState.DeniedByTS, uint64(102))
	require.NotContains(t, result.NewState.AcceptedHistory, uint64(101))
	require.Len(t, result.Effects, 2)
	require.Equal(t, PruneDeniedDecisions{AfterTimestamp: 100}, result.Effects[0])
	require.Equal(t, ResetChainToAccepted{
		ChainID:   chainA,
		Timestamp: 100,
		L2Head:    eth.BlockID{Hash: common.HexToHash("0x22"), Number: 100},
	}, result.Effects[1])
}

func TestStepRewindsFromActivationToPreActivation(t *testing.T) {
	t.Parallel()

	engine, err := New(Config{ActivationTimestamp: 100})
	require.NoError(t, err)

	chainA := eth.ChainIDFromUInt64(10)
	accepted := AcceptedSnapshot{
		Timestamp:   100,
		L1Inclusion: eth.BlockID{Hash: common.HexToHash("0x11"), Number: 5},
		L1Heads: map[eth.ChainID]eth.BlockID{
			chainA: {Hash: common.HexToHash("0x11"), Number: 5},
		},
		L2Heads: map[eth.ChainID]eth.BlockID{
			chainA: {Hash: common.HexToHash("0x22"), Number: 100},
		},
	}
	validatedTS := uint64(100)
	observedAccepted := accepted
	observedAccepted.L2Heads = map[eth.ChainID]eth.BlockID{
		chainA: {Hash: common.HexToHash("0x99"), Number: 100},
	}

	result, err := engine.Step(InteropState{
		Accepted: &accepted,
		AcceptedHistory: map[uint64]AcceptedSnapshot{
			100: accepted,
		},
		LastValidatedTS: &validatedTS,
		DeniedByTS: map[uint64][]DeniedDecision{
			100: {{
				Timestamp:      100,
				DeniedFrontier: FrontierSnapshot{Timestamp: 100},
				InvalidHeads: map[eth.ChainID]eth.BlockID{
					chainA: {Hash: common.HexToHash("0x22"), Number: 100},
				},
			}},
		},
	}, StepInput{
		Observation: RoundObservation{
			AcceptedTS: 100,
			Accepted: SnapshotAvailability[AcceptedSnapshot]{
				Present: true,
				Reason:  AvailabilityPresent,
				Value:   observedAccepted,
			},
			FrontierTS: 101,
			Frontier: SnapshotAvailability[FrontierSnapshot]{
				Present: false,
				Reason:  AvailabilityNotReady,
			},
		},
		Verification: VerificationResult{
			Timestamp: 101,
			Status:    VerificationNotReady,
		},
	})
	require.NoError(t, err)
	require.Equal(t, OutcomeRewind, result.Outcome)
	require.Nil(t, result.NewState.Accepted)
	require.Nil(t, result.NewState.LastValidatedTS)
	require.Empty(t, result.NewState.AcceptedHistory)
	require.Empty(t, result.NewState.DeniedByTS)
	require.Len(t, result.Effects, 2)
	require.Equal(t, ClearDeniedDecisions{}, result.Effects[0])
	require.Equal(t, ResetChainToAccepted{
		ChainID:   chainA,
		Timestamp: 99,
		L2Head:    eth.BlockID{},
	}, result.Effects[1])
}
