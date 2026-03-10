package interop

import (
	"testing"

	"github.com/ethereum-optimism/optimism/op-service/eth"
	"github.com/ethereum/go-ethereum/common"
	"github.com/stretchr/testify/require"
)

func TestDecide(t *testing.T) {
	t.Parallel()

	ts := uint64(1000)
	validResult := &Result{
		Timestamp:   ts + 1,
		L1Inclusion: eth.BlockID{Hash: common.HexToHash("0xl1"), Number: 50},
		L2Heads: map[eth.ChainID]eth.BlockID{
			eth.ChainIDFromUInt64(1): {Hash: common.HexToHash("0xa"), Number: 100},
		},
	}
	invalidResult := &Result{
		Timestamp:   ts + 1,
		L1Inclusion: eth.BlockID{Hash: common.HexToHash("0xl1"), Number: 50},
		L2Heads: map[eth.ChainID]eth.BlockID{
			eth.ChainIDFromUInt64(1): {Hash: common.HexToHash("0xa"), Number: 100},
		},
		InvalidHeads: map[eth.ChainID]eth.BlockID{
			eth.ChainIDFromUInt64(2): {Hash: common.HexToHash("0xbad"), Number: 200},
		},
	}

	tests := []struct {
		name     string
		obs      RoundObservation
		verified *Result
		want     Decision
	}{
		{
			name: "rewind takes priority over everything",
			obs: RoundObservation{
				LastVerifiedTS:     &ts,
				AcceptedStillValid: false,
				ChainsReady:        true,
				L1Consistent:       true,
			},
			verified: validResult,
			want:     DecisionRewind,
		},
		{
			name: "pause when paused",
			obs: RoundObservation{
				AcceptedStillValid: true,
				Paused:             true,
				ChainsReady:        true,
				L1Consistent:       true,
			},
			verified: nil,
			want:     DecisionWait,
		},
		{
			name: "wait when chains not ready",
			obs: RoundObservation{
				AcceptedStillValid: true,
				ChainsReady:        false,
			},
			verified: nil,
			want:     DecisionWait,
		},
		{
			name: "conflict when L1 inconsistent",
			obs: RoundObservation{
				AcceptedStillValid: true,
				ChainsReady:        true,
				L1Consistent:       false,
			},
			verified: nil,
			want:     DecisionConflict,
		},
		{
			name: "wait when verification not available",
			obs: RoundObservation{
				AcceptedStillValid: true,
				ChainsReady:        true,
				L1Consistent:       true,
			},
			verified: nil,
			want:     DecisionWait,
		},
		{
			name: "wait when verification result is empty",
			obs: RoundObservation{
				AcceptedStillValid: true,
				ChainsReady:        true,
				L1Consistent:       true,
			},
			verified: &Result{},
			want:     DecisionWait,
		},
		{
			name: "invalidate on invalid verification",
			obs: RoundObservation{
				AcceptedStillValid: true,
				ChainsReady:        true,
				L1Consistent:       true,
			},
			verified: invalidResult,
			want:     DecisionInvalidate,
		},
		{
			name: "advance on valid verification",
			obs: RoundObservation{
				AcceptedStillValid: true,
				ChainsReady:        true,
				L1Consistent:       true,
			},
			verified: validResult,
			want:     DecisionAdvance,
		},
		{
			name: "no verified state means no rewind check",
			obs: RoundObservation{
				LastVerifiedTS:     nil,
				AcceptedStillValid: false, // shouldn't matter since LastVerifiedTS is nil
				ChainsReady:        true,
				L1Consistent:       true,
			},
			verified: validResult,
			want:     DecisionAdvance,
		},
		{
			name: "rewind beats L1 inconsistency",
			obs: RoundObservation{
				LastVerifiedTS:     &ts,
				AcceptedStillValid: false,
				ChainsReady:        true,
				L1Consistent:       false,
			},
			verified: validResult,
			want:     DecisionRewind,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := Decide(tt.obs, tt.verified)
			require.Equal(t, tt.want, got.Decision, "unexpected decision")

			if tt.want == DecisionInvalidate {
				require.NotEmpty(t, got.Result.InvalidHeads, "invalidate should carry invalid heads")
			}
			if tt.want == DecisionAdvance {
				require.False(t, got.Result.IsEmpty(), "advance should carry result")
				require.True(t, got.Result.IsValid(), "advance result should be valid")
			}
		})
	}
}
