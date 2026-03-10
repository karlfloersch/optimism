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
			name: "pause when paused",
			obs: RoundObservation{
				Paused:       true,
				ChainsReady:  true,
				L1Consistent: true,
			},
			verified: nil,
			want:     DecisionWait,
		},
		{
			name: "wait when chains not ready",
			obs: RoundObservation{
				ChainsReady: false,
			},
			verified: nil,
			want:     DecisionWait,
		},
		{
			name: "rewind when L1 inconsistent",
			obs: RoundObservation{
				ChainsReady:  true,
				L1Consistent: false,
			},
			verified: nil,
			want:     DecisionRewind,
		},
		{
			name: "wait when verification not available",
			obs: RoundObservation{
				ChainsReady:  true,
				L1Consistent: true,
			},
			verified: nil,
			want:     DecisionWait,
		},
		{
			name: "wait when verification result is empty",
			obs: RoundObservation{
				ChainsReady:  true,
				L1Consistent: true,
			},
			verified: &Result{},
			want:     DecisionWait,
		},
		{
			name: "invalidate on invalid verification",
			obs: RoundObservation{
				ChainsReady:  true,
				L1Consistent: true,
			},
			verified: invalidResult,
			want:     DecisionInvalidate,
		},
		{
			name: "advance on valid verification",
			obs: RoundObservation{
				ChainsReady:  true,
				L1Consistent: true,
			},
			verified: validResult,
			want:     DecisionAdvance,
		},
		{
			name: "L1 inconsistency beats valid verification",
			obs: RoundObservation{
				ChainsReady:  true,
				L1Consistent: false,
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
