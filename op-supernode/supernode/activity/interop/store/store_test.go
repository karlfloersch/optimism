package store

import (
	"testing"

	"github.com/ethereum-optimism/optimism/op-service/eth"
	interopengine "github.com/ethereum-optimism/optimism/op-supernode/supernode/activity/interop/engine"
	"github.com/ethereum/go-ethereum/common"
	"github.com/stretchr/testify/require"
)

func TestStoreLoadEmptyState(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	store, err := Open(dir)
	require.NoError(t, err)
	defer func() { require.NoError(t, store.Close()) }()

	state, err := store.Load()
	require.NoError(t, err)
	require.Nil(t, state.Accepted)
	require.Nil(t, state.LastValidatedTS)
	require.Empty(t, state.AcceptedHistory)
	require.Empty(t, state.DeniedByTS)
	require.Empty(t, state.PendingEffects)
}

func TestStoreRoundTripsInteropState(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	store, err := Open(dir)
	require.NoError(t, err)

	chainA := eth.ChainIDFromUInt64(10)
	validatedTS := uint64(101)
	state := interopengine.InteropState{
		Accepted: &interopengine.AcceptedSnapshot{
			Timestamp:   101,
			L1Inclusion: eth.BlockID{Hash: common.HexToHash("0x11"), Number: 5},
			L1Heads: map[eth.ChainID]eth.BlockID{
				chainA: {Hash: common.HexToHash("0x11"), Number: 5},
			},
			L2Heads: map[eth.ChainID]eth.BlockID{
				chainA: {Hash: common.HexToHash("0x22"), Number: 101},
			},
		},
		AcceptedHistory: map[uint64]interopengine.AcceptedSnapshot{
			101: {
				Timestamp:   101,
				L1Inclusion: eth.BlockID{Hash: common.HexToHash("0x11"), Number: 5},
				L1Heads: map[eth.ChainID]eth.BlockID{
					chainA: {Hash: common.HexToHash("0x11"), Number: 5},
				},
				L2Heads: map[eth.ChainID]eth.BlockID{
					chainA: {Hash: common.HexToHash("0x22"), Number: 101},
				},
			},
		},
		DeniedByTS: map[uint64][]interopengine.DeniedDecision{
			102: {{
				Timestamp: 102,
				DeniedFrontier: interopengine.FrontierSnapshot{
					Timestamp:   102,
					L1Inclusion: eth.BlockID{Hash: common.HexToHash("0x33"), Number: 6},
					L1Heads: map[eth.ChainID]eth.BlockID{
						chainA: {Hash: common.HexToHash("0x33"), Number: 6},
					},
					L2Heads: map[eth.ChainID]eth.BlockID{
						chainA: {Hash: common.HexToHash("0x44"), Number: 102},
					},
				},
				InvalidHeads: map[eth.ChainID]eth.BlockID{
					chainA: {Hash: common.HexToHash("0x44"), Number: 102},
				},
			}},
		},
		LastValidatedTS: &validatedTS,
		PendingEffects: []interopengine.PendingEffect{
			{
				ID:     interopengine.EffectID(interopengine.PruneDeniedDecisions{AfterTimestamp: 101}),
				Effect: interopengine.PruneDeniedDecisions{AfterTimestamp: 101},
			},
			{
				ID: interopengine.EffectID(interopengine.ResetChainToAccepted{
					ChainID:   chainA,
					Timestamp: 101,
					L2Head:    eth.BlockID{Hash: common.HexToHash("0x22"), Number: 101},
				}),
				Effect: interopengine.ResetChainToAccepted{
					ChainID:   chainA,
					Timestamp: 101,
					L2Head:    eth.BlockID{Hash: common.HexToHash("0x22"), Number: 101},
				},
			},
			{
				ID:     interopengine.EffectID(interopengine.ClearDeniedDecisions{}),
				Effect: interopengine.ClearDeniedDecisions{},
			},
			{
				ID: interopengine.EffectID(interopengine.InvalidateChainHead{
					ChainID: chainA,
					Block:   eth.BlockID{Hash: common.HexToHash("0x44"), Number: 102},
				}),
				Effect: interopengine.InvalidateChainHead{
					ChainID: chainA,
					Block:   eth.BlockID{Hash: common.HexToHash("0x44"), Number: 102},
				},
			},
		},
	}

	require.NoError(t, store.Commit(state))
	require.NoError(t, store.Close())

	store, err = Open(dir)
	require.NoError(t, err)
	defer func() { require.NoError(t, store.Close()) }()

	loaded, err := store.Load()
	require.NoError(t, err)
	require.Equal(t, state, loaded)
}
