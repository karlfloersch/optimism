package interop

import (
	"context"
	"testing"

	"github.com/ethereum-optimism/optimism/op-service/eth"
	"github.com/ethereum/go-ethereum/common"
	"github.com/stretchr/testify/require"
)

type stubL1Source struct {
	refs map[uint64]eth.L1BlockRef
}

func (s stubL1Source) L1BlockRefByNumber(_ context.Context, num uint64) (eth.L1BlockRef, error) {
	return s.refs[num], nil
}

func TestByNumberChecker_AllowsGenesisHeadWithoutHash(t *testing.T) {
	checker := NewByNumberChecker(stubL1Source{
		refs: map[uint64]eth.L1BlockRef{
			0: {Number: 0, Hash: common.HexToHash("0x1234")},
		},
	})

	snapshot := VerifiedResult{
		Timestamp:   100,
		L1Inclusion: eth.BlockID{Number: 0},
		L1Heads: map[eth.ChainID]eth.BlockID{
			eth.ChainIDFromUInt64(10): {Number: 0},
			eth.ChainIDFromUInt64(11): {Number: 0},
		},
		L2Heads: map[eth.ChainID]eth.BlockID{
			eth.ChainIDFromUInt64(10): {Number: 0, Hash: common.HexToHash("0xa")},
			eth.ChainIDFromUInt64(11): {Number: 0, Hash: common.HexToHash("0xb")},
		},
	}

	ok, err := checker.FrontierConsistent(context.Background(), VerifiedResult{}, snapshot)
	require.NoError(t, err)
	require.True(t, ok)
}

func TestByNumberChecker_RejectsFrontierL1Regression(t *testing.T) {
	checker := NewByNumberChecker(stubL1Source{
		refs: map[uint64]eth.L1BlockRef{
			99:  {Number: 99, Hash: common.HexToHash("0x99")},
			100: {Number: 100, Hash: common.HexToHash("0x100")},
		},
	})

	chainID := eth.ChainIDFromUInt64(10)
	accepted := VerifiedResult{
		Timestamp:   1000,
		L1Inclusion: eth.BlockID{Number: 100, Hash: common.HexToHash("0x100")},
		L1Heads: map[eth.ChainID]eth.BlockID{
			chainID: {Number: 100, Hash: common.HexToHash("0x100")},
		},
		L2Heads: map[eth.ChainID]eth.BlockID{
			chainID: {Number: 1000, Hash: common.HexToHash("0xa")},
		},
	}
	frontier := VerifiedResult{
		Timestamp:   1001,
		L1Inclusion: eth.BlockID{Number: 99, Hash: common.HexToHash("0x99")},
		L1Heads: map[eth.ChainID]eth.BlockID{
			chainID: {Number: 99, Hash: common.HexToHash("0x99")},
		},
		L2Heads: map[eth.ChainID]eth.BlockID{
			chainID: {Number: 1001, Hash: common.HexToHash("0xb")},
		},
	}

	ok, err := checker.FrontierConsistent(context.Background(), accepted, frontier)
	require.NoError(t, err)
	require.False(t, ok)
}
