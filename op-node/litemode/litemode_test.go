package litemode

import (
	"context"
	"testing"

	"github.com/ethereum-optimism/optimism/op-service/eth"
	opsigner "github.com/ethereum-optimism/optimism/op-service/signer"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/log"
	"github.com/stretchr/testify/require"
)

type fakeEngine struct {
	safe      eth.L2BlockRef
	localSafe eth.L2BlockRef
	fin       eth.L2BlockRef
	committed []uint64
	l2        *fakeL2
}

func (f *fakeEngine) UnsafeL2Head() eth.L2BlockRef        { return eth.L2BlockRef{} }
func (f *fakeEngine) SafeL2Head() eth.L2BlockRef          { return f.safe }
func (f *fakeEngine) Finalized() eth.L2BlockRef           { return f.fin }
func (f *fakeEngine) SetUnsafeHead(r eth.L2BlockRef)      {}
func (f *fakeEngine) SetSafeHead(r eth.L2BlockRef)        { f.safe = r }
func (f *fakeEngine) SetLocalSafeHead(r eth.L2BlockRef)   { f.localSafe = r }
func (f *fakeEngine) SetFinalizedHead(r eth.L2BlockRef)   { f.fin = r }
func (f *fakeEngine) SetCrossUnsafeHead(r eth.L2BlockRef) {}
func (f *fakeEngine) TryUpdateEngine(ctx context.Context) {}
func (f *fakeEngine) CommitBlock(ctx context.Context, signed *opsigner.SignedExecutionPayloadEnvelope) error {
	if signed != nil && signed.Envelope != nil && signed.Envelope.ExecutionPayload != nil {
		f.committed = append(f.committed, uint64(signed.Envelope.ExecutionPayload.BlockNumber))
		if f.l2 != nil {
			f.l2.markPresent(signed.Envelope.ExecutionPayload.BlockHash)
		}
	}
	return nil
}

type fakeL2 struct{ present map[common.Hash]bool }

func (f *fakeL2) L2BlockRefByHash(ctx context.Context, h common.Hash) (eth.L2BlockRef, error) {
	if f.present != nil && f.present[h] {
		return eth.L2BlockRef{Hash: h}, nil
	}
	return eth.L2BlockRef{}, context.DeadlineExceeded
}

func (f *fakeL2) L2BlockRefByNumber(ctx context.Context, num uint64) (eth.L2BlockRef, error) {
	// Create a synthetic hash from the number for tests that call by number.
	// Tests that require specific hashes will ensure presence via markPresent.
	b := []byte{byte(num)}
	h := common.BytesToHash(b)
	if f.present != nil && f.present[h] {
		return eth.L2BlockRef{Hash: h, Number: num}, nil
	}
	return eth.L2BlockRef{}, context.DeadlineExceeded
}

func (f *fakeL2) markPresent(h common.Hash) {
	if f.present == nil {
		f.present = make(map[common.Hash]bool)
	}
	f.present[h] = true
}

func TestApplyFinalizedAndSafe(t *testing.T) {
	ctx := context.Background()
	l2 := &fakeL2{}
	eng := &fakeEngine{l2: l2}
	c := New(Config{RPC: "", Interval: 0}, log.New(), eng, l2)

	fin := eth.L2BlockRef{Hash: common.HexToHash("0x01"), Number: 10}
	l2.markPresent(fin.Hash)
	c.applyFinalized(ctx, fin)
	require.Equal(t, uint64(10), eng.Finalized().Number)

	safe := eth.L2BlockRef{Hash: common.HexToHash("0x02"), Number: 12}
	l2.markPresent(safe.Hash)
	c.applySafe(ctx, safe)
	require.Equal(t, uint64(12), eng.SafeL2Head().Number)
}

type fakeFetcher struct {
	safeNum uint64
	finNum  uint64
	blocks  map[uint64]eth.L2BlockRef
}

func (f *fakeFetcher) SafeHeadNumber(ctx context.Context) (uint64, bool) {
	return f.safeNum, f.safeNum != 0
}
func (f *fakeFetcher) FinalizedHeadNumber(ctx context.Context) (uint64, bool) {
	return f.finNum, f.finNum != 0
}
func (f *fakeFetcher) BlockByNumber(ctx context.Context, num uint64) (eth.L2BlockRef, bool) {
	b, ok := f.blocks[num]
	return b, ok
}

func mkRef(num int, hashHex string, parentHex string) eth.L2BlockRef {
	return eth.L2BlockRef{
		Hash:       common.HexToHash(hashHex),
		Number:     uint64(num),
		ParentHash: common.HexToHash(parentHex),
	}
}

func TestAdvanceSafeStraightLine(t *testing.T) {
	ctx := context.Background()
	l2 := &fakeL2{}
	eng := &fakeEngine{l2: l2}
	c := New(Config{RPC: "", Interval: 0}, log.New(), eng, l2)

	// local safe at 0
	h0 := common.HexToHash("0x00")
	eng.safe = eth.L2BlockRef{Hash: h0, Number: 0}

	ff := &fakeFetcher{
		safeNum: 3,
		blocks: map[uint64]eth.L2BlockRef{
			1: mkRef(1, "0x01", "0x00"),
			2: mkRef(2, "0x02", "0x01"),
			3: mkRef(3, "0x03", "0x02"),
		},
	}
	c.fetch = ff
	// Inject a payload builder that returns envelopes with matching numbers
	c.buildEnvelopeFn = func(ctx context.Context, num uint64) (*eth.ExecutionPayloadEnvelope, bool) {
		h := common.HexToHash("0x" + common.Bytes2Hex([]byte{byte(num)}))
		return &eth.ExecutionPayloadEnvelope{ExecutionPayload: &eth.ExecutionPayload{BlockNumber: eth.Uint64Quantity(num), BlockHash: h}}, true
	}

	c.advanceSafeTo(ctx, 3)
	require.Equal(t, uint64(3), eng.safe.Number)
	require.Equal(t, uint64(3), eng.localSafe.Number)
	require.Equal(t, []uint64{1, 2, 3}, eng.committed)
}

func TestAdvanceSafeStopsOnMismatch(t *testing.T) {
	ctx := context.Background()
	eng := &fakeEngine{}
	l2 := &fakeL2{}
	c := New(Config{RPC: "", Interval: 0}, log.New(), eng, l2)

	// local safe at 1
	h1 := common.HexToHash("0x11")
	eng.safe = eth.L2BlockRef{Hash: h1, Number: 1}

	// external chain 2 does not build on 1
	ff := &fakeFetcher{
		safeNum: 3,
		blocks: map[uint64]eth.L2BlockRef{
			2: mkRef(2, "0x22", "0xaa"),
			3: mkRef(3, "0x33", "0x22"),
		},
	}
	c.fetch = ff

	c.advanceSafeTo(ctx, 3)
	// Should not move since cannot connect
	require.Equal(t, uint64(1), eng.safe.Number)
}

func TestAdvanceSafeStopsOnMissingBlock(t *testing.T) {
	ctx := context.Background()
	l2 := &fakeL2{}
	eng := &fakeEngine{l2: l2}
	c := New(Config{RPC: "", Interval: 0}, log.New(), eng, l2)

	// local safe at 0
	h0 := common.HexToHash("0x00")
	eng.safe = eth.L2BlockRef{Hash: h0, Number: 0}

	// missing block 1
	ff := &fakeFetcher{
		safeNum: 2,
		blocks: map[uint64]eth.L2BlockRef{
			2: mkRef(2, "0x02", "0x01"),
		},
	}
	c.fetch = ff
	c.buildEnvelopeFn = func(ctx context.Context, num uint64) (*eth.ExecutionPayloadEnvelope, bool) {
		if num == 1 {
			h := common.HexToHash("0x" + common.Bytes2Hex([]byte{byte(num)}))
			return &eth.ExecutionPayloadEnvelope{ExecutionPayload: &eth.ExecutionPayload{BlockNumber: eth.Uint64Quantity(num), BlockHash: h}}, true
		}
		return nil, false
	}

	c.advanceSafeTo(ctx, 2)
	// Should remain at 0
	require.Equal(t, uint64(0), eng.safe.Number)
}

func TestAdvanceSafeReorgWithBacktrackCommit(t *testing.T) {
	ctx := context.Background()
	l2 := &fakeL2{}
	eng := &fakeEngine{l2: l2}
	c := New(Config{RPC: "", Interval: 0}, log.New(), eng, l2)

	// local safe at 5
	h5 := common.HexToHash("0x05")
	eng.safe = eth.L2BlockRef{Hash: h5, Number: 5}

	// external reorg: 6a (parent 5), 7a (parent 6a)
	ff := &fakeFetcher{
		safeNum: 7,
		blocks: map[uint64]eth.L2BlockRef{
			6: mkRef(6, "0x06a", "0x05"),
			7: mkRef(7, "0x07a", "0x06a"),
		},
	}
	c.fetch = ff
	c.buildEnvelopeFn = func(ctx context.Context, num uint64) (*eth.ExecutionPayloadEnvelope, bool) {
		switch num {
		case 6:
			return &eth.ExecutionPayloadEnvelope{ExecutionPayload: &eth.ExecutionPayload{BlockNumber: eth.Uint64Quantity(6), BlockHash: common.HexToHash("0x06a")}}, true
		case 7:
			return &eth.ExecutionPayloadEnvelope{ExecutionPayload: &eth.ExecutionPayload{BlockNumber: eth.Uint64Quantity(7), BlockHash: common.HexToHash("0x07a")}}, true
		default:
			return nil, false
		}
	}

	c.advanceSafeTo(ctx, 7)
	require.Equal(t, uint64(7), eng.safe.Number)
	require.Equal(t, []uint64{6, 7}, eng.committed)
}

// no-op: using geth log.New() which is a valid Logger implementation
