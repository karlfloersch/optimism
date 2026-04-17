package interop

import (
	"context"
	"errors"
	"math/big"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ethereum-optimism/optimism/op-service/eth"
	"github.com/ethereum/go-ethereum/common"
	"github.com/stretchr/testify/require"
)

func TestLogBackfillLowerBound(t *testing.T) {
	tests := []struct {
		name           string
		crossSafe, act uint64
		depth          time.Duration
		wantTlo        uint64
	}{
		{"zero depth returns crossSafe", 1000, 100, 0, 1000},
		{"clamp when raw before activation", 200, 100, 500 * time.Second, 100},
		{"raw above activation", 1000, 100, 100 * time.Second, 900},
		{"crossSafe below depth underflow then clamp", 50, 40, 100 * time.Second, 40},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := LogBackfillLowerBound(tt.crossSafe, tt.act, tt.depth)
			require.Equal(t, tt.wantTlo, got)
		})
	}
}

// progressInteropUntil calls progressAndRecord up to maxIters times until cond() is true.
func progressInteropUntil(t *testing.T, i *Interop, maxIters int, cond func() bool) {
	t.Helper()
	for range maxIters {
		if cond() {
			return
		}
		_, err := i.progressAndRecord()
		require.NoError(t, err)
	}
}

func TestLogBackfill_ResumesAfterInterruption(t *testing.T) {
	const act = uint64(100)
	depth := 10 * time.Second // crossSafe 110, depth 10s -> T_lo 100; should seal 100..110

	h := newInteropTestHarness(t).
		WithActivation(act).
		WithLogBackfillDepth(depth).
		WithChain(10, func(m *mockChainContainer) {
			m.currentL1 = eth.BlockRef{Number: 1, Hash: common.HexToHash("0xL1")}
			m.syncStatusFull = &eth.SyncStatus{
				CurrentL1:   eth.L1BlockRef{Number: 1, Hash: common.HexToHash("0xL1")},
				UnsafeL2:    eth.L2BlockRef{Number: 110, Time: 110},
				SafeL2:      eth.L2BlockRef{Number: 110, Time: 110},
				LocalSafeL2: eth.L2BlockRef{Number: 110, Time: 110},
			}
		}).
		Build()
	h.interop.ctx = context.Background()

	// Simulate a previous partial run: seal blocks 100..105 into the logsDB.
	chain10 := h.Mock(10)
	for num := uint64(100); num <= 105; num++ {
		out, err := chain10.OutputV0AtBlockNumber(context.Background(), num)
		require.NoError(t, err)
		bid := eth.BlockID{Hash: out.BlockHash, Number: num}
		blockInfo, receipts, err := chain10.FetchReceipts(context.Background(), bid)
		require.NoError(t, err)
		err = h.interop.sealBlockDataIntoLogsDB(chain10.id, bid, blockInfo, receipts, blockInfo.Time(), true)
		require.NoError(t, err)
	}

	latest, has := h.interop.logsDBs[chain10.id].LatestSealedBlock()
	require.True(t, has)
	require.Equal(t, uint64(105), latest.Number)

	// Track how many OutputV0 calls happen during backfill to confirm we
	// don't re-fetch blocks 100..105.
	var fetchCount atomic.Int32
	chain10.outputV0Override = func(ctx context.Context, num uint64) (*eth.OutputV0, error) {
		fetchCount.Add(1)
		return &eth.OutputV0{
			StateRoot:                eth.Bytes32(common.HexToHash("0xmockstate")),
			MessagePasserStorageRoot: eth.Bytes32(common.HexToHash("0xmockmsg")),
			BlockHash:                common.BigToHash(new(big.Int).SetUint64(num)),
		}, nil
	}

	require.NoError(t, h.interop.runLogBackfill())
	require.Equal(t, uint64(111), h.interop.runtimeActivationTimestamp)
	require.Equal(t, act, h.interop.activationTimestamp, "protocol activation must not change")

	latest, has = h.interop.logsDBs[chain10.id].LatestSealedBlock()
	require.True(t, has)
	require.Equal(t, uint64(110), latest.Number)

	// Should have fetched only blocks 106..110 (5 blocks), not 100..110 (11 blocks).
	require.Equal(t, int32(5), fetchCount.Load())
}

func TestLogBackfill_RetriesWhenVirtualNodesNotReady(t *testing.T) {
	const act = uint64(100)
	depth := 10 * time.Second

	// Track SyncStatus call count so we can make the first N calls fail.
	var syncStatusCalls atomic.Int32
	failUntil := int32(3) // first 3 calls return error, then succeed

	h := newInteropTestHarness(t).
		WithActivation(act).
		WithLogBackfillDepth(depth).
		WithChain(10, func(m *mockChainContainer) {
			m.currentL1 = eth.BlockRef{Number: 1, Hash: common.HexToHash("0xL1")}
			m.syncStatusFull = &eth.SyncStatus{
				CurrentL1:   eth.L1BlockRef{Number: 1, Hash: common.HexToHash("0xL1")},
				UnsafeL2:    eth.L2BlockRef{Number: 110, Time: 110},
				SafeL2:      eth.L2BlockRef{Number: 110, Time: 110},
				LocalSafeL2: eth.L2BlockRef{Number: 110, Time: 110},
			}
			m.currentL1Err = errors.New("virtual node not ready")
			m.syncStatusOverride = func() (*eth.SyncStatus, error) {
				n := syncStatusCalls.Add(1)
				if n <= failUntil {
					return nil, errors.New("virtual node not ready")
				}
				return &eth.SyncStatus{
					CurrentL1:   eth.L1BlockRef{Number: 1, Hash: common.HexToHash("0xL1")},
					UnsafeL2:    eth.L2BlockRef{Number: 110, Time: 110},
					SafeL2:      eth.L2BlockRef{Number: 110, Time: 110},
					LocalSafeL2: eth.L2BlockRef{Number: 110, Time: 110},
				}, nil
			}
		}).
		Build()

	// Use a shorter backoff for tests.
	origBackoff := errorBackoffPeriod
	errorBackoffPeriod = 10 * time.Millisecond
	t.Cleanup(func() { errorBackoffPeriod = origBackoff })

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	go func() { done <- h.interop.Start(ctx) }()

	// Wait for backfill to complete: runtime activation should advance past 110.
	require.Eventually(t, func() bool {
		return h.interop.runtimeActivationTimestamp > act
	}, 5*time.Second, 20*time.Millisecond, "backfill should eventually succeed after retries")

	require.GreaterOrEqual(t, syncStatusCalls.Load(), failUntil,
		"SyncStatus should have been called at least %d times (the failing ones)", failUntil)
	require.Equal(t, uint64(111), h.interop.runtimeActivationTimestamp)
	require.Equal(t, act, h.interop.activationTimestamp, "protocol activation must not change")

	cancel()
	<-done
}

func TestLogBackfill_RetriesStopOnContextCancel(t *testing.T) {
	const act = uint64(100)
	depth := 10 * time.Second

	h := newInteropTestHarness(t).
		WithActivation(act).
		WithLogBackfillDepth(depth).
		WithChain(10, func(m *mockChainContainer) {
			// SyncStatus always fails — backfill will retry forever.
			m.currentL1Err = errors.New("virtual node not ready")
		}).
		Build()

	origBackoff := errorBackoffPeriod
	errorBackoffPeriod = 10 * time.Millisecond
	t.Cleanup(func() { errorBackoffPeriod = origBackoff })

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan error, 1)
	go func() { done <- h.interop.Start(ctx) }()

	// Let it retry a few times, then cancel.
	time.Sleep(100 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(5 * time.Second):
		t.Fatal("Start did not return after context cancellation")
	}
}

func TestLogBackfill_AdvancesActivationAndStartsVerifyAfterCeiling(t *testing.T) {
	const act = uint64(108)
	depth := time.Second // crossSafe 110, depth 1s -> T_lo 109; seals 109..110; activation advances to 111

	h := newInteropTestHarness(t).
		WithActivation(act).
		WithLogBackfillDepth(depth).
		WithChain(10, func(m *mockChainContainer) {
			m.currentL1 = eth.BlockRef{Number: 1, Hash: common.HexToHash("0xL1")}
			m.syncStatusFull = &eth.SyncStatus{
				CurrentL1:   eth.L1BlockRef{Number: 1, Hash: common.HexToHash("0xL1")},
				UnsafeL2:    eth.L2BlockRef{Number: 110, Time: 110},
				SafeL2:      eth.L2BlockRef{Number: 110, Time: 110},
				LocalSafeL2: eth.L2BlockRef{Number: 110, Time: 110},
			}
		}).
		Build()

	var verifyCalls atomic.Int32
	var firstVerifyTS atomic.Uint64
	h.interop.verifyFn = func(ts uint64, blocks map[eth.ChainID]eth.BlockID) (Result, error) {
		if verifyCalls.Add(1) == 1 {
			firstVerifyTS.Store(ts)
		}
		return Result{
			Timestamp:   ts,
			L1Inclusion: eth.BlockID{Number: 1, Hash: common.HexToHash("0xL1")},
			L2Heads:     blocks,
		}, nil
	}
	h.interop.ctx = context.Background()

	require.NoError(t, h.interop.runLogBackfill())
	require.Equal(t, uint64(111), h.interop.runtimeActivationTimestamp)
	require.Equal(t, act, h.interop.activationTimestamp, "protocol activation must not change")

	chain10 := h.Mock(10)
	latest, has := h.interop.logsDBs[chain10.id].LatestSealedBlock()
	require.True(t, has)
	require.Equal(t, uint64(110), latest.Number)
	require.Zero(t, verifyCalls.Load())

	// Progress the main loop — first verify should be at 111 (activation after backfill).
	progressInteropUntil(t, h.interop, 10, func() bool {
		lastTS, ok := h.interop.verifiedDB.LastTimestamp()
		return ok && lastTS >= 111
	})
	lastTS, ok := h.interop.verifiedDB.LastTimestamp()
	require.True(t, ok)
	require.GreaterOrEqual(t, lastTS, uint64(111))
	require.Equal(t, int32(1), verifyCalls.Load())
	require.Equal(t, uint64(111), firstVerifyTS.Load())
}
