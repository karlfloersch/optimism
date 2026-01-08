package filter

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_findBlockByTimestamp(t *testing.T) {
	ctx := context.Background()

	t.Run("empty chain returns 1", func(t *testing.T) {
		fetcher := func(ctx context.Context, blockNum uint64) (uint64, error) {
			return 0, errors.New("should not be called")
		}

		result, err := findBlockByTimestamp(ctx, 1000, 0, fetcher)
		require.NoError(t, err)
		require.Equal(t, uint64(1), result)
	})

	t.Run("target before first block returns 1", func(t *testing.T) {
		// Blocks: 1=100, 2=200, 3=300
		fetcher := func(ctx context.Context, blockNum uint64) (uint64, error) {
			return blockNum * 100, nil
		}

		result, err := findBlockByTimestamp(ctx, 50, 3, fetcher)
		require.NoError(t, err)
		require.Equal(t, uint64(1), result)
	})

	t.Run("target after latest block returns latest", func(t *testing.T) {
		// Blocks: 1=100, 2=200, 3=300
		fetcher := func(ctx context.Context, blockNum uint64) (uint64, error) {
			return blockNum * 100, nil
		}

		result, err := findBlockByTimestamp(ctx, 500, 3, fetcher)
		require.NoError(t, err)
		require.Equal(t, uint64(3), result)
	})

	t.Run("finds exact timestamp match", func(t *testing.T) {
		// Blocks: 1=100, 2=200, 3=300, 4=400, 5=500
		fetcher := func(ctx context.Context, blockNum uint64) (uint64, error) {
			return blockNum * 100, nil
		}

		result, err := findBlockByTimestamp(ctx, 300, 5, fetcher)
		require.NoError(t, err)
		require.Equal(t, uint64(3), result)
	})

	t.Run("finds first block after target timestamp", func(t *testing.T) {
		// Blocks: 1=100, 2=200, 3=300, 4=400, 5=500
		// Looking for 250, should return block 3 (timestamp 300)
		fetcher := func(ctx context.Context, blockNum uint64) (uint64, error) {
			return blockNum * 100, nil
		}

		result, err := findBlockByTimestamp(ctx, 250, 5, fetcher)
		require.NoError(t, err)
		require.Equal(t, uint64(3), result)
	})

	t.Run("works with 1-second block times", func(t *testing.T) {
		// Simulate 1-second blocks starting at timestamp 1000
		// Block 1 = 1000, Block 2 = 1001, ..., Block 100 = 1099
		fetcher := func(ctx context.Context, blockNum uint64) (uint64, error) {
			return 999 + blockNum, nil
		}

		// Find block at timestamp 1050
		result, err := findBlockByTimestamp(ctx, 1050, 100, fetcher)
		require.NoError(t, err)
		require.Equal(t, uint64(51), result) // Block 51 has timestamp 1050
	})

	t.Run("works with 2-second block times", func(t *testing.T) {
		// Simulate 2-second blocks starting at timestamp 1000
		// Block 1 = 1000, Block 2 = 1002, Block 3 = 1004, ...
		fetcher := func(ctx context.Context, blockNum uint64) (uint64, error) {
			return 998 + (blockNum * 2), nil
		}

		// Find block at timestamp 1050, should return block 26 (timestamp 1050)
		result, err := findBlockByTimestamp(ctx, 1050, 100, fetcher)
		require.NoError(t, err)
		require.Equal(t, uint64(26), result)
	})

	t.Run("works with irregular block times", func(t *testing.T) {
		// Irregular timestamps (some gaps, some close together)
		timestamps := map[uint64]uint64{
			1: 100,
			2: 105,
			3: 150,
			4: 151,
			5: 200,
			6: 300,
			7: 301,
			8: 302,
		}
		fetcher := func(ctx context.Context, blockNum uint64) (uint64, error) {
			ts, ok := timestamps[blockNum]
			if !ok {
				return 0, errors.New("block not found")
			}
			return ts, nil
		}

		// Find block at timestamp 160, should return block 5 (timestamp 200)
		result, err := findBlockByTimestamp(ctx, 160, 8, fetcher)
		require.NoError(t, err)
		require.Equal(t, uint64(5), result)
	})

	t.Run("works with large chain", func(t *testing.T) {
		// Simulate a chain with 1 million blocks, 1-second block time
		const chainLength = 1_000_000
		callCount := 0

		fetcher := func(ctx context.Context, blockNum uint64) (uint64, error) {
			callCount++
			return blockNum, nil // timestamp = block number for simplicity
		}

		// Find block at timestamp 500000
		result, err := findBlockByTimestamp(ctx, 500000, chainLength, fetcher)
		require.NoError(t, err)
		require.Equal(t, uint64(500000), result)

		// Should take at most log2(1M) + 2 calls ≈ 22 calls
		require.LessOrEqual(t, callCount, 25, "binary search should be efficient")
		t.Logf("Found block in %d RPC calls", callCount)
	})

	t.Run("handles fetch error", func(t *testing.T) {
		fetcher := func(ctx context.Context, blockNum uint64) (uint64, error) {
			if blockNum == 1 {
				return 100, nil
			}
			return 0, errors.New("rpc error")
		}

		_, err := findBlockByTimestamp(ctx, 500, 10, fetcher)
		require.Error(t, err)
		require.Contains(t, err.Error(), "rpc error")
	})

	t.Run("respects context cancellation", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())

		callCount := 0
		fetcher := func(ctx context.Context, blockNum uint64) (uint64, error) {
			callCount++
			if callCount == 3 {
				cancel() // Cancel after a few calls
			}
			return blockNum * 100, nil
		}

		_, err := findBlockByTimestamp(ctx, 5000, 100, fetcher)
		require.Error(t, err)
		require.ErrorIs(t, err, context.Canceled)
	})

	t.Run("single block chain", func(t *testing.T) {
		fetcher := func(ctx context.Context, blockNum uint64) (uint64, error) {
			return 1000, nil
		}

		// Target before the only block
		result, err := findBlockByTimestamp(ctx, 500, 1, fetcher)
		require.NoError(t, err)
		require.Equal(t, uint64(1), result)

		// Target after the only block
		result, err = findBlockByTimestamp(ctx, 1500, 1, fetcher)
		require.NoError(t, err)
		require.Equal(t, uint64(1), result)

		// Target exactly at the only block
		result, err = findBlockByTimestamp(ctx, 1000, 1, fetcher)
		require.NoError(t, err)
		require.Equal(t, uint64(1), result)
	})
}
