package filter

import (
	"context"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/log"
	"github.com/stretchr/testify/require"

	"github.com/ethereum-optimism/optimism/op-interop-filter/metrics"
	"github.com/ethereum-optimism/optimism/op-service/eth"
	suptypes "github.com/ethereum-optimism/optimism/op-supervisor/supervisor/types"
)

func TestBackend_FailsafeEnabled(t *testing.T) {
	logger := log.New()
	m := metrics.NoopMetrics
	cfg := &Config{}

	b := &Backend{
		log:     logger,
		metrics: m,
		cfg:     cfg,
		chains:  make(map[eth.ChainID]*Chain),
	}

	// Initially failsafe should be disabled
	require.False(t, b.FailsafeEnabled())

	// Simulate reorg detection
	chainID := eth.ChainIDFromUInt64(10)
	b.onReorg(chainID, nil)

	// Now failsafe should be enabled
	require.True(t, b.FailsafeEnabled())
}

func TestBackend_CheckAccessList_FailsafeEnabled(t *testing.T) {
	logger := log.New()
	m := metrics.NoopMetrics
	cfg := &Config{}

	b := &Backend{
		log:     logger,
		metrics: m,
		cfg:     cfg,
		chains:  make(map[eth.ChainID]*Chain),
	}

	// Enable failsafe
	b.failsafe.Store(true)

	// CheckAccessList should return ErrFailsafeEnabled
	err := b.CheckAccessList(context.Background(), nil, suptypes.LocalUnsafe, suptypes.ExecutingDescriptor{})
	require.ErrorIs(t, err, ErrFailsafeEnabled)
}

func TestBackend_CheckAccessList_NotReady(t *testing.T) {
	logger := log.New()
	m := metrics.NoopMetrics
	cfg := &Config{}

	chainID := eth.ChainIDFromUInt64(10)
	chain := &Chain{
		log:     logger,
		chainID: chainID,
	}
	// Chain is not ready (ready is false by default)

	b := &Backend{
		log:     logger,
		metrics: m,
		cfg:     cfg,
		chains:  map[eth.ChainID]*Chain{chainID: chain},
	}

	// CheckAccessList should return ErrNotReady when chain is not ready
	err := b.CheckAccessList(context.Background(), nil, suptypes.LocalUnsafe, suptypes.ExecutingDescriptor{})
	require.ErrorIs(t, err, ErrNotReady)
}

func TestBackend_CheckAccessList_UnknownChain(t *testing.T) {
	logger := log.New()
	m := metrics.NoopMetrics
	cfg := &Config{}

	// Create a ready chain for chainID 10
	chainID := eth.ChainIDFromUInt64(10)
	chain := &Chain{
		log:     logger,
		chainID: chainID,
	}
	chain.ready.Store(true)

	b := &Backend{
		log:     logger,
		metrics: m,
		cfg:     cfg,
		chains:  map[eth.ChainID]*Chain{chainID: chain},
	}

	// Create access entries for an unknown chain (chainID 999)
	// Using raw encoding for a simple access entry
	unknownChainAccess := createMockAccessEntry(999, 100, 0, 12345, common.Hash{})

	err := b.CheckAccessList(context.Background(), unknownChainAccess, suptypes.LocalUnsafe, suptypes.ExecutingDescriptor{})
	require.ErrorIs(t, err, ErrUnknownChain)
}

func TestBackend_Ready(t *testing.T) {
	logger := log.New()
	m := metrics.NoopMetrics
	cfg := &Config{}

	chain1 := &Chain{log: logger, chainID: eth.ChainIDFromUInt64(10)}
	chain2 := &Chain{log: logger, chainID: eth.ChainIDFromUInt64(8453)}

	b := &Backend{
		log:     logger,
		metrics: m,
		cfg:     cfg,
		chains: map[eth.ChainID]*Chain{
			eth.ChainIDFromUInt64(10):   chain1,
			eth.ChainIDFromUInt64(8453): chain2,
		},
	}

	// No chains ready
	require.False(t, b.Ready())

	// One chain ready
	chain1.ready.Store(true)
	require.False(t, b.Ready())

	// Both chains ready
	chain2.ready.Store(true)
	require.True(t, b.Ready())
}

// createMockAccessEntry creates a mock access entry for testing
// This follows the encoding format from supervisor types:
// Hash 0: byte 0 = PrefixLookup (1), bytes 1-3 = zeros, bytes 4-12 = chainID,
//         bytes 12-20 = blockNum, bytes 20-28 = timestamp, bytes 28-32 = logIdx
// Hash 1: byte 0 = PrefixChecksum (3), rest = checksum data
func createMockAccessEntry(chainID uint64, blockNum uint64, logIdx uint32, timestamp uint64, checksum common.Hash) []common.Hash {
	const (
		PrefixLookup   = 1
		PrefixChecksum = 3
	)

	var entries []common.Hash

	// First hash: lookup entry
	var lookup common.Hash
	lookup[0] = PrefixLookup
	// bytes 1-3 are zero padding (already zero)
	// bytes 4-12: chainID (big endian)
	lookup[4] = byte(chainID >> 56)
	lookup[5] = byte(chainID >> 48)
	lookup[6] = byte(chainID >> 40)
	lookup[7] = byte(chainID >> 32)
	lookup[8] = byte(chainID >> 24)
	lookup[9] = byte(chainID >> 16)
	lookup[10] = byte(chainID >> 8)
	lookup[11] = byte(chainID)
	// bytes 12-20: blockNum (big endian)
	lookup[12] = byte(blockNum >> 56)
	lookup[13] = byte(blockNum >> 48)
	lookup[14] = byte(blockNum >> 40)
	lookup[15] = byte(blockNum >> 32)
	lookup[16] = byte(blockNum >> 24)
	lookup[17] = byte(blockNum >> 16)
	lookup[18] = byte(blockNum >> 8)
	lookup[19] = byte(blockNum)
	// bytes 20-28: timestamp (big endian)
	lookup[20] = byte(timestamp >> 56)
	lookup[21] = byte(timestamp >> 48)
	lookup[22] = byte(timestamp >> 40)
	lookup[23] = byte(timestamp >> 32)
	lookup[24] = byte(timestamp >> 24)
	lookup[25] = byte(timestamp >> 16)
	lookup[26] = byte(timestamp >> 8)
	lookup[27] = byte(timestamp)
	// bytes 28-32: logIdx (big endian uint32)
	lookup[28] = byte(logIdx >> 24)
	lookup[29] = byte(logIdx >> 16)
	lookup[30] = byte(logIdx >> 8)
	lookup[31] = byte(logIdx)
	entries = append(entries, lookup)

	// Second hash: checksum entry
	var checksumEntry common.Hash
	checksumEntry[0] = PrefixChecksum
	// Copy checksum data to bytes 1-31
	copy(checksumEntry[1:], checksum[1:])
	entries = append(entries, checksumEntry)

	return entries
}
