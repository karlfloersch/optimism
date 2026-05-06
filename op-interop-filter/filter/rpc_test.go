package filter

import (
	"context"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/rpc"
	"github.com/stretchr/testify/require"

	"github.com/ethereum-optimism/optimism/op-interop-filter/metrics"
	"github.com/ethereum-optimism/optimism/op-service/eth"
	oprpc "github.com/ethereum-optimism/optimism/op-service/rpc"
	"github.com/ethereum-optimism/optimism/op-service/testlog"
)

func TestQueryFrontendGetBlockHashByNumberRPC(t *testing.T) {
	logger := testlog.Logger(t, log.LevelInfo)
	mock := newMockChainIngester()
	mock.AddBlock(eth.BlockID{Hash: common.HexToHash("0x01"), Number: 100})
	mock.AddBlock(eth.BlockID{Hash: common.HexToHash("0x02"), Number: 200})

	backend := NewBackend(context.Background(), BackendParams{
		Logger:         logger,
		Metrics:        metrics.NoopMetrics,
		Chains:         map[eth.ChainID]ChainIngester{eth.ChainIDFromUInt64(testChainA): mock},
		CrossValidator: &mockCrossValidator{},
	})

	server := oprpc.NewServer(
		"127.0.0.1",
		0,
		"test",
		oprpc.WithLogger(logger),
	)
	server.AddAPI(rpc.API{
		Namespace: "supervisor",
		Service:   &QueryFrontend{backend: backend},
	})

	require.NoError(t, server.Start())
	t.Cleanup(func() {
		_ = server.Stop()
	})

	client, err := rpc.Dial("http://" + server.Endpoint())
	require.NoError(t, err)
	t.Cleanup(client.Close)

	t.Run("latest selector", func(t *testing.T) {
		var result common.Hash
		err := client.Call(&result, "supervisor_getBlockHashByNumber", eth.ChainIDFromUInt64(testChainA), "latest")
		require.NoError(t, err)
		require.Equal(t, common.HexToHash("0x02"), result)
	})

	t.Run("numeric selector", func(t *testing.T) {
		var result common.Hash
		err := client.Call(&result, "supervisor_getBlockHashByNumber", eth.ChainIDFromUInt64(testChainA), rpc.BlockNumber(100))
		require.NoError(t, err)
		require.Equal(t, common.HexToHash("0x01"), result)
	})

	t.Run("missing block", func(t *testing.T) {
		var result common.Hash
		err := client.Call(&result, "supervisor_getBlockHashByNumber", eth.ChainIDFromUInt64(testChainA), rpc.BlockNumber(999))
		require.ErrorContains(t, err, "not found")
	})

	t.Run("unknown chain", func(t *testing.T) {
		var result common.Hash
		err := client.Call(&result, "supervisor_getBlockHashByNumber", eth.ChainIDFromUInt64(999), rpc.BlockNumber(100))
		require.ErrorContains(t, err, "unknown chain")
	})

	t.Run("unsupported tag", func(t *testing.T) {
		var result common.Hash
		err := client.Call(&result, "supervisor_getBlockHashByNumber", eth.ChainIDFromUInt64(testChainA), "safe")
		require.ErrorContains(t, err, "unsupported block tag")
	})
}

func TestQueryFrontendGetLatestCrossUnsafeBlockRPC(t *testing.T) {
	logger := testlog.Logger(t, log.LevelInfo)
	chainID := eth.ChainIDFromUInt64(testChainA)
	mock := newMockChainIngester()
	mock.AddBlockWithTimestamp(eth.BlockID{Hash: common.HexToHash("0x01"), Number: 100}, 1000)
	mock.AddBlockWithTimestamp(eth.BlockID{Hash: common.HexToHash("0x02"), Number: 101}, 1002)
	mock.AddBlockWithTimestamp(eth.BlockID{Hash: common.HexToHash("0x03"), Number: 102}, 1004)

	crossValidator := &mockCrossValidator{}
	crossValidator.SetCrossValidatedTimestamp(1003)

	backend := NewBackend(context.Background(), BackendParams{
		Logger:         logger,
		Metrics:        metrics.NoopMetrics,
		Chains:         map[eth.ChainID]ChainIngester{chainID: mock},
		CrossValidator: crossValidator,
	})

	server := oprpc.NewServer(
		"127.0.0.1",
		0,
		"test",
		oprpc.WithLogger(logger),
	)
	server.AddAPI(rpc.API{
		Namespace: "supervisor",
		Service:   &QueryFrontend{backend: backend},
	})

	require.NoError(t, server.Start())
	t.Cleanup(func() {
		_ = server.Stop()
	})

	client, err := rpc.Dial("http://" + server.Endpoint())
	require.NoError(t, err)
	t.Cleanup(client.Close)

	t.Run("returns latest block at or before cross-unsafe timestamp", func(t *testing.T) {
		var result eth.BlockID
		err := client.Call(&result, "supervisor_getLatestCrossUnsafeBlock", chainID)
		require.NoError(t, err)
		require.Equal(t, eth.BlockID{Hash: common.HexToHash("0x02"), Number: 101}, result)
	})

	t.Run("unknown chain", func(t *testing.T) {
		var result eth.BlockID
		err := client.Call(&result, "supervisor_getLatestCrossUnsafeBlock", eth.ChainIDFromUInt64(999))
		require.ErrorContains(t, err, "unknown chain")
	})

	t.Run("cross-unsafe timestamp unavailable", func(t *testing.T) {
		backend.crossValidator = &mockCrossValidator{}
		var result eth.BlockID
		err := client.Call(&result, "supervisor_getLatestCrossUnsafeBlock", chainID)
		require.ErrorContains(t, err, "out of scope")
	})
}
