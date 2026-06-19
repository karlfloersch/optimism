package interop

import (
	"context"
	"net/http/httptest"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/stretchr/testify/require"

	coreinterop "github.com/ethereum-optimism/optimism/op-core/interop"
	"github.com/ethereum-optimism/optimism/op-core/interop/messages"
	"github.com/ethereum-optimism/optimism/op-service/eth"
	"github.com/ethereum-optimism/optimism/op-supernode/supernode/remote"
	"github.com/ethereum-optimism/optimism/op-supernode/supernode/remote/remotetest"
)

// newRemoteNode wires a deterministic test server to a real HTTPAdapter and the ingester,
// returning the ingester, the fake chain (for ExpectedChecksum), and a LogsDB to seal into.
func newRemoteNode(t *testing.T, cfg remotetest.Config) (*remoteNode, *remotetest.Chain, LogsDB) {
	chain := remotetest.New(cfg)
	srv := httptest.NewServer(chain.Handler())
	t.Cleanup(srv.Close)

	db, err := openLogsDB(testLogger(), cfg.ChainID, t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	adapter := remote.NewHTTPAdapter(cfg.ChainID, srv.URL, srv.Client())
	return &remoteNode{log: testLogger(), adapter: adapter, db: db, poll: defaultRemotePollInterval}, chain, db
}

// TestRemoteNodeIngestAndContains exercises the full path against a real LogsDB: the
// ingester polls the remote node over HTTP, seals the fabricated initiating messages, and
// they become referenceable via the checksum the chain advertises (positive), while a
// wrong checksum is rejected (negative).
func TestRemoteNodeIngestAndContains(t *testing.T) {
	t.Parallel()
	const (
		activation   = uint64(1000)
		blockTime    = uint64(2)
		msgsPerBlock = 2
	)
	chainID := eth.ChainIDFromUInt64(8453)
	node, chain, db := newRemoteNode(t, remotetest.Config{
		ChainID: chainID, BlockTime: blockTime, MsgsPerBlock: msgsPerBlock, StartTimestamp: activation,
	})

	// Ingest two finalized blocks; the LogsDB resume cursor advances each time.
	for n := uint64(1); n <= 2; n++ {
		ingested, err := node.ingestOnce(context.Background())
		require.NoError(t, err)
		require.True(t, ingested)
		latest, ok := db.LatestSealedBlock()
		require.True(t, ok)
		require.Equal(t, n, latest.Number)
	}

	// Every fabricated message is referenceable via its advertised checksum.
	for n := uint64(1); n <= 2; n++ {
		ts := activation + n*blockTime
		for logIdx := uint32(0); logIdx < msgsPerBlock; logIdx++ {
			seal, err := db.Contains(messages.ContainsQuery{
				BlockNum:  n,
				LogIdx:    logIdx,
				Timestamp: ts,
				Checksum:  chain.ExpectedChecksum(n, logIdx),
			})
			require.NoError(t, err, "block %d log %d should be referenceable", n, logIdx)
			require.Equal(t, n, seal.Number)
			require.Equal(t, ts, seal.Timestamp)
		}
	}

	// A wrong checksum at a real (block, log) position is a conflict.
	_, err := db.Contains(messages.ContainsQuery{
		BlockNum:  1,
		LogIdx:    0,
		Timestamp: activation + blockTime,
		Checksum:  messages.MessageChecksum(common.HexToHash("0xbad")),
	})
	require.ErrorIs(t, err, coreinterop.ErrConflict)
}

func TestAddRemoteNode(t *testing.T) {
	// newInteropTestHarness calls t.Parallel() internally.
	h := newInteropTestHarness(t).WithActivation(1000).WithChain(10, nil).Build()
	require.NotNil(t, h.interop)

	remoteID := eth.ChainIDFromUInt64(8453)
	adapter := remote.NewHTTPAdapter(remoteID, "http://example.invalid", nil)

	require.NoError(t, h.interop.AddRemoteNode(adapter))
	require.Contains(t, h.interop.remoteNodes, remoteID)
	require.Contains(t, h.interop.logsDBs, remoteID, "remote logsDB must be registered for executing-message validation to read it")

	// Duplicate remote registration fails.
	require.Error(t, h.interop.AddRemoteNode(adapter))

	// A driven chain cannot also be a remote node.
	require.Error(t, h.interop.AddRemoteNode(remote.NewHTTPAdapter(eth.ChainIDFromUInt64(10), "http://example.invalid", nil)))
}

// TestVerifyExecutingMessageReferencesRemoteNode is the end-to-end check: a driven chain
// references a remote node's initiating message as an executing message, through the real
// verifyExecutingMessage path (including the remote block-time fallback). The remote
// node's messages arrive over HTTP from the test server.
func TestVerifyExecutingMessageReferencesRemoteNode(t *testing.T) {
	// newInteropTestHarness calls t.Parallel() internally.
	const (
		drivenChain = 10
		remoteChain = 8453
		activation  = uint64(1000)
		blockTime   = uint64(2)
	)
	h := newInteropTestHarness(t).WithActivation(activation).WithChain(drivenChain, nil).Build()
	require.NotNil(t, h.interop)

	remoteID := eth.ChainIDFromUInt64(remoteChain)
	chain := remotetest.New(remotetest.Config{
		ChainID: remoteID, BlockTime: blockTime, MsgsPerBlock: 1, StartTimestamp: activation,
	})
	srv := httptest.NewServer(chain.Handler())
	defer srv.Close()
	require.NoError(t, h.interop.AddRemoteNode(remote.NewHTTPAdapter(remoteID, srv.URL, srv.Client())))

	// Ingest one finalized block from the remote node (also populates the adapter's
	// cached block time, used by the activation invariant below).
	node := h.interop.remoteNodes[remoteID]
	require.NotNil(t, node)
	ingested, err := node.ingestOnce(context.Background())
	require.NoError(t, err)
	require.True(t, ingested)

	const initBlock, initLogIdx = uint64(1), uint32(0)
	initTimestamp := activation + blockTime // block 1 timestamp = 1002

	execMsg := &messages.ExecutingMessage{
		ChainID:   remoteID,
		BlockNum:  initBlock,
		LogIdx:    initLogIdx,
		Timestamp: initTimestamp,
		Checksum:  chain.ExpectedChecksum(initBlock, initLogIdx),
	}
	drivenID := eth.ChainIDFromUInt64(drivenChain)

	// Positive: valid executing message referencing the remote node passes.
	require.NoError(t, h.interop.verifyExecutingMessage(drivenID, initTimestamp+50, 0, execMsg, nil),
		"valid executing message referencing a remote node must pass")

	// Negative: a wrong checksum is a conflict.
	bad := *execMsg
	bad.Checksum = messages.MessageChecksum(common.HexToHash("0xdeadbeef"))
	err = h.interop.verifyExecutingMessage(drivenID, initTimestamp+50, 0, &bad, nil)
	require.ErrorIs(t, err, coreinterop.ErrConflict)

	// Negative: referencing a block the remote node has not ingested yet fails.
	future := *execMsg
	future.BlockNum = 99
	future.Timestamp = activation + 99*blockTime
	future.Checksum = chain.ExpectedChecksum(99, 0)
	require.Error(t, h.interop.verifyExecutingMessage(drivenID, future.Timestamp+50, 0, &future, nil),
		"referencing a not-yet-ingested remote block must fail")
}
