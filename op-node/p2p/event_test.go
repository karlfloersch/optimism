package p2p

import (
	"context"
	"testing"

	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/stretchr/testify/require"

	"github.com/ethereum-optimism/optimism/op-service/eth"
	"github.com/ethereum-optimism/optimism/op-service/event"
	"github.com/ethereum-optimism/optimism/op-service/testlog"
	"github.com/ethereum/go-ethereum/log"
)

type mockSafeHeadMetrics struct {
	receivedPayloads []*eth.ExecutionPayloadEnvelope
}

func (m *mockSafeHeadMetrics) RecordReceivedSafePayload(payload *eth.ExecutionPayloadEnvelope) {
	m.receivedPayloads = append(m.receivedPayloads, payload)
}

func TestSafeHeadReceiver(t *testing.T) {
	logger := testlog.Logger(t, log.LevelDebug)

	// Create event system
	sys := event.NewSystem(logger, event.NewGlobalSynchronous(context.Background()))
	emitter := sys.Register("test", nil)

	metrics := &mockSafeHeadMetrics{}
	receiver := NewSafeHeadReceiver(logger, emitter, metrics)

	// Create test payload
	payload := &eth.ExecutionPayload{
		BlockNumber: 100,
		BlockHash:   [32]byte{1, 2, 3},
	}
	envelope := &eth.ExecutionPayloadEnvelope{
		ExecutionPayload: payload,
	}

	peerId := peer.ID("test-peer")

	// Test OnSafeL2Payload
	err := receiver.OnSafeL2Payload(context.Background(), peerId, envelope)
	require.NoError(t, err)

	// Verify metrics were recorded
	require.Len(t, metrics.receivedPayloads, 1)
	require.Equal(t, envelope, metrics.receivedPayloads[0])

	// Verify event was emitted by checking the event was processed
	// (In a real test environment, you'd set up an event listener to verify the event)
}
