package engine

import (
	"context"
	"math/big"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/log"

	"github.com/ethereum-optimism/optimism/op-node/p2p"
	"github.com/ethereum-optimism/optimism/op-node/rollup"
	"github.com/ethereum-optimism/optimism/op-service/eth"
	"github.com/ethereum-optimism/optimism/op-service/event"
	"github.com/ethereum-optimism/optimism/op-service/testlog"
)

type mockL2Chain struct {
	mock.Mock
}

func (m *mockL2Chain) PayloadByNumber(ctx context.Context, number uint64) (*eth.ExecutionPayloadEnvelope, error) {
	args := m.Called(ctx, number)
	return args.Get(0).(*eth.ExecutionPayloadEnvelope), args.Error(1)
}

type mockEngineController struct {
	mock.Mock
	safeHead eth.L2BlockRef
}

func (m *mockEngineController) SafeL2Head() eth.L2BlockRef {
	return m.safeHead
}

func (m *mockEngineController) SetSafeHead(ref eth.L2BlockRef) {
	m.safeHead = ref
}

func (m *mockEngineController) LocalSafeL2Head() eth.L2BlockRef {
	return m.safeHead // For simplicity, same as safe head in testing
}

func (m *mockEngineController) UnsafeL2Head() eth.L2BlockRef {
	args := m.Called()
	return args.Get(0).(eth.L2BlockRef)
}

type mockMetrics struct {
	mock.Mock
}

func (m *mockMetrics) RecordL2Ref(name string, ref eth.L2BlockRef) {
	m.Called(name, ref)
}

func TestEngDeriver_ProverModeEmitsSafeHeadGossip(t *testing.T) {
	logger := testlog.Logger(t, log.LevelDebug)
	cfg := &rollup.Config{
		L2ChainID: big.NewInt(100),
	}

	mockL2 := &mockL2Chain{}
	mockEC := &mockEngineController{}
	mockMetrics := &mockMetrics{}

	// Create test payload
	testPayload := &eth.ExecutionPayloadEnvelope{
		ExecutionPayload: &eth.ExecutionPayload{
			BlockNumber: 42,
			BlockHash:   common.HexToHash("0x1234"),
		},
	}

	// Setup mocks
	mockL2.On("PayloadByNumber", mock.Anything, uint64(42)).Return(testPayload, nil)
	mockEC.On("UnsafeL2Head").Return(eth.L2BlockRef{})
	mockMetrics.On("RecordL2Ref", mock.Anything, mock.Anything).Return()

	// Create EngDeriver in prover mode
	ctx := context.Background()
	deriver := NewEngDeriver(logger, ctx, cfg, mockMetrics, mockEC, mockL2, "prover")

	// Setup event system
	sys := event.NewSystem(logger, event.NewGlobalSynchronous(ctx))
	emitter := sys.Register("test-emitter", nil)
	deriver.AttachEmitter(emitter)

	// Event capture
	var capturedEvents []event.Event
	eventCapture := event.DeriverFunc(func(ctx context.Context, ev event.Event) bool {
		capturedEvents = append(capturedEvents, ev)
		return true
	})
	sys.Register("event-capture", eventCapture)

	// Create a PromoteSafeEvent
	safeRef := eth.L2BlockRef{
		Number:     42,
		Hash:       common.HexToHash("0x1234"),
		ParentHash: common.HexToHash("0x0000"),
	}

	promoteEvent := PromoteSafeEvent{
		Ref:    safeRef,
		Source: eth.L1BlockRef{}, // Minimal test data
	}

	// Process the event
	handled := deriver.OnEvent(ctx, promoteEvent)
	require.True(t, handled, "PromoteSafeEvent should be handled")

	// Verify that GossipSafeHeadEvent was emitted
	require.Len(t, capturedEvents, 3, "Should emit SafeDerivedEvent, CrossSafeUpdateEvent, and GossipSafeHeadEvent")

	// Find the GossipSafeHeadEvent
	var gossipEvent *p2p.GossipSafeHeadEvent
	for _, ev := range capturedEvents {
		if ge, ok := ev.(p2p.GossipSafeHeadEvent); ok {
			gossipEvent = &ge
			break
		}
	}

	require.NotNil(t, gossipEvent, "GossipSafeHeadEvent should be emitted")
	require.Equal(t, testPayload, gossipEvent.Envelope, "Gossiped payload should match")
	require.Equal(t, uint64(42), uint64(gossipEvent.Envelope.ExecutionPayload.BlockNumber), "Block number should match")

	// Verify mocks
	mockL2.AssertExpectations(t)
	mockEC.AssertExpectations(t)
}

func TestEngDeriver_NormalModeDoesNotEmitSafeHeadGossip(t *testing.T) {
	logger := testlog.Logger(t, log.LevelDebug)
	cfg := &rollup.Config{
		L2ChainID: big.NewInt(100),
	}

	mockL2 := &mockL2Chain{}
	mockEC := &mockEngineController{}
	mockMetrics := &mockMetrics{}

	// Setup mocks - PayloadByNumber should NOT be called in normal mode
	mockEC.On("UnsafeL2Head").Return(eth.L2BlockRef{})
	mockMetrics.On("RecordL2Ref", mock.Anything, mock.Anything).Return()

	// Create EngDeriver in normal mode
	ctx := context.Background()
	deriver := NewEngDeriver(logger, ctx, cfg, mockMetrics, mockEC, mockL2, "normal")

	// Setup event system
	sys := event.NewSystem(logger, event.NewGlobalSynchronous(ctx))
	emitter := sys.Register("test-emitter", nil)
	deriver.AttachEmitter(emitter)

	// Event capture
	var capturedEvents []event.Event
	eventCapture := event.DeriverFunc(func(ctx context.Context, ev event.Event) bool {
		capturedEvents = append(capturedEvents, ev)
		return true
	})
	sys.Register("event-capture", eventCapture)

	// Create a PromoteSafeEvent
	safeRef := eth.L2BlockRef{
		Number:     42,
		Hash:       common.HexToHash("0x1234"),
		ParentHash: common.HexToHash("0x0000"),
	}

	promoteEvent := PromoteSafeEvent{
		Ref:    safeRef,
		Source: eth.L1BlockRef{}, // Minimal test data
	}

	// Process the event
	handled := deriver.OnEvent(ctx, promoteEvent)
	require.True(t, handled, "PromoteSafeEvent should be handled")

	// Verify that NO GossipSafeHeadEvent was emitted
	require.Len(t, capturedEvents, 2, "Should only emit SafeDerivedEvent and CrossSafeUpdateEvent")

	// Check that no GossipSafeHeadEvent exists
	for _, ev := range capturedEvents {
		_, isGossipEvent := ev.(p2p.GossipSafeHeadEvent)
		require.False(t, isGossipEvent, "GossipSafeHeadEvent should NOT be emitted in normal mode")
	}

	// Verify mocks - PayloadByNumber should NOT have been called
	mockL2.AssertNotCalled(t, "PayloadByNumber", mock.Anything, mock.Anything)
	mockEC.AssertExpectations(t)
}
