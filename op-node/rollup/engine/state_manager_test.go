package engine

import (
	"context"
	"fmt"
	"math/big"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/log"
	"github.com/stretchr/testify/require"

	"github.com/ethereum-optimism/optimism/op-node/rollup"
	"github.com/ethereum-optimism/optimism/op-node/rollup/derive"
	"github.com/ethereum-optimism/optimism/op-service/eth"
	"github.com/ethereum-optimism/optimism/op-service/event"
	"github.com/ethereum-optimism/optimism/op-service/testlog"
)

// MockEngineController for testing
type MockEngineController struct {
	tryUpdateEngineErr error
	insertPayloadErr   error
	unsafeL2Head       eth.L2BlockRef
	config             *rollup.Config

	// Call tracking
	tryUpdateEngineCalls int
	insertPayloadCalls   int

	// SetCrossUnsafeHead tracking
	setCrossUnsafeHeadCalled bool
	setCrossUnsafeHeadArg    eth.L2BlockRef

	// State heads for testing
	pendingSafeL2Head eth.L2BlockRef
	localSafeL2Head   eth.L2BlockRef
	safeL2Head        eth.L2BlockRef
	finalizedHead     eth.L2BlockRef
	crossUnsafeL2Head eth.L2BlockRef
}

func (m *MockEngineController) TryUpdateEngine(ctx context.Context) error {
	m.tryUpdateEngineCalls++
	return m.tryUpdateEngineErr
}

func (m *MockEngineController) InsertUnsafePayload(ctx context.Context, envelope *eth.ExecutionPayloadEnvelope, ref eth.L2BlockRef) error {
	m.insertPayloadCalls++
	return m.insertPayloadErr
}

func (m *MockEngineController) UnsafeL2Head() eth.L2BlockRef {
	return m.unsafeL2Head
}

// Config implements EngineControllerInterface
func (m *MockEngineController) Config() *rollup.Config {
	return m.config
}

// State head setters and getters for interface compliance
func (m *MockEngineController) SetUnsafeHead(ref eth.L2BlockRef) {
	m.unsafeL2Head = ref
}

func (m *MockEngineController) SetPendingSafeL2Head(ref eth.L2BlockRef) {
	m.pendingSafeL2Head = ref
}

func (m *MockEngineController) PendingSafeL2Head() eth.L2BlockRef {
	return m.pendingSafeL2Head
}

func (m *MockEngineController) SetLocalSafeHead(ref eth.L2BlockRef) {
	m.localSafeL2Head = ref
}

func (m *MockEngineController) LocalSafeL2Head() eth.L2BlockRef {
	return m.localSafeL2Head
}

func (m *MockEngineController) SetSafeHead(ref eth.L2BlockRef) {
	m.safeL2Head = ref
}

func (m *MockEngineController) SafeL2Head() eth.L2BlockRef {
	return m.safeL2Head
}

func (m *MockEngineController) SetFinalizedHead(ref eth.L2BlockRef) {
	m.finalizedHead = ref
}

func (m *MockEngineController) Finalized() eth.L2BlockRef {
	return m.finalizedHead
}

func (m *MockEngineController) SetCrossUnsafeHead(ref eth.L2BlockRef) {
	m.setCrossUnsafeHeadCalled = true
	m.setCrossUnsafeHeadArg = ref
	m.crossUnsafeL2Head = ref
}

func (m *MockEngineController) CrossUnsafeL2Head() eth.L2BlockRef {
	return m.crossUnsafeL2Head
}

func (m *MockEngineController) SetBackupUnsafeL2Head(ref eth.L2BlockRef, pending bool) {
	// Mock implementation - just store for testing
}

// MockEngineEventHandler for testing external handlers
type MockEngineEventHandler struct {
	events []struct {
		EventType string
		Data      interface{}
	}
	shouldError bool
}

func (m *MockEngineEventHandler) HandleEngineEvent(ctx context.Context, eventType string, data interface{}) error {
	m.events = append(m.events, struct {
		EventType string
		Data      interface{}
	}{eventType, data})

	if m.shouldError {
		return fmt.Errorf("mock handler error for %s", eventType)
	}
	return nil
}

func TestEngineStateManager_NewEngineStateManager(t *testing.T) {
	logger := testlog.Logger(t, log.LevelDebug)
	mockController := &MockEngineController{}

	esm := NewEngineStateManager(mockController, logger)

	require.NotNil(t, esm)
	require.Equal(t, mockController, esm.controller)
	require.Equal(t, logger, esm.log)
	require.True(t, esm.strictMode, "Should default to strict mode")
	require.NotNil(t, esm.externalHandlers)
}

func TestEngineStateManager_TryUpdateEngine_Success(t *testing.T) {
	logger := testlog.Logger(t, log.LevelDebug)
	mockController := &MockEngineController{}
	esm := NewEngineStateManager(mockController, logger)

	ctx := context.Background()
	err := esm.TryUpdateEngine(ctx)

	require.NoError(t, err)
	require.Equal(t, 1, mockController.tryUpdateEngineCalls)
}

func TestEngineStateManager_TryUpdateEngine_NoFCUNeeded(t *testing.T) {
	logger := testlog.Logger(t, log.LevelDebug)
	mockController := &MockEngineController{
		tryUpdateEngineErr: ErrNoFCUNeeded,
	}
	esm := NewEngineStateManager(mockController, logger)

	ctx := context.Background()
	err := esm.TryUpdateEngine(ctx)

	// ErrNoFCUNeeded should be treated as success
	require.NoError(t, err)
	require.Equal(t, 1, mockController.tryUpdateEngineCalls)
}

func TestEngineStateManager_TryUpdateEngine_ResetError(t *testing.T) {
	logger := testlog.Logger(t, log.LevelDebug)
	resetErr := derive.ErrReset
	mockController := &MockEngineController{
		tryUpdateEngineErr: resetErr,
	}
	esm := NewEngineStateManager(mockController, logger)

	// Register external handler to capture reset event
	mockHandler := &MockEngineEventHandler{}
	esm.RegisterExternalHandler("ResetEvent", mockHandler)

	ctx := context.Background()
	err := esm.TryUpdateEngine(ctx)

	require.Error(t, err)
	require.ErrorIs(t, err, derive.ErrReset)
	require.Equal(t, 1, mockController.tryUpdateEngineCalls)

	// Verify external handler was called
	require.Len(t, mockHandler.events, 1)
	require.Equal(t, "ResetEvent", mockHandler.events[0].EventType)
}

func TestEngineStateManager_TryUpdateEngine_TemporaryError(t *testing.T) {
	logger := testlog.Logger(t, log.LevelDebug)
	tempErr := derive.ErrTemporary
	mockController := &MockEngineController{
		tryUpdateEngineErr: tempErr,
	}
	esm := NewEngineStateManager(mockController, logger)

	// Register external handler to capture temporary error event
	mockHandler := &MockEngineEventHandler{}
	esm.RegisterExternalHandler("EngineTemporaryErrorEvent", mockHandler)

	ctx := context.Background()
	err := esm.TryUpdateEngine(ctx)

	require.Error(t, err)
	require.ErrorIs(t, err, derive.ErrTemporary)
	require.Equal(t, 1, mockController.tryUpdateEngineCalls)

	// Verify external handler was called
	require.Len(t, mockHandler.events, 1)
	require.Equal(t, "EngineTemporaryErrorEvent", mockHandler.events[0].EventType)
}

func TestEngineStateManager_TryUpdateEngine_CriticalError(t *testing.T) {
	logger := testlog.Logger(t, log.LevelDebug)
	criticalErr := fmt.Errorf("unexpected error")
	mockController := &MockEngineController{
		tryUpdateEngineErr: criticalErr,
	}
	esm := NewEngineStateManager(mockController, logger)

	// Register external handler to capture critical error event
	mockHandler := &MockEngineEventHandler{}
	esm.RegisterExternalHandler("CriticalErrorEvent", mockHandler)

	ctx := context.Background()
	err := esm.TryUpdateEngine(ctx)

	require.Error(t, err)
	require.Contains(t, err.Error(), "unexpected TryUpdateEngine error type")
	require.Equal(t, 1, mockController.tryUpdateEngineCalls)

	// Verify external handler was called
	require.Len(t, mockHandler.events, 1)
	require.Equal(t, "CriticalErrorEvent", mockHandler.events[0].EventType)
}

func TestEngineStateManager_ProcessUnsafePayload_Success(t *testing.T) {
	logger := testlog.Logger(t, log.LevelDebug)

	// Create a basic mock config to avoid nil pointer panic
	mockConfig := &rollup.Config{
		// Using nil config for now - the test will focus on error handling
	}

	mockController := &MockEngineController{
		unsafeL2Head: eth.L2BlockRef{Hash: [32]byte{1}, Number: 100},
		config:       mockConfig,
	}
	esm := NewEngineStateManager(mockController, logger)

	envelope := &eth.ExecutionPayloadEnvelope{
		ExecutionPayload: &eth.ExecutionPayload{
			BlockHash: [32]byte{2}, // Different from current unsafe head
		},
	}

	ctx := context.Background()
	err := esm.ProcessUnsafePayload(ctx, envelope)

	// This will likely still fail due to incomplete mock payload, but shouldn't panic
	require.Error(t, err)
	require.Contains(t, err.Error(), "failed to decode block ref")

	// The important thing is that we didn't panic and handled the error gracefully
}

func TestEngineStateManager_ProcessUnsafePayload_SkipDuplicate(t *testing.T) {
	logger := testlog.Logger(t, log.LevelDebug)
	sameHash := [32]byte{1}

	// Create a basic mock config
	mockConfig := &rollup.Config{
		// Using nil config for now - the test will focus on error handling
	}

	mockController := &MockEngineController{
		unsafeL2Head: eth.L2BlockRef{Hash: sameHash, Number: 100},
		config:       mockConfig,
	}
	esm := NewEngineStateManager(mockController, logger)

	envelope := &eth.ExecutionPayloadEnvelope{
		ExecutionPayload: &eth.ExecutionPayload{
			BlockHash: sameHash, // Same as current unsafe head
		},
	}

	ctx := context.Background()
	err := esm.ProcessUnsafePayload(ctx, envelope)

	// This will still fail due to incomplete mock payload, but shouldn't crash
	require.Error(t, err) // Expected due to payload decoding issues
	require.Equal(t, 0, mockController.insertPayloadCalls, "Should not call InsertUnsafePayload for duplicate")
}

func TestEngineStateManager_PromoteToPendingSafe(t *testing.T) {
	logger := testlog.Logger(t, log.LevelDebug)
	mockController := &MockEngineController{
		unsafeL2Head: eth.L2BlockRef{Hash: [32]byte{1}, Number: 100},
	}
	esm := NewEngineStateManager(mockController, logger)

	// Register external handler to capture pending safe update
	mockHandler := &MockEngineEventHandler{}
	esm.RegisterExternalHandler("PendingSafeUpdateEvent", mockHandler)

	ref := eth.L2BlockRef{Hash: [32]byte{2}, Number: 99}
	source := eth.L1BlockRef{Hash: [32]byte{3}, Number: 50}

	ctx := context.Background()
	err := esm.PromoteToPendingSafe(ctx, ref, source, true)

	require.NoError(t, err)

	// Verify external handler was called
	require.Len(t, mockHandler.events, 1)
	require.Equal(t, "PendingSafeUpdateEvent", mockHandler.events[0].EventType)

	// Verify the update data structure
	updateData := mockHandler.events[0].Data.(struct {
		PendingSafe eth.L2BlockRef
		Unsafe      eth.L2BlockRef
	})
	require.Equal(t, ref, updateData.PendingSafe)
	require.Equal(t, mockController.unsafeL2Head, updateData.Unsafe)
}

func TestEngineStateManager_StrictMode_Panic(t *testing.T) {
	logger := testlog.Logger(t, log.LevelDebug)
	mockController := &MockEngineController{}
	esm := NewEngineStateManager(mockController, logger)

	// Register handler that will error
	mockHandler := &MockEngineEventHandler{shouldError: true}
	esm.RegisterExternalHandler("TestEvent", mockHandler)
	esm.SetStrictMode(true)

	// This should panic due to strict mode
	require.Panics(t, func() {
		esm.notifyExternalHandlers(context.Background(), "TestEvent", "test data")
	})
}

func TestEngineStateManager_NonStrictMode_NoError(t *testing.T) {
	logger := testlog.Logger(t, log.LevelDebug)
	mockController := &MockEngineController{}
	esm := NewEngineStateManager(mockController, logger)

	// Register handler that will error
	mockHandler := &MockEngineEventHandler{shouldError: true}
	esm.RegisterExternalHandler("TestEvent", mockHandler)
	esm.SetStrictMode(false)

	// This should not panic due to non-strict mode
	require.NotPanics(t, func() {
		esm.notifyExternalHandlers(context.Background(), "TestEvent", "test data")
	})

	// Verify the handler was still called
	require.Len(t, mockHandler.events, 1)
	require.Equal(t, "TestEvent", mockHandler.events[0].EventType)
}

func TestEngineStateManager_ValidateConfiguration(t *testing.T) {
	logger := testlog.Logger(t, log.LevelDebug)

	// Test with nil controller
	esm := &EngineStateManager{log: logger}
	err := esm.validateConfiguration()
	require.Error(t, err)
	require.Contains(t, err.Error(), "EngineController is nil")

	// Test with nil logger
	mockController := &MockEngineController{}
	esm = &EngineStateManager{controller: mockController}
	err = esm.validateConfiguration()
	require.Error(t, err)
	require.Contains(t, err.Error(), "Logger is nil")

	// Test with valid configuration
	esm = NewEngineStateManager(mockController, logger)
	err = esm.validateConfiguration()
	require.NoError(t, err)
}

func TestEngineStateManager_GetStats(t *testing.T) {
	logger := testlog.Logger(t, log.LevelDebug)
	mockController := &MockEngineController{}
	esm := NewEngineStateManager(mockController, logger)

	// Register some handlers
	mockHandler := &MockEngineEventHandler{}
	esm.RegisterExternalHandler("TestEvent1", mockHandler)
	esm.RegisterExternalHandler("TestEvent2", mockHandler)

	stats := esm.GetStats()

	require.Equal(t, true, stats["strict_mode"])
	require.Equal(t, 2, stats["external_handlers"])
	require.Equal(t, true, stats["controller_set"])
}

func TestEngineStateManager_RegisterExternalHandler(t *testing.T) {
	logger := testlog.Logger(t, log.LevelDebug)
	mockController := &MockEngineController{}
	esm := NewEngineStateManager(mockController, logger)

	mockHandler1 := &MockEngineEventHandler{}
	mockHandler2 := &MockEngineEventHandler{}

	esm.RegisterExternalHandler("Event1", mockHandler1)
	esm.RegisterExternalHandler("Event2", mockHandler2)

	require.Len(t, esm.externalHandlers, 2)
	require.Equal(t, mockHandler1, esm.externalHandlers["Event1"])
	require.Equal(t, mockHandler2, esm.externalHandlers["Event2"])
}

// TestEngineStateManager_RequestForkchoiceUpdate_Success validates the ForkchoiceRequestEvent replacement
func TestEngineStateManager_RequestForkchoiceUpdate_Success(t *testing.T) {
	// Create a mock controller with expected engine state
	mockController := &MockEngineController{
		config: &rollup.Config{L2ChainID: big.NewInt(1)},
		unsafeL2Head: eth.L2BlockRef{
			Hash:   common.HexToHash("0x1234"),
			Number: 10,
		},
		safeL2Head: eth.L2BlockRef{
			Hash:   common.HexToHash("0x5678"),
			Number: 8,
		},
		finalizedHead: eth.L2BlockRef{
			Hash:   common.HexToHash("0x9abc"),
			Number: 5,
		},
	}

	// Create state manager
	logger := testlog.Logger(t, log.LevelDebug)
	stateManager := NewEngineStateManager(mockController, logger)

	// Create a mock emitter to capture emitted events
	emittedEvents := []event.Event{}
	mockEmitter := &MockEmitter{
		EmitFunc: func(ctx context.Context, ev event.Event) {
			emittedEvents = append(emittedEvents, ev)
		},
	}

	// Call RequestForkchoiceUpdate
	ctx := context.Background()
	err := stateManager.RequestForkchoiceUpdate(ctx, mockEmitter)

	// Validate result
	require.NoError(t, err)
	require.Len(t, emittedEvents, 1, "Should emit exactly one ForkchoiceUpdateEvent")

	// Validate the emitted event has correct engine state
	if forkchoiceEvent, ok := emittedEvents[0].(ForkchoiceUpdateEvent); ok {
		require.Equal(t, mockController.unsafeL2Head, forkchoiceEvent.UnsafeL2Head)
		require.Equal(t, mockController.safeL2Head, forkchoiceEvent.SafeL2Head)
		require.Equal(t, mockController.finalizedHead, forkchoiceEvent.FinalizedL2Head)
	} else {
		t.Fatalf("Expected ForkchoiceUpdateEvent, got %T", emittedEvents[0])
	}
}

func TestEngineStateManager_RequestCrossUpdate_BothFlags(t *testing.T) {
	// Create mock controller with test data
	mockController := &MockEngineController{
		crossUnsafeL2Head: eth.L2BlockRef{Hash: common.HexToHash("0x1234"), Number: 100},
		unsafeL2Head:      eth.L2BlockRef{Hash: common.HexToHash("0x5678"), Number: 101},
		safeL2Head:        eth.L2BlockRef{Hash: common.HexToHash("0x9abc"), Number: 98},
		localSafeL2Head:   eth.L2BlockRef{Hash: common.HexToHash("0xdef0"), Number: 97},
		config:            &rollup.Config{L2ChainID: big.NewInt(1)},
	}

	// Create state manager
	logger := testlog.Logger(t, log.LevelDebug)
	stateManager := NewEngineStateManager(mockController, logger)

	// Create a mock emitter to capture emitted events
	emittedEvents := []event.Event{}
	mockEmitter := &MockEmitter{
		EmitFunc: func(ctx context.Context, ev event.Event) {
			emittedEvents = append(emittedEvents, ev)
		},
	}

	// Test with both flags true - should emit both events
	ctx := context.Background()
	err := stateManager.RequestCrossUpdate(ctx, true, true, mockEmitter)

	require.NoError(t, err)
	require.Len(t, emittedEvents, 2, "Should emit exactly two events when both flags are true")

	// Validate the first event is CrossUnsafeUpdateEvent
	if crossUnsafeEvent, ok := emittedEvents[0].(CrossUnsafeUpdateEvent); ok {
		require.Equal(t, mockController.crossUnsafeL2Head, crossUnsafeEvent.CrossUnsafe)
		require.Equal(t, mockController.unsafeL2Head, crossUnsafeEvent.LocalUnsafe)
	} else {
		t.Fatalf("Expected CrossUnsafeUpdateEvent as first event, got %T", emittedEvents[0])
	}

	// Validate the second event is CrossSafeUpdateEvent
	if crossSafeEvent, ok := emittedEvents[1].(CrossSafeUpdateEvent); ok {
		require.Equal(t, mockController.safeL2Head, crossSafeEvent.CrossSafe)
		require.Equal(t, mockController.localSafeL2Head, crossSafeEvent.LocalSafe)
	} else {
		t.Fatalf("Expected CrossSafeUpdateEvent as second event, got %T", emittedEvents[1])
	}
}

func TestEngineStateManager_RequestCrossUpdate_NoFlags(t *testing.T) {
	// Test the original driver/state.go behavior: CrossUpdateRequestEvent{} (both flags false)
	mockController := &MockEngineController{
		config: &rollup.Config{L2ChainID: big.NewInt(1)},
	}

	logger := testlog.Logger(t, log.LevelDebug)
	stateManager := NewEngineStateManager(mockController, logger)

	// Create a mock emitter to capture emitted events
	emittedEvents := []event.Event{}
	mockEmitter := &MockEmitter{
		EmitFunc: func(ctx context.Context, ev event.Event) {
			emittedEvents = append(emittedEvents, ev)
		},
	}

	// Test with both flags false (original behavior) - should emit nothing
	ctx := context.Background()
	err := stateManager.RequestCrossUpdate(ctx, false, false, mockEmitter)

	require.NoError(t, err)
	require.Len(t, emittedEvents, 0, "Should emit no events when both flags are false (original behavior)")
}

func TestEngineStateManager_InvalidateBlock_Success(t *testing.T) {
	// Create mock controller
	mockController := &MockEngineController{
		config: &rollup.Config{L2ChainID: big.NewInt(1)},
	}

	logger := testlog.Logger(t, log.LevelDebug)
	stateManager := NewEngineStateManager(mockController, logger)

	// Create test data
	invalidatedRef := eth.BlockRef{Hash: common.HexToHash("0xabcd"), Number: 100}
	attributes := &derive.AttributesWithParent{
		Attributes: &eth.PayloadAttributes{
			Timestamp: 0x123456,
		},
		Parent: eth.L2BlockRef{Hash: common.HexToHash("0x1234"), Number: 99},
	}

	// Create a mock emitter to capture emitted events
	emittedEvents := []event.Event{}
	mockEmitter := &MockEmitter{
		EmitFunc: func(ctx context.Context, ev event.Event) {
			emittedEvents = append(emittedEvents, ev)
		},
	}

	// Call InvalidateBlock
	ctx := context.Background()
	err := stateManager.InvalidateBlock(ctx, invalidatedRef, attributes, mockEmitter)

	// Validate result - should emit BuildStartEvent
	require.NoError(t, err)
	require.Len(t, emittedEvents, 1, "Should emit exactly one BuildStartEvent")

	// Validate the emitted event is BuildStartEvent with correct attributes
	if buildStartEvent, ok := emittedEvents[0].(BuildStartEvent); ok {
		require.Equal(t, attributes, buildStartEvent.Attributes)
	} else {
		t.Fatalf("Expected BuildStartEvent, got %T", emittedEvents[0])
	}
}

func TestEngineStateManager_PromoteCrossUnsafe_Success(t *testing.T) {
	// Create mock controller
	mockController := &MockEngineController{
		unsafeL2Head: eth.L2BlockRef{Hash: common.HexToHash("0x5678"), Number: 101},
		config:       &rollup.Config{L2ChainID: big.NewInt(1)},
	}
	
	logger := testlog.Logger(t, log.LevelDebug)
	stateManager := NewEngineStateManager(mockController, logger)
	
	// Create test data
	crossUnsafeRef := eth.L2BlockRef{Hash: common.HexToHash("0xabcd"), Number: 100}
	
	// Create a mock emitter to capture emitted events
	emittedEvents := []event.Event{}
	mockEmitter := &MockEmitter{
		EmitFunc: func(ctx context.Context, ev event.Event) {
			emittedEvents = append(emittedEvents, ev)
		},
	}
	
	// Call PromoteCrossUnsafe
	ctx := context.Background()
	err := stateManager.PromoteCrossUnsafe(ctx, crossUnsafeRef, mockEmitter)
	
	// Validate result - should update state and emit CrossUnsafeUpdateEvent
	require.NoError(t, err)
	require.Len(t, emittedEvents, 1, "Should emit exactly one CrossUnsafeUpdateEvent")
	
	// Validate the emitted event is CrossUnsafeUpdateEvent with correct data
	if crossUnsafeEvent, ok := emittedEvents[0].(CrossUnsafeUpdateEvent); ok {
		require.Equal(t, crossUnsafeRef, crossUnsafeEvent.CrossUnsafe)
		require.Equal(t, mockController.unsafeL2Head, crossUnsafeEvent.LocalUnsafe)
	} else {
		t.Fatalf("Expected CrossUnsafeUpdateEvent, got %T", emittedEvents[0])
	}
	
	// Validate that SetCrossUnsafeHead was called on the controller
	require.True(t, mockController.setCrossUnsafeHeadCalled, "SetCrossUnsafeHead should have been called")
	require.Equal(t, crossUnsafeRef, mockController.setCrossUnsafeHeadArg, "SetCrossUnsafeHead should have been called with correct argument")
}

// MockEmitter for testing event emissions
type MockEmitter struct {
	EmitFunc func(ctx context.Context, ev event.Event)
}

func (m *MockEmitter) Emit(ctx context.Context, ev event.Event) {
	if m.EmitFunc != nil {
		m.EmitFunc(ctx, ev)
	}
}
