package controllers

import (
	"context"
	"errors"
	"testing"

	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum-optimism/optimism/op-service/eth"
	"github.com/ethereum-optimism/optimism/op-service/testlog"
	"github.com/stretchr/testify/require"
)

// MockForkchoiceHandler for testing
type MockForkchoiceHandler struct {
	callCount int
	lastUpdate ForkchoiceUpdate
	shouldError bool
}

func (m *MockForkchoiceHandler) HandleForkchoiceUpdate(ctx context.Context, update ForkchoiceUpdate) error {
	m.callCount++
	m.lastUpdate = update
	if m.shouldError {
		return errors.New("mock handler error")
	}
	return nil
}

func TestForkchoiceController_ProcessForkchoiceUpdate(t *testing.T) {
	logger := testlog.Logger(t, log.LevelInfo)
	
	// Create mock handlers
	handler1 := &MockForkchoiceHandler{}
	handler2 := &MockForkchoiceHandler{}
	handler3 := &MockForkchoiceHandler{shouldError: true} // This one will error
	
	// Create controller with handlers
	fc := NewForkchoiceController(logger, handler1, handler2, handler3)
	
	// Create test update
	update := ForkchoiceUpdate{
		UnsafeL2Head:    eth.L2BlockRef{Hash: [32]byte{1}, Number: 100},
		SafeL2Head:      eth.L2BlockRef{Hash: [32]byte{2}, Number: 99},
		FinalizedL2Head: eth.L2BlockRef{Hash: [32]byte{3}, Number: 98},
	}
	
	// Process the update
	ctx := context.Background()
	err := fc.ProcessForkchoiceUpdate(ctx, update)
	require.NoError(t, err, "ProcessForkchoiceUpdate should not return error even if handlers fail")
	
	// Verify all handlers were called
	require.Equal(t, 1, handler1.callCount, "Handler 1 should be called once")
	require.Equal(t, 1, handler2.callCount, "Handler 2 should be called once")
	require.Equal(t, 1, handler3.callCount, "Handler 3 should be called once (even though it errors)")
	
	// Verify handlers received correct data
	require.Equal(t, update, handler1.lastUpdate, "Handler 1 should receive correct update")
	require.Equal(t, update, handler2.lastUpdate, "Handler 2 should receive correct update")
	require.Equal(t, update, handler3.lastUpdate, "Handler 3 should receive correct update")
}

func TestForkchoiceController_RegisterHandler(t *testing.T) {
	logger := testlog.Logger(t, log.LevelInfo)
	
	// Create controller with one handler
	handler1 := &MockForkchoiceHandler{}
	fc := NewForkchoiceController(logger, handler1)
	
	// Register additional handler
	handler2 := &MockForkchoiceHandler{}
	fc.RegisterHandler(handler2)
	
	// Process update
	update := ForkchoiceUpdate{
		UnsafeL2Head: eth.L2BlockRef{Hash: [32]byte{1}, Number: 100},
	}
	
	ctx := context.Background()
	err := fc.ProcessForkchoiceUpdate(ctx, update)
	require.NoError(t, err)
	
	// Verify both handlers were called
	require.Equal(t, 1, handler1.callCount, "Original handler should be called")
	require.Equal(t, 1, handler2.callCount, "Registered handler should be called")
}

func TestForkchoiceUpdate_ConvertToEvent(t *testing.T) {
	// Test conversion between our format and the old event format
	update := ForkchoiceUpdate{
		UnsafeL2Head:    eth.L2BlockRef{Hash: [32]byte{1}, Number: 100},
		SafeL2Head:      eth.L2BlockRef{Hash: [32]byte{2}, Number: 99}, 
		FinalizedL2Head: eth.L2BlockRef{Hash: [32]byte{3}, Number: 98},
	}
	
	// Convert to event and back
	event := update.ConvertToEvent()
	backToUpdate := ConvertFromEvent(event)
	
	// Should be identical
	require.Equal(t, update, backToUpdate, "Conversion should be lossless")
	
	// Verify event fields
	require.Equal(t, update.UnsafeL2Head, event.UnsafeL2Head)
	require.Equal(t, update.SafeL2Head, event.SafeL2Head)
	require.Equal(t, update.FinalizedL2Head, event.FinalizedL2Head)
}

func TestForkchoiceController_EmptyHandlers(t *testing.T) {
	logger := testlog.Logger(t, log.LevelInfo)
	
	// Create controller with no handlers
	fc := NewForkchoiceController(logger)
	
	// Process update - should not panic
	update := ForkchoiceUpdate{
		UnsafeL2Head: eth.L2BlockRef{Hash: [32]byte{1}, Number: 100},
	}
	
	ctx := context.Background()
	err := fc.ProcessForkchoiceUpdate(ctx, update)
	require.NoError(t, err, "Should handle empty handlers gracefully")
}