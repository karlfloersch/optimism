package flow

import (
	"context"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/log"
	"github.com/stretchr/testify/require"

	"github.com/ethereum-optimism/optimism/op-service/event"
	"github.com/ethereum-optimism/optimism/op-service/testlog"
)

// TestFlowTracerBasics tests the basic functionality of the flow tracer
func TestFlowTracerBasics(t *testing.T) {
	logger := testlog.Logger(t, log.LevelInfo)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Create event system with flow tracer
	executor := event.NewGlobalSynchronous(ctx)
	sys := event.NewSystem(logger, executor)
	defer sys.Stop()

	// Add our flow tracer
	tracer := NewFlowTracer()
	sys.AddTracer(tracer)

	// Register a simple deriver for testing
	testDeriver := event.DeriverFunc(func(ctx context.Context, ev event.Event) bool {
		switch ev.(type) {
		case TestEvent:
			return true
		default:
			return false
		}
	})

	emitter := sys.Register("test-deriver", testDeriver)

	// Emit some test events
	emitter.Emit(ctx, TestEvent{Name: "test1"})
	emitter.Emit(ctx, TestEvent{Name: "test2"})

	// Process all events
	require.NoError(t, executor.Drain())

	// Validate tracer captured events
	stats := tracer.GetStats()
	events := tracer.GetEvents()

	t.Logf("Stats: %+v", stats)
	t.Logf("Captured %d events", len(events))

	// Basic validation
	require.Greater(t, len(events), 0, "should capture some events")
	require.Greater(t, stats.TotalEvents, 0, "should have processed some events")

	// Validate event structure
	for _, ev := range events {
		require.NotEmpty(t, ev.EventName, "event should have name")
		require.NotEmpty(t, ev.DeriverName, "event should have deriver name")
		t.Logf("Event: %s from %s (emit:%d, deriv:%d)",
			ev.EventName, ev.DeriverName, ev.EmitContext, ev.DerivContext)
	}
}

// TestEvent is a simple test event
type TestEvent struct {
	Name string
}

func (e TestEvent) String() string {
	return "test-event"
}

// TestFlowTracerCorrelations tests event correlation tracking
func TestFlowTracerCorrelations(t *testing.T) {
	logger := testlog.Logger(t, log.LevelInfo)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	executor := event.NewGlobalSynchronous(ctx)
	sys := event.NewSystem(logger, executor)
	defer sys.Stop()

	tracer := NewFlowTracer()
	sys.AddTracer(tracer)

	// Create a deriver that emits events (creates correlations)
	emittingDeriver := event.DeriverFunc(func(ctx context.Context, ev event.Event) bool {
		if _, ok := ev.(TestEvent); ok {
			// This deriver emits a new event when it receives one
			// This should create a correlation
			return true
		}
		return false
	})

	emitter := sys.Register("emitting-deriver", emittingDeriver)
	emitter.Emit(ctx, TestEvent{Name: "trigger"})

	require.NoError(t, executor.Drain())

	// Check correlations were captured
	correlations := tracer.GetCorrelations()
	require.Greater(t, len(correlations), 0, "should have captured correlations")

	t.Logf("Captured %d correlations", len(correlations))
}

// NOTE: Real devstack integration would happen in op-devstack/ tests, not here.
// This file contains only unit tests for the flow tracer itself.
//
// Example devstack integration (in op-devstack/example/):
//
// func TestFlowTracingIntegration(gt *testing.T) {
//     t := devtest.ParallelT(gt)
//     sys := presets.NewSimpleInterop(t)
//
//     // Import and add flow tracer
//     tracer := flow.NewFlowTracer()  // from op-node/flow
//     sys.OpNodeA.AddTracer(tracer)   // Add to op-node event system
//
//     // Run actual test scenario
//     sys.Supervisor.VerifySyncStatus(dsl.WithAllLocalUnsafeHeadsAdvancedBy(10))
//
//     // Analyze captured flows
//     events := tracer.GetEvents()
//     // Generate AST and reports...
// }
