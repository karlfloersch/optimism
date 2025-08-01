package flow

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/log"
	"github.com/stretchr/testify/require"

	"github.com/ethereum-optimism/optimism/op-service/event"
	"github.com/ethereum-optimism/optimism/op-service/testlog"
)

// TestEventLoopMigrationBaseline establishes a baseline of event patterns
// that we'll use to verify our controller migration preserves behavior
func TestEventLoopMigrationBaseline(t *testing.T) {
	logger := testlog.Logger(t, log.LevelInfo)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Enable flow tracing for event capture
	os.Setenv("OP_NODE_FLOW_TRACING", "true")
	defer os.Unsetenv("OP_NODE_FLOW_TRACING")

	executor := event.NewGlobalSynchronous(ctx)
	sys := event.NewSystem(logger, executor)
	defer sys.Stop()

	// Initialize flow tracer (simulates op-node initialization)
	var flowTracer *FlowTracer
	if os.Getenv("OP_NODE_FLOW_TRACING") == "true" {
		flowTracer = NewFlowTracer()
		sys.AddTracer(flowTracer)
		logger.Info("Flow tracing enabled for event loop migration testing")
	}

	require.NotNil(t, flowTracer, "flow tracer should be initialized")

	// Create derivers that simulate the high-frequency event patterns we want to replace
	forkchoiceEmitter := sys.Register("forkchoice-handler", nil)
	engineEmitter := sys.Register("engine-handler", nil)
	unsafeEmitter := sys.Register("unsafe-handler", nil)
	sequencerEmitter := sys.Register("sequencer-handler", nil)
	buildEmitter := sys.Register("build-handler", nil)

	forkchoiceDeriver := event.DeriverFunc(func(ctx context.Context, ev event.Event) bool {
		switch e := ev.(type) {
		case *ForkchoiceRequestEvent:
			// Simulate the forkchoice-update event pattern (180x frequency in real traces)
			forkchoiceEmitter.Emit(ctx, &ForkchoiceUpdateEvent{
				UnsafeL2Head:    e.UnsafeL2Head,
				SafeL2Head:      e.SafeL2Head,
				FinalizedL2Head: e.FinalizedL2Head,
			})
		case *ForkchoiceUpdateEvent:
			// Simulate downstream processing from forkchoice-update
			engineEmitter.Emit(ctx, &TryUpdateEngineEvent{})
		}
		return true
	})

	engineDeriver := event.DeriverFunc(func(ctx context.Context, ev event.Event) bool {
		switch e := ev.(type) {
		case *TryUpdateEngineEvent:
			// Simulate try-update-engine processing (165x frequency, depth-8 chains)
			for i := 0; i < 3; i++ { // Simulate a shorter chain for testing
				engineEmitter.Emit(ctx, &UnsafeUpdateEvent{Ref: e.Ref})
			}
		case *UnsafeUpdateEvent:
			// Simulate unsafe update cascading
			unsafeEmitter.Emit(ctx, &PromoteUnsafeEvent{Ref: e.Ref})
		}
		return true
	})

	sequencerDeriver := event.DeriverFunc(func(ctx context.Context, ev event.Event) bool {
		switch e := ev.(type) {
		case *SequencerActionEvent:
			// Simulate sequencer-action event pattern (120x frequency)
			sequencerEmitter.Emit(ctx, &BuildStartEvent{L1Origin: e.L1Origin})
		case *BuildStartEvent:
			// Simulate block building cascade
			buildEmitter.Emit(ctx, &BuildSealEvent{Block: e.L1Origin})
		}
		return true
	})

	// Register derivers
	mainEmitter := sys.Register("forkchoice-system", forkchoiceDeriver)
	sys.Register("engine-system", engineDeriver)
	sys.Register("sequencer-system", sequencerDeriver)

	// Simulate the high-frequency events from our real trace analysis
	testEvents := []event.Event{
		// Forkchoice events (180x in real traces)
		&ForkchoiceRequestEvent{UnsafeL2Head: "unsafe1", SafeL2Head: "safe1", FinalizedL2Head: "finalized1"},
		&ForkchoiceRequestEvent{UnsafeL2Head: "unsafe2", SafeL2Head: "safe2", FinalizedL2Head: "finalized2"},
		&ForkchoiceRequestEvent{UnsafeL2Head: "unsafe3", SafeL2Head: "safe3", FinalizedL2Head: "finalized3"},

		// Try-update-engine events (165x in real traces, creates depth-8 chains)
		&TryUpdateEngineEvent{Ref: "engine1"},
		&TryUpdateEngineEvent{Ref: "engine2"},

		// Sequencer-action events (120x in real traces)
		&SequencerActionEvent{L1Origin: "origin1"},
		&SequencerActionEvent{L1Origin: "origin2"},
	}

	// Emit events to create realistic patterns
	for _, ev := range testEvents {
		mainEmitter.Emit(ctx, ev)
	}

	// Process all events
	require.NoError(t, executor.Drain())

	// Capture results for baseline
	events := flowTracer.GetEvents()
	stats := flowTracer.GetStats()

	require.Greater(t, len(events), 10, "should capture cascading events from derivers")
	require.Greater(t, stats.TotalEvents, 10, "should have substantial event activity")

	// Log baseline metrics
	t.Logf("🔍 BASELINE EVENT LOOP METRICS:")
	t.Logf("   📊 Total Events: %d", stats.TotalEvents)
	t.Logf("   ⚡ Forkchoice Events: %d", countEventsByName(events, "forkchoice-update"))
	t.Logf("   🎯 Engine Events: %d", countEventsByName(events, "try-update-engine"))
	t.Logf("   🎬 Sequencer Events: %d", countEventsByName(events, "sequencer-action"))
	t.Logf("   🔗 Correlations: %d", stats.Correlations)
	t.Logf("   📋 Unique Patterns: %d", stats.UniquePatterns)

	// Verify we captured the patterns we're trying to replace
	forkchoiceCount := countEventsByName(events, "forkchoice-update")
	engineCount := countEventsByName(events, "try-update-engine")
	sequencerCount := countEventsByName(events, "sequencer-action")

	require.Greater(t, forkchoiceCount, 0, "should capture forkchoice events")
	require.Greater(t, engineCount, 0, "should capture engine events")
	require.Greater(t, sequencerCount, 0, "should capture sequencer events")

	t.Log("✅ Baseline established - ready for controller migration testing")
}

// Helper function to count events by name
func countEventsByName(events []CapturedEvent, eventName string) int {
	count := 0
	for _, ev := range events {
		if ev.EventName == eventName {
			count++
		}
	}
	return count
}

// Test event types that simulate the real events we're refactoring
type ForkchoiceRequestEvent struct {
	UnsafeL2Head    string `json:"unsafe_l2_head"`
	SafeL2Head      string `json:"safe_l2_head"`
	FinalizedL2Head string `json:"finalized_l2_head"`
}

func (e ForkchoiceRequestEvent) String() string { return "forkchoice-request" }

type ForkchoiceUpdateEvent struct {
	UnsafeL2Head    string `json:"unsafe_l2_head"`    
	SafeL2Head      string `json:"safe_l2_head"`
	FinalizedL2Head string `json:"finalized_l2_head"`
}

func (e ForkchoiceUpdateEvent) String() string { return "forkchoice-update" }

type TryUpdateEngineEvent struct {
	Ref string `json:"ref"`
}

func (e TryUpdateEngineEvent) String() string { return "try-update-engine" }

type UnsafeUpdateEvent struct {
	Ref string `json:"ref"`
}

func (e UnsafeUpdateEvent) String() string { return "unsafe-update" }

type PromoteUnsafeEvent struct {
	Ref string `json:"ref"`
}

func (e PromoteUnsafeEvent) String() string { return "promote-unsafe" }

type SequencerActionEvent struct {
	L1Origin string `json:"l1_origin"`
}

func (e SequencerActionEvent) String() string { return "sequencer-action" }

type BuildStartEvent struct {
	L1Origin string `json:"l1_origin"`
}

func (e BuildStartEvent) String() string { return "build-start" }

type BuildSealEvent struct {
	Block string `json:"block"`
}

func (e BuildSealEvent) String() string { return "build-seal" }