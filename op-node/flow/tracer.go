package flow

import (
	"sync"
	"time"

	"github.com/ethereum-optimism/optimism/op-service/event"
)

// FlowTracer captures event flows for AST generation
type FlowTracer struct {
	mu sync.RWMutex

	// Event capture
	events []CapturedEvent

	// Correlation tracking
	correlations map[uint64]uint64 // emitContext -> derivContext

	// Metrics
	startTime time.Time
	stats     TracingStats
}

// CapturedEvent represents a single event with full context
type CapturedEvent struct {
	// From AnnotatedEvent
	EmitContext  uint64
	DerivContext uint64
	EventName    string

	// Timing
	EmitTime    time.Time
	ProcessTime time.Time
	Duration    time.Duration

	// Metadata
	DeriverName string
	Effect      bool
}

// TracingStats tracks tracer performance and completeness
type TracingStats struct {
	TotalEvents    int
	MissedEvents   int
	Correlations   int
	UniquePatterns int
	ProcessingTime time.Duration
}

// NewFlowTracer creates a new flow tracer
func NewFlowTracer() *FlowTracer {
	return &FlowTracer{
		events:       make([]CapturedEvent, 0),
		correlations: make(map[uint64]uint64),
		startTime:    time.Now(),
	}
}

// Implement event.Tracer interface
var _ event.Tracer = (*FlowTracer)(nil)

func (ft *FlowTracer) OnDeriveStart(name string, ev event.AnnotatedEvent, derivContext uint64, startTime time.Time) {
	ft.mu.Lock()
	defer ft.mu.Unlock()

	// Track the start of event processing
	captured := CapturedEvent{
		EmitContext:  ev.EmitContext,
		DerivContext: derivContext,
		EventName:    ev.Event.String(),
		ProcessTime:  startTime,
		DeriverName:  name,
	}

	// Find corresponding emit event
	for i := len(ft.events) - 1; i >= 0; i-- {
		if ft.events[i].EmitContext == ev.EmitContext && ft.events[i].EmitTime.IsZero() == false {
			captured.EmitTime = ft.events[i].EmitTime
			break
		}
	}

	ft.events = append(ft.events, captured)
	ft.stats.TotalEvents++
}

func (ft *FlowTracer) OnDeriveEnd(name string, ev event.AnnotatedEvent, derivContext uint64, startTime time.Time, duration time.Duration, effect bool) {
	ft.mu.Lock()
	defer ft.mu.Unlock()

	// Update the most recent matching event with completion info
	for i := len(ft.events) - 1; i >= 0; i-- {
		if ft.events[i].EmitContext == ev.EmitContext && ft.events[i].DerivContext == derivContext {
			ft.events[i].Duration = duration
			ft.events[i].Effect = effect
			break
		}
	}
}

func (ft *FlowTracer) OnEmit(name string, ev event.AnnotatedEvent, derivContext uint64, emitTime time.Time) {
	ft.mu.Lock()
	defer ft.mu.Unlock()

	// Record correlation between this emit and the deriver that caused it
	ft.correlations[ev.EmitContext] = derivContext

	// Track emit event
	captured := CapturedEvent{
		EmitContext:  ev.EmitContext,
		DerivContext: derivContext,
		EventName:    ev.Event.String(),
		EmitTime:     emitTime,
		DeriverName:  name,
	}

	ft.events = append(ft.events, captured)
	ft.stats.Correlations++
}

func (ft *FlowTracer) OnRateLimited(name string, derivContext uint64) {
	ft.mu.Lock()
	defer ft.mu.Unlock()

	// Track rate limiting events
	captured := CapturedEvent{
		DerivContext: derivContext,
		EventName:    "rate-limited",
		EmitTime:     time.Now(),
		DeriverName:  name,
	}

	ft.events = append(ft.events, captured)
}

func (ft *FlowTracer) OnAfterProcessed(evtype string) {
	// Track completion of event processing
	// This is called after all derivers have processed an event
}

// GetStats returns current tracing statistics
func (ft *FlowTracer) GetStats() TracingStats {
	ft.mu.RLock()
	defer ft.mu.RUnlock()

	stats := ft.stats
	stats.ProcessingTime = time.Since(ft.startTime)
	return stats
}

// GetEvents returns all captured events (for analysis)
func (ft *FlowTracer) GetEvents() []CapturedEvent {
	ft.mu.RLock()
	defer ft.mu.RUnlock()

	// Return a copy to avoid race conditions
	events := make([]CapturedEvent, len(ft.events))
	copy(events, ft.events)
	return events
}

// GetCorrelations returns event correlation map
func (ft *FlowTracer) GetCorrelations() map[uint64]uint64 {
	ft.mu.RLock()
	defer ft.mu.RUnlock()

	// Return a copy
	correlations := make(map[uint64]uint64)
	for k, v := range ft.correlations {
		correlations[k] = v
	}
	return correlations
}

// GenerateReport creates a completeness and analysis report
func (ft *FlowTracer) GenerateReport() ([]byte, error) {
	// TODO: Implement comprehensive reporting
	// This will generate JSON reports with:
	// - Completeness metrics
	// - Flow patterns identified
	// - Missing correlations
	// - Performance stats
	panic("not implemented yet - Phase 2")
}
