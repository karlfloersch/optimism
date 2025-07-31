package flow

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
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

	// File output
	outputDir   string
	autoSave    bool
	flushOnExit bool
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
	outputDir := os.Getenv("OP_NODE_FLOW_TRACE_DIR")
	if outputDir == "" {
		outputDir = "/tmp/flow-traces"
	}

	return &FlowTracer{
		events:       make([]CapturedEvent, 0),
		correlations: make(map[uint64]uint64),
		startTime:    time.Now(),
		stats:        TracingStats{},
		outputDir:    outputDir,
		autoSave:     os.Getenv("OP_NODE_FLOW_AUTOSAVE") == "true",
		flushOnExit:  true,
	}
}

// NewFlowTracerWithOptions creates a flow tracer with custom options
func NewFlowTracerWithOptions(outputDir string, autoSave bool) *FlowTracer {
	return &FlowTracer{
		events:       make([]CapturedEvent, 0),
		correlations: make(map[uint64]uint64),
		startTime:    time.Now(),
		stats:        TracingStats{},
		outputDir:    outputDir,
		autoSave:     autoSave,
		flushOnExit:  true,
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

// File Output Methods

// FlushToFile saves all captured events to a JSON file
func (ft *FlowTracer) FlushToFile() error {
	ft.mu.RLock()
	defer ft.mu.RUnlock()

	if err := os.MkdirAll(ft.outputDir, 0755); err != nil {
		return fmt.Errorf("failed to create output directory: %w", err)
	}

	timestamp := time.Now().Format("20060102-150405")
	filename := filepath.Join(ft.outputDir, fmt.Sprintf("flow-events-%s.json", timestamp))

	output := FlowTraceOutput{
		Metadata: TraceMetadata{
			StartTime:     ft.startTime,
			EndTime:       time.Now(),
			TotalEvents:   len(ft.events),
			TotalDuration: time.Since(ft.startTime),
			Version:       "1.0.0",
		},
		Events:       ft.events,
		Correlations: ft.correlations,
		Stats:        ft.computeStats(),
	}

	data, err := json.MarshalIndent(output, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal events: %w", err)
	}

	if err := os.WriteFile(filename, data, 0644); err != nil {
		return fmt.Errorf("failed to write file: %w", err)
	}

	fmt.Printf("Flow trace saved to: %s\n", filename)
	fmt.Printf("Captured %d events with %d correlations\n", len(ft.events), len(ft.correlations))

	return nil
}

// GenerateReport creates a completeness and analysis report
func (ft *FlowTracer) GenerateReport() ([]byte, error) {
	ft.mu.RLock()
	defer ft.mu.RUnlock()

	report := AnalysisReport{
		Summary: ReportSummary{
			TotalEvents:     len(ft.events),
			Correlations:    len(ft.correlations),
			Duration:        time.Since(ft.startTime),
			EventsPerSecond: float64(len(ft.events)) / time.Since(ft.startTime).Seconds(),
		},
		Completeness: ft.computeCompleteness(),
		Patterns:     ft.identifyPatterns(),
		Stats:        ft.computeStats(),
	}

	return json.MarshalIndent(report, "", "  ")
}

// computeStats calculates comprehensive statistics
func (ft *FlowTracer) computeStats() TracingStats {
	return TracingStats{
		TotalEvents:    len(ft.events),
		MissedEvents:   0, // TODO: Implement missed event detection
		Correlations:   len(ft.correlations),
		UniquePatterns: ft.countUniquePatterns(),
		ProcessingTime: time.Since(ft.startTime),
	}
}

// computeCompleteness analyzes trace completeness
func (ft *FlowTracer) computeCompleteness() CompletenessMetrics {
	return CompletenessMetrics{
		EventCoverage:       100.0, // TODO: Calculate actual coverage
		CorrelationAccuracy: ft.calculateCorrelationAccuracy(),
		MissingEvents:       []string{}, // TODO: Identify missing events
		Confidence:          ft.calculateConfidence(),
	}
}

// identifyPatterns finds common event sequences
func (ft *FlowTracer) identifyPatterns() []EventPattern {
	// TODO: Implement pattern recognition
	return []EventPattern{}
}

// Helper methods

func (ft *FlowTracer) countUniquePatterns() int {
	// TODO: Implement pattern counting
	return 0
}

func (ft *FlowTracer) calculateCorrelationAccuracy() float64 {
	if len(ft.events) == 0 {
		return 0.0
	}
	return float64(len(ft.correlations)) / float64(len(ft.events)) * 100.0
}

func (ft *FlowTracer) calculateConfidence() float64 {
	// Simple confidence based on event count and correlations
	if len(ft.events) < 10 {
		return 50.0
	}
	if len(ft.correlations) > len(ft.events)/2 {
		return 90.0
	}
	return 75.0
}
