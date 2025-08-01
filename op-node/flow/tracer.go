package flow

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
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

	// 🚀 NEW: Static code analysis mapping
	staticCodeMap map[string]*StaticEventMapping
}

// StaticEventMapping represents what we learned from static code analysis
type StaticEventMapping struct {
	EventName   string
	Produced    []string // What events this handler emits
	Consumed    []string // What data this handler reads/uses
	HandlerFunc string   // Which function handles this event
	SourceFile  string   // Where the handler is defined
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

	// 🚀 NEW: Data Flow Analysis
	EventData    map[string]interface{} `json:"event_data,omitempty"`    // Actual event content
	ProducedData []string               `json:"produced_data,omitempty"` // What data this event produces
	ConsumedData []string               `json:"consumed_data,omitempty"` // What data this event reads
	StateChanges map[string]interface{} `json:"state_changes,omitempty"` // System state changes
	DataflowID   string                 `json:"dataflow_id,omitempty"`   // Groups related data flows
}

// TracingStats tracks tracer performance and completeness
type TracingStats struct {
	TotalEvents          int
	MissedEvents         int
	Correlations         int
	UniquePatterns       int
	ProcessingTime       time.Duration
	MissingEventMappings int  // 🚨 Count events without static analysis
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

	// 🚀 NEW: Extract actual event data and analyze data flow
	eventData, producedData, consumedData, stateChanges := ft.analyzeEventDataFlow(ev.Event, name)
	dataflowID := ft.generateDataflowID(ev.Event, derivContext)

	// Track emit event
	captured := CapturedEvent{
		EmitContext:  ev.EmitContext,
		DerivContext: derivContext,
		EventName:    ev.Event.String(),
		EmitTime:     emitTime,
		DeriverName:  name,
		// Data flow analysis
		EventData:    eventData,
		ProducedData: producedData,
		ConsumedData: consumedData,
		StateChanges: stateChanges,
		DataflowID:   dataflowID,
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
		TotalEvents:          len(ft.events),
		MissedEvents:         0, // TODO: Implement missed event detection
		Correlations:         len(ft.correlations),
		UniquePatterns:       ft.countUniquePatterns(),
		ProcessingTime:       time.Since(ft.startTime),
		MissingEventMappings: ft.stats.MissingEventMappings, // 🚀 PRESERVE manually tracked value!
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

// 🚀 NEW: Data Flow Analysis Methods

// analyzeEventDataFlow extracts event data and infers producer/consumer relationships
func (ft *FlowTracer) analyzeEventDataFlow(ev event.Event, deriverName string) (
	eventData map[string]interface{},
	producedData []string,
	consumedData []string,
	stateChanges map[string]interface{},
) {
	eventData = make(map[string]interface{})
	producedData = make([]string, 0)
	consumedData = make([]string, 0)
	stateChanges = make(map[string]interface{})

	// Extract event data using reflection and type analysis
	eventData["event_type"] = ev.String()
	eventData["raw_data"] = ft.extractEventFields(ev)

	// Infer data flow patterns based on event name and deriver
	producedData, consumedData = ft.inferDataDependencies(ev.String(), deriverName)

	// Infer state changes based on event type
	stateChanges = ft.inferStateChanges(ev.String(), deriverName)

	return eventData, producedData, consumedData, stateChanges
}

// extractEventFields uses reflection to extract actual event data
func (ft *FlowTracer) extractEventFields(ev event.Event) map[string]interface{} {
	fields := make(map[string]interface{})

	// Use reflection to extract event fields
	// This is a systematic approach - no ML needed!
	eventType := fmt.Sprintf("%T", ev)
	fields["go_type"] = eventType

	// For known event types, extract specific fields
	switch e := ev.(type) {
	case interface{ GetPayload() interface{} }:
		fields["payload"] = e.GetPayload()
	case interface{ GetBlockHash() string }:
		fields["block_hash"] = e.GetBlockHash()
	case interface{ GetBlockNumber() uint64 }:
		fields["block_number"] = e.GetBlockNumber()
	case interface{ GetTxHash() string }:
		fields["tx_hash"] = e.GetTxHash()
	// Add more type assertions as we discover event types
	default:
		// Generic string representation
		fields["string_repr"] = ev.String()
	}

	return fields
}

// inferDataDependencies systematically infers what data each event produces/consumes
func (ft *FlowTracer) inferDataDependencies(eventName, deriverName string) (produced []string, consumed []string) {
	produced = make([]string, 0)
	consumed = make([]string, 0)

	// 🚀 STRICT: Only use pre-analyzed static code mapping - NO GUESSING!
	staticMapping := ft.getStaticCodeMapping(eventName)
	if staticMapping == nil {
		// 🚨 ERROR: Force us to analyze ALL events systematically
		ft.recordMissingEventAnalysis(eventName, deriverName)
		return []string{}, []string{} // Return empty - no guessing allowed!
	}
	
	return staticMapping.Produced, staticMapping.Consumed
}

// inferStateChanges systematically infers how events change system state
func (ft *FlowTracer) inferStateChanges(eventName, deriverName string) map[string]interface{} {
	changes := make(map[string]interface{})

	// Systematic inference based on event patterns
	switch {
	case strings.Contains(eventName, "forkchoice-update"):
		changes["head_changed"] = true
		changes["state_transition"] = "forkchoice_updated"

	case strings.Contains(eventName, "build-start"):
		changes["building"] = true
		changes["state_transition"] = "block_building_started"

	case strings.Contains(eventName, "payload-process"):
		changes["processing"] = true
		changes["state_transition"] = "payload_processing"

	case strings.Contains(eventName, "unsafe-update"):
		changes["unsafe_head_updated"] = true
		changes["state_transition"] = "unsafe_progression"

	case strings.Contains(eventName, "finalized"):
		changes["finalized_head_updated"] = true
		changes["state_transition"] = "finalization"

	case strings.Contains(eventName, "reset"):
		changes["pipeline_reset"] = true
		changes["state_transition"] = "reset_recovery"
	}

	return changes
}

// generateDataflowID creates identifiers to group related data flows
func (ft *FlowTracer) generateDataflowID(ev event.Event, derivContext uint64) string {
	eventName := ev.String()

	// Group events into logical data flows
	switch {
	case strings.Contains(eventName, "forkchoice"):
		return fmt.Sprintf("forkchoice_flow_%d", derivContext/100) // Group related forkchoice events

	case strings.Contains(eventName, "build") || strings.Contains(eventName, "payload"):
		return fmt.Sprintf("block_building_flow_%d", derivContext/100)

	case strings.Contains(eventName, "sequencer"):
		return fmt.Sprintf("sequencer_flow_%d", derivContext/100)

	case strings.Contains(eventName, "unsafe") || strings.Contains(eventName, "finalize"):
		return fmt.Sprintf("progression_flow_%d", derivContext/100)

	default:
		return fmt.Sprintf("general_flow_%d", derivContext/100)
	}
}

// 🚨 NEW: Error tracking for missing event analysis
func (ft *FlowTracer) recordMissingEventAnalysis(eventName, deriverName string) {
	// NOTE: This assumes the caller already holds ft.mu.Lock()
	// Track missing events for systematic analysis coverage
	ft.stats.MissingEventMappings++
	
	// TODO: Later we can log these or store them to guide our static analysis
	// For now, we silently track them in stats
}

// 🚀 NEW: Static Code Analysis Methods

// getStaticCodeMapping returns pre-analyzed producer/consumer data from static code analysis
func (ft *FlowTracer) getStaticCodeMapping(eventName string) *StaticEventMapping {
	if ft.staticCodeMap == nil {
		ft.initializeStaticCodeMap()
	}
	return ft.staticCodeMap[eventName]
}

// initializeStaticCodeMap populates the static analysis results from actual code parsing
func (ft *FlowTracer) initializeStaticCodeMap() {
	ft.staticCodeMap = map[string]*StaticEventMapping{
		// 📚 REAL DATA from payload_process.go analysis
		"payload-process": {
			EventName:   "payload-process",
			HandlerFunc: "onPayloadProcess",
			SourceFile:  "op-node/rollup/engine/payload_process.go",
			// ACTUAL consumed data from code:
			Consumed: []string{
				"ExecutionPayload",      // ev.Envelope.ExecutionPayload
				"ParentBeaconBlockRoot", // ev.Envelope.ParentBeaconBlockRoot
				"engine_client",         // eq.ec.engine
			},
			// ACTUAL produced events from code:
			Produced: []string{
				"PayloadSuccessEvent",       // case eth.ExecutionValid
				"PayloadInvalidEvent",       // case eth.ExecutionInvalid
				"EngineTemporaryErrorEvent", // default case
			},
		},

		// 📚 REAL DATA from payload_success.go analysis
		"payload-success": {
			EventName:   "payload-success",
			HandlerFunc: "onPayloadSuccess",
			SourceFile:  "op-node/rollup/engine/payload_success.go",
			// ACTUAL consumed data from code:
			Consumed: []string{
				"PayloadSuccessEvent", // The input event itself
				"Ref",                 // ev.Ref
				"DerivedFrom",         // ev.DerivedFrom
			},
			// ACTUAL produced events from code:
			Produced: []string{
				"PromoteUnsafeEvent",      // line 50
				"PromotePendingSafeEvent", // line 54
				"TryUpdateEngineEvent",    // line 61
				"ForceResetEvent",         // line 32 (replacement case)
			},
		},

		// 📚 REAL DATA from build_start.go analysis
		"build-start": {
			EventName:   "build-start",
			HandlerFunc: "onBuildStart",
			SourceFile:  "op-node/rollup/engine/build_start.go",
			Consumed: []string{
				"BuildStartEvent",   // The input event
				"PayloadAttributes", // ev.Attributes
				"engine_client",     // eq.ec.engine
			},
			Produced: []string{
				"ForkchoiceUpdateEvent",     // line 71
				"BuildStartedEvent",         // line 73
				"BuildInvalidEvent",         // line 62 (error case)
				"EngineTemporaryErrorEvent", // line 52 (error case)
			},
		},

		// 📚 REAL DATA from build_seal.go analysis
		"build-seal": {
			EventName:   "build-seal",
			HandlerFunc: "onBuildSeal",
			SourceFile:  "op-node/rollup/engine/build_seal.go",
			Consumed: []string{
				"BuildSealEvent", // The input event
				"PayloadID",      // ev.ID
				"engine_client",  // eq.ec.engine
			},
			Produced: []string{
				"BuildSealedEvent",             // line 118 (success)
				"PayloadSealExpiredErrorEvent", // line 74 (timeout)
				"PayloadSealInvalidEvent",      // line 84, 96 (invalid)
			},
		},

		// 📚 REAL DATA from engine_controller.go analysis
		"forkchoice-update": {
			EventName:   "forkchoice-update",
			HandlerFunc: "updateForkchoice",
			SourceFile:  "op-node/rollup/engine/engine_controller.go",
			Consumed: []string{
				"ForkchoiceUpdateEvent", // The input event
				"UnsafeL2Head",          // ev.UnsafeL2Head
				"SafeL2Head",            // ev.SafeL2Head
				"FinalizedL2Head",       // ev.FinalizedL2Head
			},
			Produced: []string{
				"UnsafeUpdateEvent",     // line 424, 455
				"CrossSafeUpdateEvent",  // line 427
				"ForkchoiceUpdateEvent", // line 463, 544 (recursive)
			},
		},
	}
}
