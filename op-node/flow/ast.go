package flow

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"
)

// FlowNode represents a single point in the control flow
type FlowNode interface {
	ID() string
	Type() FlowNodeType
	Execute(ctx context.Context, state *FlowState) (*FlowResult, error)
}

type FlowNodeType int

const (
	EventTrigger FlowNodeType = iota // Entry point: specific event type
	StateCheck                       // Conditional: check system state
	ActionNode                       // Effect: emit event, call function
	DecisionNode                     // Branch: multiple possible paths
	SyncPoint                        // Wait: synchronization barrier
	TerminalNode                     // Exit: end of flow
)

func (t FlowNodeType) String() string {
	switch t {
	case EventTrigger:
		return "trigger"
	case StateCheck:
		return "state-check"
	case ActionNode:
		return "action"
	case DecisionNode:
		return "decision"
	case SyncPoint:
		return "sync"
	case TerminalNode:
		return "terminal"
	default:
		return "unknown"
	}
}

// FlowEdge represents transitions between nodes
type FlowEdge struct {
	From      string         `json:"from"`      // source node ID
	To        string         `json:"to"`        // target node ID
	Condition *ConditionExpr `json:"condition"` // when this transition is valid
	Weight    float64        `json:"weight"`    // probability/priority of this edge
	Metadata  map[string]any `json:"metadata"`  // debugging info, timing, etc.
}

// FlowGraph represents a complete control flow
type FlowGraph struct {
	ID          string              `json:"id"`
	Name        string              `json:"name"`
	EntryPoints []string            `json:"entry_points"` // event types that can trigger this flow
	Nodes       map[string]FlowNode `json:"nodes"`
	Edges       []FlowEdge          `json:"edges"`

	// Learning data
	ObservedPaths  [][]string     `json:"observed_paths"`  // actual execution paths seen
	StateSnapshots map[string]any `json:"state_snapshots"` // system state at key points
	Metrics        *FlowMetrics   `json:"metrics"`         // timing, frequency, success rates
}

// FlowMetrics tracks performance and reliability of flows
type FlowMetrics struct {
	ExecutionCount  int           `json:"execution_count"`
	SuccessRate     float64       `json:"success_rate"`
	AverageDuration time.Duration `json:"average_duration"`
	ErrorPatterns   []string      `json:"error_patterns"`
	BottleneckNodes []string      `json:"bottleneck_nodes"`
}

// ConditionExpr represents decision logic
type ConditionExpr interface {
	Evaluate(ctx context.Context, state *FlowState) (bool, error)
	String() string // human-readable representation
}

// StateCondition checks system state
type StateCondition struct {
	Path     string `json:"path"`     // e.g., "engine.buildInProgress"
	Operator string `json:"operator"` // "==", "!=", ">", etc.
	Value    any    `json:"value"`    // expected value
}

func (sc StateCondition) Evaluate(ctx context.Context, state *FlowState) (bool, error) {
	// TODO: Implement state evaluation logic
	panic("Phase 2: Implement condition evaluation")
}

func (sc StateCondition) String() string {
	return sc.Path + " " + sc.Operator + " " + toString(sc.Value)
}

// EventCondition checks event properties
type EventCondition struct {
	Type  string `json:"type"`  // event type name
	Field string `json:"field"` // field to check
	Value any    `json:"value"` // expected value
}

func (ec EventCondition) Evaluate(ctx context.Context, state *FlowState) (bool, error) {
	// TODO: Implement event condition evaluation
	panic("Phase 2: Implement condition evaluation")
}

func (ec EventCondition) String() string {
	return ec.Type + "." + ec.Field + " == " + toString(ec.Value)
}

// CompositeCondition combines multiple conditions
type CompositeCondition struct {
	Operator string          `json:"operator"` // "AND", "OR", "NOT"
	Children []ConditionExpr `json:"children"`
}

func (cc CompositeCondition) Evaluate(ctx context.Context, state *FlowState) (bool, error) {
	// TODO: Implement composite condition evaluation
	panic("Phase 2: Implement condition evaluation")
}

func (cc CompositeCondition) String() string {
	// TODO: Implement string representation
	return "(" + cc.Operator + " ...)"
}

// FlowState represents the execution state during flow traversal
type FlowState struct {
	// System state snapshot
	EngineState  map[string]any `json:"engine_state"`
	DeriverState map[string]any `json:"deriver_state"`
	P2PState     map[string]any `json:"p2p_state"`

	// Flow-specific state
	Variables    map[string]any `json:"variables"`
	Checkpoints  []string       `json:"checkpoints"`
	CurrentEvent string         `json:"current_event"`
}

// FlowResult represents the outcome of executing a flow node
type FlowResult struct {
	Success      bool           `json:"success"`
	NextNodes    []string       `json:"next_nodes"`
	StateChanges map[string]any `json:"state_changes"`
	Events       []string       `json:"events"` // events to emit
	Duration     time.Duration  `json:"duration"`
}

// FlowExecution tracks a single flow execution instance
type FlowExecution struct {
	FlowID      string          `json:"flow_id"`
	CurrentNode string          `json:"current_node"`
	State       *FlowState      `json:"state"`
	History     []FlowStep      `json:"history"`
	StartTime   time.Time       `json:"start_time"`
	Context     context.Context `json:"-"` // Don't serialize context
}

// FlowStep represents one step in flow execution
type FlowStep struct {
	NodeID        string         `json:"node_id"`
	Event         string         `json:"event"` // event name that triggered this step
	StateSnapshot map[string]any `json:"state_snapshot"`
	Duration      time.Duration  `json:"duration"`
	Decision      string         `json:"decision"` // which edge was taken and why
	Timestamp     time.Time      `json:"timestamp"`
}

// Basic node implementations

// TriggerNode represents an event trigger entry point
type TriggerNode struct {
	NodeID    string         `json:"id"`
	EventType string         `json:"event_type"`
	Metadata  map[string]any `json:"metadata"`
}

func (tn TriggerNode) ID() string         { return tn.NodeID }
func (tn TriggerNode) Type() FlowNodeType { return EventTrigger }
func (tn TriggerNode) Execute(ctx context.Context, state *FlowState) (*FlowResult, error) {
	// TODO: Implement trigger execution
	panic("Phase 2: Implement node execution")
}

// ActionNodeImpl represents an action in the flow
type ActionNodeImpl struct {
	NodeID     string         `json:"id"`
	ActionType string         `json:"action_type"` // "emit", "call", "wait", etc.
	Parameters map[string]any `json:"parameters"`
}

func (an ActionNodeImpl) ID() string         { return an.NodeID }
func (an ActionNodeImpl) Type() FlowNodeType { return ActionNode }
func (an ActionNodeImpl) Execute(ctx context.Context, state *FlowState) (*FlowResult, error) {
	// TODO: Implement action execution
	panic("Phase 2: Implement node execution")
}

// EventTriggerNode implements FlowNode for event triggers
type EventTriggerNode struct {
	id       string
	nodeType FlowNodeType
	metadata map[string]any
}

func (n *EventTriggerNode) ID() string         { return n.id }
func (n *EventTriggerNode) Type() FlowNodeType { return n.nodeType }
func (n *EventTriggerNode) Execute(ctx context.Context, state *FlowState) (*FlowResult, error) {
	// TODO: Phase 4 - implement execution
	return &FlowResult{Success: true}, nil
}

// FlowPattern represents a recurring pattern in event flows
type FlowPattern struct {
	Name        string   `json:"name"`
	Sequence    []string `json:"sequence"`
	Frequency   int      `json:"frequency"`
	Probability float64  `json:"probability"`
	Description string   `json:"description"`
}

// Helper functions

func toString(v any) string {
	// TODO: Better string conversion
	return "TODO"
}

// FlowBuilder constructs AST from traced events (Phase 2)
type FlowBuilder struct {
	events       []CapturedEvent
	correlations map[uint64]uint64
}

func NewFlowBuilder(events []CapturedEvent, correlations map[uint64]uint64) *FlowBuilder {
	return &FlowBuilder{
		events:       events,
		correlations: correlations,
	}
}

func (fb *FlowBuilder) BuildAST() (*FlowGraph, error) {
	// Phase 1: Group events by trigger type and build node library
	nodeMap := make(map[string]FlowNode)
	eventGroups := fb.groupEventsByType()

	// Create nodes from unique event types
	for eventType, events := range eventGroups {
		node := &EventTriggerNode{
			id:       eventType,
			nodeType: fb.inferNodeType(eventType, events),
			metadata: map[string]any{
				"frequency":    len(events),
				"avg_duration": fb.calculateAvgDuration(events),
			},
		}
		nodeMap[eventType] = node
	}

	// Phase 2: Build REAL data flow edges (not just temporal)
	edges := fb.buildDataFlowEdges(nodeMap)

	// Phase 3: Identify patterns and flows
	patterns := fb.identifyCommonPatterns()
	observedPaths := fb.extractObservedPaths()

	// Phase 4: Build complete flow graph
	graph := &FlowGraph{
		ID:             fmt.Sprintf("flow_%d", len(fb.events)),
		Name:           "Generated Flow Graph",
		EntryPoints:    []string{fb.findMostCommonStartEvent(eventGroups)},
		Nodes:          nodeMap,
		Edges:          edges,
		ObservedPaths:  observedPaths,
		StateSnapshots: make(map[string]any),
		Metrics: &FlowMetrics{
			ExecutionCount:  len(patterns),
			SuccessRate:     0.95, // TODO: calculate from actual data
			AverageDuration: fb.calculateOverallAvgDuration(),
			ErrorPatterns:   []string{}, // TODO: extract error patterns
			BottleneckNodes: fb.findBottleneckNodes(eventGroups),
		},
	}

	return graph, nil
}

// Helper methods for AST building

func (fb *FlowBuilder) groupEventsByType() map[string][]CapturedEvent {
	groups := make(map[string][]CapturedEvent)
	for _, event := range fb.events {
		groups[event.EventName] = append(groups[event.EventName], event)
	}
	return groups
}

func (fb *FlowBuilder) inferNodeType(eventType string, events []CapturedEvent) FlowNodeType {
	// Infer node type based on event name patterns and behavior
	name := eventType

	// Look for common patterns in event naming
	if strings.Contains(name, "start") || strings.Contains(name, "init") {
		return EventTrigger
	}
	if strings.Contains(name, "check") || strings.Contains(name, "validate") {
		return StateCheck
	}
	if strings.Contains(name, "update") || strings.Contains(name, "process") {
		return ActionNode
	}
	if strings.Contains(name, "fork") || strings.Contains(name, "branch") {
		return DecisionNode
	}
	if strings.Contains(name, "sync") || strings.Contains(name, "wait") {
		return SyncPoint
	}
	if strings.Contains(name, "complete") || strings.Contains(name, "finish") {
		return TerminalNode
	}

	// Default to ActionNode for processing events
	return ActionNode
}

func (fb *FlowBuilder) calculateAvgDuration(events []CapturedEvent) time.Duration {
	if len(events) == 0 {
		return 0
	}

	var total time.Duration
	for _, event := range events {
		total += event.Duration
	}

	return total / time.Duration(len(events))
}

func (fb *FlowBuilder) buildEdgesFromSequences(nodeMap map[string]FlowNode) []FlowEdge {
	edges := make([]FlowEdge, 0)
	edgeFreq := make(map[string]int) // track frequency of transitions

	// Build temporal sequences from correlated events
	sequences := fb.buildEventSequences()

	// Create edges from sequential events
	for _, sequence := range sequences {
		for i := 0; i < len(sequence)-1; i++ {
			from := sequence[i]
			to := sequence[i+1]

			// Track edge frequency
			edgeKey := from + "->" + to
			edgeFreq[edgeKey]++
		}
	}

	// Convert frequency map to actual edges
	for edgeKey, frequency := range edgeFreq {
		parts := strings.Split(edgeKey, "->")
		if len(parts) != 2 {
			continue
		}

		// Verify nodes exist
		_, fromExists := nodeMap[parts[0]]
		_, toExists := nodeMap[parts[1]]
		if !fromExists || !toExists {
			continue
		}

		edge := FlowEdge{
			From:      parts[0], // FlowEdge uses node IDs, not node objects
			To:        parts[1],
			Condition: nil,                // TODO: Phase 3 - infer conditions
			Weight:    float64(frequency), // Convert to float64
			Metadata: map[string]any{
				"frequency": frequency,
				"edge_type": "temporal_sequence",
			},
		}

		edges = append(edges, edge)
	}

	return edges
}

// 🚀 NEW: Build edges based on ACTUAL data flow dependencies (producer → consumer)
func (fb *FlowBuilder) buildDataFlowEdges(nodeMap map[string]FlowNode) []FlowEdge {
	edges := make([]FlowEdge, 0)
	edgeFreq := make(map[string]int)

	// Build producer-consumer mappings from our captured data
	producerMap := make(map[string][]CapturedEvent) // data_type -> events that produce it
	consumerMap := make(map[string][]CapturedEvent) // data_type -> events that consume it

	// Phase 1: Index all producers and consumers
	for _, event := range fb.events {
		// Index producers
		for _, produced := range event.ProducedData {
			producerMap[produced] = append(producerMap[produced], event)
		}

		// Index consumers
		for _, consumed := range event.ConsumedData {
			consumerMap[consumed] = append(consumerMap[consumed], event)
		}
	}

	// Phase 2: Connect producers to consumers through data dependencies
	for dataType, producers := range producerMap {
		consumers, hasConsumers := consumerMap[dataType]
		if !hasConsumers {
			continue // No consumers for this data type
		}

		// Create edges from each producer type to each consumer type
		for _, producer := range producers {
			for _, consumer := range consumers {
				// Don't connect event to itself
				if producer.EventName == consumer.EventName {
					continue
				}

				// Verify nodes exist in our node map
				_, producerExists := nodeMap[producer.EventName]
				_, consumerExists := nodeMap[consumer.EventName]
				if !producerExists || !consumerExists {
					continue
				}

				// Track frequency of this data dependency
				edgeKey := producer.EventName + "--[" + dataType + "]-->" + consumer.EventName
				edgeFreq[edgeKey]++
			}
		}
	}

	// Phase 3: Convert to actual edges with data flow metadata
	for edgeKey, frequency := range edgeFreq {
		parts := strings.Split(edgeKey, "--[")
		if len(parts) != 2 {
			continue
		}

		fromEvent := parts[0]
		rest := strings.Split(parts[1], "]-->")
		if len(rest) != 2 {
			continue
		}

		dataType := rest[0]
		toEvent := rest[1]

		edge := FlowEdge{
			From:      fromEvent,
			To:        toEvent,
			Condition: nil, // TODO: infer conditions from state changes
			Weight:    float64(frequency),
			Metadata: map[string]any{
				"edge_type":   "data_dependency", // ← REAL DATA FLOW!
				"data_type":   dataType,          // What data connects them
				"frequency":   frequency,         // How often this dependency occurs
				"causal_link": true,              // This is a causal relationship
			},
		}

		edges = append(edges, edge)
	}

	// Phase 4: Add temporal edges for events in same dataflow
	temporalEdges := fb.buildTemporalEdgesWithinFlows(nodeMap)
	edges = append(edges, temporalEdges...)

	return edges
}

// Build temporal edges within the same dataflow (complement to data dependency edges)
func (fb *FlowBuilder) buildTemporalEdgesWithinFlows(nodeMap map[string]FlowNode) []FlowEdge {
	edges := make([]FlowEdge, 0)
	edgeFreq := make(map[string]int)

	// Group events by dataflow ID
	flowGroups := make(map[string][]CapturedEvent)
	for _, event := range fb.events {
		if event.DataflowID != "" {
			flowGroups[event.DataflowID] = append(flowGroups[event.DataflowID], event)
		}
	}

	// Build temporal sequences within each flow
	for flowID, events := range flowGroups {
		if len(events) < 2 {
			continue
		}

		// Sort by time within this flow
		sort.Slice(events, func(i, j int) bool {
			return events[i].EmitTime.Before(events[j].EmitTime)
		})

		// Connect sequential events within the same logical flow
		for i := 0; i < len(events)-1; i++ {
			from := events[i].EventName
			to := events[i+1].EventName

			// Only connect if nodes exist and aren't the same
			_, fromExists := nodeMap[from]
			_, toExists := nodeMap[to]
			if !fromExists || !toExists || from == to {
				continue
			}

			edgeKey := from + "--[" + flowID + "]-->" + to
			edgeFreq[edgeKey]++
		}
	}

	// Convert to edges
	for edgeKey, frequency := range edgeFreq {
		parts := strings.Split(edgeKey, "--[")
		if len(parts) != 2 {
			continue
		}

		fromEvent := parts[0]
		rest := strings.Split(parts[1], "]-->")
		if len(rest) != 2 {
			continue
		}

		flowID := rest[0]
		toEvent := rest[1]

		edge := FlowEdge{
			From:      fromEvent,
			To:        toEvent,
			Condition: nil,
			Weight:    float64(frequency) * 0.5, // Lower weight than data dependencies
			Metadata: map[string]any{
				"edge_type":   "temporal_within_flow", // Temporal but within logical flows
				"flow_id":     flowID,
				"frequency":   frequency,
				"causal_link": false, // This is temporal ordering, not causal
			},
		}

		edges = append(edges, edge)
	}

	return edges
}

func (fb *FlowBuilder) buildEventSequences() [][]string {
	sequences := make([][]string, 0)

	// Group events by correlation context to build sequences
	contextGroups := make(map[uint64][]CapturedEvent)
	for _, event := range fb.events {
		if event.DerivContext != 0 {
			contextGroups[event.DerivContext] = append(contextGroups[event.DerivContext], event)
		}
	}

	// Sort each context group by time and extract event names
	for _, events := range contextGroups {
		if len(events) < 2 {
			continue // Need at least 2 events for a sequence
		}

		// Sort by emit time
		sort.Slice(events, func(i, j int) bool {
			return events[i].EmitTime.Before(events[j].EmitTime)
		})

		// Extract event names as sequence
		sequence := make([]string, len(events))
		for i, event := range events {
			sequence[i] = event.EventName
		}

		sequences = append(sequences, sequence)
	}

	return sequences
}

func (fb *FlowBuilder) identifyCommonPatterns() []FlowPattern {
	patterns := make([]FlowPattern, 0)
	sequences := fb.buildEventSequences()

	// Find common subsequences (patterns)
	patternCounts := make(map[string]int)

	// Look for patterns of length 2-4
	for _, sequence := range sequences {
		for length := 2; length <= 4 && length <= len(sequence); length++ {
			for start := 0; start <= len(sequence)-length; start++ {
				pattern := strings.Join(sequence[start:start+length], " → ")
				patternCounts[pattern]++
			}
		}
	}

	// Convert to FlowPattern objects, filtering for significant patterns
	for pattern, count := range patternCounts {
		if count >= 3 { // Only include patterns that occur at least 3 times
			patterns = append(patterns, FlowPattern{
				Name:        fmt.Sprintf("Pattern_%d", len(patterns)+1),
				Sequence:    strings.Split(pattern, " → "),
				Frequency:   count,
				Probability: float64(count) / float64(len(sequences)),
				Description: fmt.Sprintf("Common sequence: %s", pattern),
			})
		}
	}

	// Sort by frequency (most common first)
	sort.Slice(patterns, func(i, j int) bool {
		return patterns[i].Frequency > patterns[j].Frequency
	})

	return patterns
}

func (fb *FlowBuilder) findMostCommonStartEvent(eventGroups map[string][]CapturedEvent) string {
	// Find the event type that most commonly starts sequences
	startEventCounts := make(map[string]int)

	for _, sequence := range fb.buildEventSequences() {
		if len(sequence) > 0 {
			startEventCounts[sequence[0]]++
		}
	}

	// Find the most common start event
	maxCount := 0
	mostCommon := ""
	for eventType, count := range startEventCounts {
		if count > maxCount {
			maxCount = count
			mostCommon = eventType
		}
	}

	return mostCommon
}

// Additional helper methods for BuildAST

func (fb *FlowBuilder) extractObservedPaths() [][]string {
	// Extract the actual execution paths observed
	return fb.buildEventSequences()
}

func (fb *FlowBuilder) calculateOverallAvgDuration() time.Duration {
	if len(fb.events) == 0 {
		return 0
	}

	var total time.Duration
	for _, event := range fb.events {
		total += event.Duration
	}

	return total / time.Duration(len(fb.events))
}

func (fb *FlowBuilder) findBottleneckNodes(eventGroups map[string][]CapturedEvent) []string {
	bottlenecks := make([]string, 0)

	// Find event types with highest average duration
	type nodePerf struct {
		eventType   string
		avgDuration time.Duration
	}

	perfs := make([]nodePerf, 0)
	for eventType, events := range eventGroups {
		avg := fb.calculateAvgDuration(events)
		perfs = append(perfs, nodePerf{eventType, avg})
	}

	// Sort by duration (highest first)
	sort.Slice(perfs, func(i, j int) bool {
		return perfs[i].avgDuration > perfs[j].avgDuration
	})

	// Take top 20% as bottlenecks
	count := len(perfs) / 5
	if count < 1 {
		count = 1
	}
	if count > len(perfs) {
		count = len(perfs)
	}

	for i := 0; i < count; i++ {
		bottlenecks = append(bottlenecks, perfs[i].eventType)
	}

	return bottlenecks
}
