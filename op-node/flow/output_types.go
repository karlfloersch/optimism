package flow

import "time"

// FlowTraceOutput represents the complete output of a flow tracing session
type FlowTraceOutput struct {
	Metadata     TraceMetadata     `json:"metadata"`
	Events       []CapturedEvent   `json:"events"`
	Correlations map[uint64]uint64 `json:"correlations"`
	Stats        TracingStats      `json:"stats"`
}

// TraceMetadata contains information about the tracing session
type TraceMetadata struct {
	StartTime     time.Time     `json:"start_time"`
	EndTime       time.Time     `json:"end_time"`
	TotalEvents   int           `json:"total_events"`
	TotalDuration time.Duration `json:"total_duration"`
	Version       string        `json:"version"`
}

// AnalysisReport provides comprehensive analysis of captured flows
type AnalysisReport struct {
	Summary      ReportSummary       `json:"summary"`
	Completeness CompletenessMetrics `json:"completeness"`
	Patterns     []EventPattern      `json:"patterns"`
	Stats        TracingStats        `json:"stats"`
}

// ReportSummary provides high-level metrics
type ReportSummary struct {
	TotalEvents     int           `json:"total_events"`
	Correlations    int           `json:"correlations"`
	Duration        time.Duration `json:"duration"`
	EventsPerSecond float64       `json:"events_per_second"`
}

// CompletenessMetrics tracks how complete our event capture is
type CompletenessMetrics struct {
	EventCoverage       float64  `json:"event_coverage_percent"`
	CorrelationAccuracy float64  `json:"correlation_accuracy_percent"`
	MissingEvents       []string `json:"missing_events"`
	Confidence          float64  `json:"confidence_percent"`
}

// EventPattern represents a discovered sequence of events
type EventPattern struct {
	ID          string   `json:"id"`
	Name        string   `json:"name"`
	Sequence    []string `json:"event_sequence"`
	Frequency   int      `json:"frequency"`
	Confidence  float64  `json:"confidence"`
	Description string   `json:"description"`
}

// FlowAnalysis provides detailed analysis of event flows
type FlowAnalysis struct {
	FlowID      string            `json:"flow_id"`
	StartEvent  string            `json:"start_event"`
	EndEvent    string            `json:"end_event"`
	Steps       []TraceFlowStep   `json:"steps"`
	Duration    time.Duration     `json:"duration"`
	Frequency   int               `json:"frequency"`
	Success     bool              `json:"success"`
	ErrorEvents []string          `json:"error_events,omitempty"`
	Metadata    map[string]string `json:"metadata,omitempty"`
}

// TraceFlowStep represents a single step in a traced flow
type TraceFlowStep struct {
	EventName   string        `json:"event_name"`
	Timestamp   time.Time     `json:"timestamp"`
	Duration    time.Duration `json:"duration"`
	Context     uint64        `json:"context"`
	Success     bool          `json:"success"`
	ErrorReason string        `json:"error_reason,omitempty"`
}

// Performance Metrics

// PerformanceMetrics tracks performance characteristics of flows
type PerformanceMetrics struct {
	AverageLatency   time.Duration        `json:"average_latency"`
	P95Latency       time.Duration        `json:"p95_latency"`
	P99Latency       time.Duration        `json:"p99_latency"`
	Throughput       float64              `json:"throughput_events_per_second"`
	ErrorRate        float64              `json:"error_rate_percent"`
	HottestFlows     []FlowPerformance    `json:"hottest_flows"`
	BottleneckEvents []BottleneckAnalysis `json:"bottleneck_events"`
}

// FlowPerformance tracks performance of specific flows
type FlowPerformance struct {
	FlowName      string        `json:"flow_name"`
	Count         int           `json:"count"`
	AverageTime   time.Duration `json:"average_time"`
	TotalTime     time.Duration `json:"total_time"`
	PercentOfTime float64       `json:"percent_of_total_time"`
}

// BottleneckAnalysis identifies performance bottlenecks
type BottleneckAnalysis struct {
	EventName      string        `json:"event_name"`
	AverageTime    time.Duration `json:"average_time"`
	Count          int           `json:"count"`
	Impact         float64       `json:"impact_score"`
	Recommendation string        `json:"recommendation"`
}
