package supervisor

// Minimal no-op metrics adapters to satisfy v1 DB openings.

// logsMetricsNoop implements op-supervisor v1 logs.Metrics
type logsMetricsNoop struct{}

func (logsMetricsNoop) RecordDBEntryCount(kind string, count int64) {}
func (logsMetricsNoop) RecordDBSearchEntriesRead(count int64)       {}

// chainMetricsNoop implements fromda.ChainMetrics
type chainMetricsNoop struct{}

func (chainMetricsNoop) RecordDBEntryCount(kind string, count int64) {}
