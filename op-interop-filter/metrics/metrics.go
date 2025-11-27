package metrics

import (
	"strconv"

	"github.com/prometheus/client_golang/prometheus"

	opmetrics "github.com/ethereum-optimism/optimism/op-service/metrics"
)

const Namespace = "op_interop_filter"

type Metricer interface {
	RecordInfo(version string)
	RecordUp()
	RecordFailsafeEnabled(enabled bool)
	RecordChainHead(chainID uint64, blockNum uint64)
	RecordCheckAccessList(success bool)
	RecordReorgDetected(chainID uint64)
	RecordChainReady(chainID uint64, ready bool)
	RecordBackfillProgress(chainID uint64, current, total uint64)
	RecordLogsDBFirstBlock(chainID uint64, blockNum uint64)
	RecordLogsDBBlocksSealed(chainID uint64)
	RecordLogsDBLogsAdded(chainID uint64, count int)
	RecordLogsDBEntries(chainID uint64, count int64)
}

type Metrics struct {
	registry *prometheus.Registry
	factory  opmetrics.Factory

	info             *prometheus.GaugeVec
	up               prometheus.Gauge
	failsafeEnabled  prometheus.Gauge
	chainHead        *prometheus.GaugeVec
	checkAccessTotal *prometheus.CounterVec

	// Chain-specific metrics
	reorgTotal        *prometheus.CounterVec
	chainReady        *prometheus.GaugeVec
	backfillProgress  *prometheus.GaugeVec
	logsDBFirstBlock  *prometheus.GaugeVec
	logsDBBlocksTotal *prometheus.CounterVec
	logsDBLogsTotal   *prometheus.CounterVec
	logsDBEntries     *prometheus.GaugeVec
}

var _ Metricer = (*Metrics)(nil)
var _ opmetrics.RegistryMetricer = (*Metrics)(nil)

func NewMetrics(procName string) *Metrics {
	if procName == "" {
		procName = "default"
	}
	registry := opmetrics.NewRegistry()
	factory := opmetrics.With(registry)

	return &Metrics{
		registry: registry,
		factory:  factory,

		info: factory.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: Namespace,
			Name:      "info",
			Help:      "Service info",
		}, []string{"version"}),

		up: factory.NewGauge(prometheus.GaugeOpts{
			Namespace: Namespace,
			Name:      "up",
			Help:      "1 if service is up",
		}),

		failsafeEnabled: factory.NewGauge(prometheus.GaugeOpts{
			Namespace: Namespace,
			Name:      "failsafe_enabled",
			Help:      "1 if failsafe is enabled",
		}),

		chainHead: factory.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: Namespace,
			Name:      "chain_head",
			Help:      "Latest ingested block number",
		}, []string{"chain_id"}),

		checkAccessTotal: factory.NewCounterVec(prometheus.CounterOpts{
			Namespace: Namespace,
			Name:      "check_access_list_total",
			Help:      "Total checkAccessList requests",
		}, []string{"success"}),

		reorgTotal: factory.NewCounterVec(prometheus.CounterOpts{
			Namespace: Namespace,
			Name:      "reorg_total",
			Help:      "Total reorgs detected",
		}, []string{"chain_id"}),

		chainReady: factory.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: Namespace,
			Name:      "chain_ready",
			Help:      "1 if chain has finished backfill and is ready",
		}, []string{"chain_id"}),

		backfillProgress: factory.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: Namespace,
			Name:      "backfill_progress",
			Help:      "Backfill progress ratio (0-1)",
		}, []string{"chain_id"}),

		logsDBFirstBlock: factory.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: Namespace,
			Name:      "logsdb_first_block",
			Help:      "First block number in LogsDB",
		}, []string{"chain_id"}),

		logsDBBlocksTotal: factory.NewCounterVec(prometheus.CounterOpts{
			Namespace: Namespace,
			Name:      "logsdb_blocks_total",
			Help:      "Total blocks sealed in LogsDB",
		}, []string{"chain_id"}),

		logsDBLogsTotal: factory.NewCounterVec(prometheus.CounterOpts{
			Namespace: Namespace,
			Name:      "logsdb_logs_total",
			Help:      "Total logs added to LogsDB",
		}, []string{"chain_id"}),

		logsDBEntries: factory.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: Namespace,
			Name:      "logsdb_entries",
			Help:      "Current number of entries in LogsDB",
		}, []string{"chain_id"}),
	}
}

func (m *Metrics) Registry() *prometheus.Registry {
	return m.registry
}

func (m *Metrics) Document() []opmetrics.DocumentedMetric {
	return m.factory.Document()
}

func (m *Metrics) RecordInfo(version string) {
	m.info.WithLabelValues(version).Set(1)
}

func (m *Metrics) RecordUp() {
	m.up.Set(1)
}

func (m *Metrics) RecordFailsafeEnabled(enabled bool) {
	if enabled {
		m.failsafeEnabled.Set(1)
	} else {
		m.failsafeEnabled.Set(0)
	}
}

func (m *Metrics) RecordChainHead(chainID uint64, blockNum uint64) {
	m.chainHead.WithLabelValues(strconv.FormatUint(chainID, 10)).Set(float64(blockNum))
}

func (m *Metrics) RecordCheckAccessList(success bool) {
	label := "false"
	if success {
		label = "true"
	}
	m.checkAccessTotal.WithLabelValues(label).Inc()
}

func (m *Metrics) RecordReorgDetected(chainID uint64) {
	m.reorgTotal.WithLabelValues(strconv.FormatUint(chainID, 10)).Inc()
}

func (m *Metrics) RecordChainReady(chainID uint64, ready bool) {
	val := float64(0)
	if ready {
		val = 1
	}
	m.chainReady.WithLabelValues(strconv.FormatUint(chainID, 10)).Set(val)
}

func (m *Metrics) RecordBackfillProgress(chainID uint64, current, total uint64) {
	progress := float64(0)
	if total > 0 {
		progress = float64(current) / float64(total)
	}
	m.backfillProgress.WithLabelValues(strconv.FormatUint(chainID, 10)).Set(progress)
}

func (m *Metrics) RecordLogsDBFirstBlock(chainID uint64, blockNum uint64) {
	m.logsDBFirstBlock.WithLabelValues(strconv.FormatUint(chainID, 10)).Set(float64(blockNum))
}

func (m *Metrics) RecordLogsDBBlocksSealed(chainID uint64) {
	m.logsDBBlocksTotal.WithLabelValues(strconv.FormatUint(chainID, 10)).Inc()
}

func (m *Metrics) RecordLogsDBLogsAdded(chainID uint64, count int) {
	m.logsDBLogsTotal.WithLabelValues(strconv.FormatUint(chainID, 10)).Add(float64(count))
}

func (m *Metrics) RecordLogsDBEntries(chainID uint64, count int64) {
	m.logsDBEntries.WithLabelValues(strconv.FormatUint(chainID, 10)).Set(float64(count))
}

// NoopMetrics is a no-op implementation for testing
var NoopMetrics Metricer = &noopMetrics{}

type noopMetrics struct{}

func (n *noopMetrics) RecordInfo(version string)                                  {}
func (n *noopMetrics) RecordUp()                                                  {}
func (n *noopMetrics) RecordFailsafeEnabled(enabled bool)                         {}
func (n *noopMetrics) RecordChainHead(chainID uint64, blockNum uint64)            {}
func (n *noopMetrics) RecordCheckAccessList(success bool)                         {}
func (n *noopMetrics) RecordReorgDetected(chainID uint64)                         {}
func (n *noopMetrics) RecordChainReady(chainID uint64, ready bool)                {}
func (n *noopMetrics) RecordBackfillProgress(chainID uint64, current, total uint64) {}
func (n *noopMetrics) RecordLogsDBFirstBlock(chainID uint64, blockNum uint64)     {}
func (n *noopMetrics) RecordLogsDBBlocksSealed(chainID uint64)                    {}
func (n *noopMetrics) RecordLogsDBLogsAdded(chainID uint64, count int)            {}
func (n *noopMetrics) RecordLogsDBEntries(chainID uint64, count int64)            {}
