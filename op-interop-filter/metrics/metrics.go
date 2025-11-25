package metrics

import (
	"fmt"

	"github.com/prometheus/client_golang/prometheus"

	opmetrics "github.com/ethereum-optimism/optimism/op-service/metrics"
)

const Namespace = "op_interop_filter"

type Metricer interface {
	RecordInfo(version string)
	RecordUp()
	RecordFailsafeEnabled(enabled bool)
	RecordChainReady(chainID uint64, ready bool)
	RecordChainHead(chainID uint64, blockNum uint64)
	RecordBackfillProgress(chainID uint64, current, total uint64)
	RecordCheckAccessList(success bool)
	RecordReorgDetected(chainID uint64)
}

type Metrics struct {
	registry *prometheus.Registry
	factory  opmetrics.Factory

	info              *prometheus.GaugeVec
	up                prometheus.Gauge
	failsafeEnabled   prometheus.Gauge
	chainReady        *prometheus.GaugeVec
	chainHead         *prometheus.GaugeVec
	backfillProgress  *prometheus.GaugeVec
	checkAccessTotal  *prometheus.CounterVec
	reorgDetected     *prometheus.CounterVec
}

var _ Metricer = (*Metrics)(nil)
var _ opmetrics.RegistryMetricer = (*Metrics)(nil)

func NewMetrics() *Metrics {
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

		chainReady: factory.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: Namespace,
			Name:      "chain_ready",
			Help:      "1 if chain has finished backfill",
		}, []string{"chain_id"}),

		chainHead: factory.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: Namespace,
			Name:      "chain_head",
			Help:      "Latest ingested block number",
		}, []string{"chain_id"}),

		backfillProgress: factory.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: Namespace,
			Name:      "backfill_progress",
			Help:      "Backfill progress (0.0 to 1.0)",
		}, []string{"chain_id"}),

		checkAccessTotal: factory.NewCounterVec(prometheus.CounterOpts{
			Namespace: Namespace,
			Name:      "check_access_list_total",
			Help:      "Total checkAccessList requests",
		}, []string{"success"}),

		reorgDetected: factory.NewCounterVec(prometheus.CounterOpts{
			Namespace: Namespace,
			Name:      "reorg_detected_total",
			Help:      "Number of reorgs detected",
		}, []string{"chain_id"}),
	}
}

func (m *Metrics) Registry() *prometheus.Registry {
	return m.registry
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

func (m *Metrics) RecordChainReady(chainID uint64, ready bool) {
	val := float64(0)
	if ready {
		val = 1
	}
	m.chainReady.WithLabelValues(chainIDLabel(chainID)).Set(val)
}

func (m *Metrics) RecordChainHead(chainID uint64, blockNum uint64) {
	m.chainHead.WithLabelValues(chainIDLabel(chainID)).Set(float64(blockNum))
}

func (m *Metrics) RecordBackfillProgress(chainID uint64, current, total uint64) {
	progress := float64(0)
	if total > 0 {
		progress = float64(current) / float64(total)
	}
	m.backfillProgress.WithLabelValues(chainIDLabel(chainID)).Set(progress)
}

func (m *Metrics) RecordCheckAccessList(success bool) {
	label := "false"
	if success {
		label = "true"
	}
	m.checkAccessTotal.WithLabelValues(label).Inc()
}

func (m *Metrics) RecordReorgDetected(chainID uint64) {
	m.reorgDetected.WithLabelValues(chainIDLabel(chainID)).Inc()
}

func chainIDLabel(chainID uint64) string {
	return fmt.Sprintf("%d", chainID)
}

// NoopMetrics is a no-op implementation
var NoopMetrics Metricer = &noopMetrics{}

type noopMetrics struct{}

func (n *noopMetrics) RecordInfo(version string)                              {}
func (n *noopMetrics) RecordUp()                                               {}
func (n *noopMetrics) RecordFailsafeEnabled(enabled bool)                      {}
func (n *noopMetrics) RecordChainReady(chainID uint64, ready bool)             {}
func (n *noopMetrics) RecordChainHead(chainID uint64, blockNum uint64)         {}
func (n *noopMetrics) RecordBackfillProgress(chainID uint64, current, total uint64) {}
func (n *noopMetrics) RecordCheckAccessList(success bool)                      {}
func (n *noopMetrics) RecordReorgDetected(chainID uint64)                      {}
