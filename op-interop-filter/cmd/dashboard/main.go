package main

import (
	"bufio"
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/urfave/cli/v2"
)

var (
	FilterMetricsFlag = &cli.StringFlag{
		Name:    "filter-metrics",
		Usage:   "Filter service metrics endpoint",
		Value:   "http://localhost:7300/metrics",
		EnvVars: []string{"FILTER_METRICS"},
	}
	SpammerMetricsFlag = &cli.StringFlag{
		Name:    "spammer-metrics",
		Usage:   "Spammer metrics endpoint",
		Value:   "http://localhost:7301/metrics",
		EnvVars: []string{"SPAMMER_METRICS"},
	}
	RefreshIntervalFlag = &cli.StringFlag{
		Name:    "refresh",
		Usage:   "Refresh interval (e.g., 1s, 2s)",
		Value:   "2s",
		EnvVars: []string{"REFRESH_INTERVAL"},
	}
)

type Metrics struct {
	// Filter metrics
	FilterUp              float64
	FilterFailsafe        float64
	FilterChainReady      map[string]float64
	FilterChainHead       map[string]float64
	FilterBackfillProg    map[string]float64
	FilterCheckSuccess    float64
	FilterCheckFailed     float64
	FilterReorgs          map[string]float64

	// LogsDB metrics
	LogsDBFirstBlock      map[string]float64
	LogsDBBlocksSealed    map[string]float64
	LogsDBLogsAdded       map[string]float64
	LogsDBEntries         map[string]float64

	// Spammer metrics
	SpammerUp             float64
	SpammerValidAccepted  float64
	SpammerValidRejected  float64
	SpammerInvalidAccepted float64
	SpammerInvalidRejected float64
	SpammerErrors         float64
	SpammerLatencyValid   float64
	SpammerLatencyInvalid float64
}

func main() {
	app := cli.NewApp()
	app.Name = "filter-dashboard"
	app.Usage = "Terminal dashboard for op-interop-filter observability"
	app.Flags = []cli.Flag{
		FilterMetricsFlag,
		SpammerMetricsFlag,
		RefreshIntervalFlag,
	}
	app.Action = run

	if err := app.Run(os.Args); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

func run(cliCtx *cli.Context) error {
	filterURL := cliCtx.String(FilterMetricsFlag.Name)
	spammerURL := cliCtx.String(SpammerMetricsFlag.Name)
	refreshStr := cliCtx.String(RefreshIntervalFlag.Name)

	refresh, err := time.ParseDuration(refreshStr)
	if err != nil {
		return fmt.Errorf("invalid refresh interval: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle interrupt
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		cancel()
	}()

	startTime := time.Now()
	ticker := time.NewTicker(refresh)
	defer ticker.Stop()

	// Initial render
	render(filterURL, spammerURL, startTime)

	for {
		select {
		case <-ctx.Done():
			clearScreen()
			fmt.Println("Dashboard stopped.")
			return nil
		case <-ticker.C:
			render(filterURL, spammerURL, startTime)
		}
	}
}

func render(filterURL, spammerURL string, startTime time.Time) {
	clearScreen()

	m := fetchAllMetrics(filterURL, spammerURL)
	uptime := time.Since(startTime).Round(time.Second)

	// Header
	fmt.Println("╔══════════════════════════════════════════════════════════════════════════════╗")
	fmt.Println("║           OP-INTEROP-FILTER DASHBOARD                                        ║")
	fmt.Printf("║  Uptime: %-20s                              %s  ║\n", uptime, time.Now().Format("15:04:05"))
	fmt.Println("╠══════════════════════════════════════════════════════════════════════════════╣")

	// Filter Service Status
	filterStatus := "🔴 DOWN"
	if m.FilterUp > 0 {
		filterStatus = "🟢 UP"
	}
	failsafeStatus := "✅ OK"
	if m.FilterFailsafe > 0 {
		failsafeStatus = "🚨 TRIGGERED"
	}

	fmt.Println("║  FILTER SERVICE                                                              ║")
	fmt.Printf("║    Status: %-12s  Failsafe: %-20s                   ║\n", filterStatus, failsafeStatus)

	// Chain status
	if len(m.FilterChainHead) > 0 {
		for chainID, head := range m.FilterChainHead {
			ready := "⏳"
			if r, ok := m.FilterChainReady[chainID]; ok && r > 0 {
				ready = "✅"
			}
			backfill := 0.0
			if bf, ok := m.FilterBackfillProg[chainID]; ok {
				backfill = bf * 100
			}
			reorgs := 0.0
			if r, ok := m.FilterReorgs[chainID]; ok {
				reorgs = r
			}
			fmt.Printf("║    Chain %s: %s Head=%-10.0f Backfill=%-5.1f%% Reorgs=%.0f             ║\n",
				chainID, ready, head, backfill, reorgs)
		}
	} else {
		fmt.Println("║    No chain data available                                                   ║")
	}

	// Check access list stats
	totalChecks := m.FilterCheckSuccess + m.FilterCheckFailed
	successRate := 0.0
	if totalChecks > 0 {
		successRate = (m.FilterCheckSuccess / totalChecks) * 100
	}
	fmt.Printf("║    Checks: %.0f total (%.1f%% success)                                         ║\n",
		totalChecks, successRate)

	fmt.Println("╠══════════════════════════════════════════════════════════════════════════════╣")

	// LogsDB Stats
	fmt.Println("║  LOGSDB                                                                      ║")
	if len(m.LogsDBBlocksSealed) > 0 {
		for chainID := range m.LogsDBBlocksSealed {
			firstBlock := m.LogsDBFirstBlock[chainID]
			blocksSealed := m.LogsDBBlocksSealed[chainID]
			logsAdded := m.LogsDBLogsAdded[chainID]
			entries := m.LogsDBEntries[chainID]
			fmt.Printf("║    Chain %s: Blocks=%-8.0f Logs=%-10.0f Entries=%-8.0f            ║\n",
				chainID, blocksSealed, logsAdded, entries)
			fmt.Printf("║              First Block=%-10.0f                                       ║\n", firstBlock)
		}
	} else {
		fmt.Println("║    No LogsDB data available                                                  ║")
	}

	fmt.Println("╠══════════════════════════════════════════════════════════════════════════════╣")

	// Spammer Status
	spammerStatus := "🔴 DOWN"
	if m.SpammerUp > 0 {
		spammerStatus = "🟢 UP"
	}

	fmt.Println("║  SPAMMER                                                                     ║")
	fmt.Printf("║    Status: %-12s  Errors: %-5.0f                                       ║\n",
		spammerStatus, m.SpammerErrors)

	// Query stats
	validTotal := m.SpammerValidAccepted + m.SpammerValidRejected
	invalidTotal := m.SpammerInvalidAccepted + m.SpammerInvalidRejected
	allQueries := validTotal + invalidTotal

	fmt.Printf("║    Valid queries:   %-6.0f (accepted: %.0f, rejected: %.0f)                     ║\n",
		validTotal, m.SpammerValidAccepted, m.SpammerValidRejected)
	fmt.Printf("║    Invalid queries: %-6.0f (accepted: %.0f, rejected: %.0f)                     ║\n",
		invalidTotal, m.SpammerInvalidAccepted, m.SpammerInvalidRejected)

	// Calculate correctness
	correct := m.SpammerValidAccepted + m.SpammerInvalidRejected
	correctRate := 0.0
	if allQueries > 0 {
		correctRate = (correct / allQueries) * 100
	}

	fmt.Println("╠══════════════════════════════════════════════════════════════════════════════╣")
	fmt.Println("║  SUMMARY                                                                     ║")
	fmt.Printf("║    Total Queries: %-8.0f  Correctness: %-6.2f%%                             ║\n",
		allQueries, correctRate)

	// Progress bar for correctness
	barWidth := 40
	filled := int(correctRate / 100 * float64(barWidth))
	bar := strings.Repeat("█", filled) + strings.Repeat("░", barWidth-filled)
	fmt.Printf("║    [%s]                        ║\n", bar)

	fmt.Println("╚══════════════════════════════════════════════════════════════════════════════╝")
	fmt.Println("Press Ctrl+C to exit")
}

func clearScreen() {
	fmt.Print("\033[H\033[2J")
}

func fetchAllMetrics(filterURL, spammerURL string) Metrics {
	m := Metrics{
		FilterChainReady:   make(map[string]float64),
		FilterChainHead:    make(map[string]float64),
		FilterBackfillProg: make(map[string]float64),
		FilterReorgs:       make(map[string]float64),
		LogsDBFirstBlock:   make(map[string]float64),
		LogsDBBlocksSealed: make(map[string]float64),
		LogsDBLogsAdded:    make(map[string]float64),
		LogsDBEntries:      make(map[string]float64),
	}

	// Fetch filter metrics
	filterMetrics := fetchMetrics(filterURL)
	m.FilterUp = filterMetrics["op_interop_filter_up"]
	m.FilterFailsafe = filterMetrics["op_interop_filter_failsafe_enabled"]
	m.FilterCheckSuccess = filterMetrics["op_interop_filter_check_access_list_total{success=\"true\"}"]
	m.FilterCheckFailed = filterMetrics["op_interop_filter_check_access_list_total{success=\"false\"}"]

	// Parse chain-specific metrics
	for k, v := range filterMetrics {
		if strings.HasPrefix(k, "op_interop_filter_chain_ready{") {
			chainID := extractLabel(k, "chain_id")
			m.FilterChainReady[chainID] = v
		}
		if strings.HasPrefix(k, "op_interop_filter_chain_head{") {
			chainID := extractLabel(k, "chain_id")
			m.FilterChainHead[chainID] = v
		}
		if strings.HasPrefix(k, "op_interop_filter_backfill_progress{") {
			chainID := extractLabel(k, "chain_id")
			m.FilterBackfillProg[chainID] = v
		}
		if strings.HasPrefix(k, "op_interop_filter_reorg_detected_total{") {
			chainID := extractLabel(k, "chain_id")
			m.FilterReorgs[chainID] = v
		}
		// LogsDB metrics
		if strings.HasPrefix(k, "op_interop_filter_logsdb_first_block{") {
			chainID := extractLabel(k, "chain_id")
			m.LogsDBFirstBlock[chainID] = v
		}
		if strings.HasPrefix(k, "op_interop_filter_logsdb_blocks_sealed_total{") {
			chainID := extractLabel(k, "chain_id")
			m.LogsDBBlocksSealed[chainID] = v
		}
		if strings.HasPrefix(k, "op_interop_filter_logsdb_logs_added_total{") {
			chainID := extractLabel(k, "chain_id")
			m.LogsDBLogsAdded[chainID] = v
		}
		if strings.HasPrefix(k, "op_interop_filter_logsdb_entries{") {
			chainID := extractLabel(k, "chain_id")
			m.LogsDBEntries[chainID] = v
		}
	}

	// Fetch spammer metrics
	spammerMetrics := fetchMetrics(spammerURL)
	m.SpammerUp = spammerMetrics["filter_spammer_up"]
	m.SpammerErrors = spammerMetrics["filter_spammer_errors_total"]
	m.SpammerValidAccepted = spammerMetrics["filter_spammer_queries_total{type=\"valid\",result=\"accepted\"}"]
	m.SpammerValidRejected = spammerMetrics["filter_spammer_queries_total{type=\"valid\",result=\"rejected\"}"]
	m.SpammerInvalidAccepted = spammerMetrics["filter_spammer_queries_total{type=\"invalid\",result=\"accepted\"}"]
	m.SpammerInvalidRejected = spammerMetrics["filter_spammer_queries_total{type=\"invalid\",result=\"rejected\"}"]

	return m
}

func fetchMetrics(url string) map[string]float64 {
	result := make(map[string]float64)

	client := &http.Client{Timeout: 2 * time.Second}
	resp, err := client.Get(url)
	if err != nil {
		return result
	}
	defer resp.Body.Close()

	scanner := bufio.NewScanner(resp.Body)
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "#") || line == "" {
			continue
		}

		// Parse prometheus text format: metric_name{labels} value
		parts := strings.Fields(line)
		if len(parts) < 2 {
			continue
		}

		metricName := parts[0]
		value, err := strconv.ParseFloat(parts[1], 64)
		if err != nil {
			continue
		}

		result[metricName] = value
	}

	return result
}

func extractLabel(metric, labelName string) string {
	// Extract label value from metric like: metric_name{chain_id="123"}
	start := strings.Index(metric, labelName+"=\"")
	if start == -1 {
		return ""
	}
	start += len(labelName) + 2
	end := strings.Index(metric[start:], "\"")
	if end == -1 {
		return ""
	}
	return metric[start : start+end]
}
