package supervisor

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"path/filepath"

	ethTypes "github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/log"

	"github.com/ethereum-optimism/optimism/op-node/rollup"
	"github.com/ethereum-optimism/optimism/op-node/rollup/derive"

	opclient "github.com/ethereum-optimism/optimism/op-service/client"
	"github.com/ethereum-optimism/optimism/op-service/eth"
	"github.com/ethereum-optimism/optimism/op-service/sources"
	logsdb "github.com/ethereum-optimism/optimism/op-supervisor/supervisor/backend/db/logs"
	"github.com/ethereum-optimism/optimism/op-supervisor/supervisor/backend/processors"
	"github.com/ethereum-optimism/optimism/op-supervisor/supervisor/types"
)

type Supervisor struct {
	log log.Logger
	mu  sync.Mutex

	// if true, HTTP handler exposes an /opnode/ reverse proxy to the virtual op-node user RPC
	enableOpNodeProxy bool

	// if true, supervisor_checkAccessList is enabled; if false, returns success without validation
	checkAccessListEnabled bool

	// denylist
	denylist *DenylistStore

	chains   map[uint64]*ChainContainer
	chainsMu sync.Mutex

	// L1 scope label used for cross-safe L1 confirmation depth gating (default: eth.Safe; tests may override to eth.Unsafe)
	l1ScopeLabel eth.BlockLabel

	// per-instance data directory for DBs
	dataDir string

	// crossSafeHistory is the global cross-safe timestamp history (monotonic, non-decreasing)
	crossSafeHistory []crossSafeMD

	// crossSafeHistoryFile is the path to the file where crossSafeHistory is persisted
	crossSafeHistoryFile string

	// testability hooks
	fetchSyncStatus func(ctx context.Context, rpc string) (*eth.SyncStatus, error)
	rollbackFn      func(ctx context.Context, chainID uint64, toBlock uint64) error

	// cache to avoid excessive access to Execution Engines
	// this would not be in production, a block builder would have a better, dedicated cache
	checkAccessListCache checkAccessListCache
}

// crossSafeMD contains metadata for a cross-safe timestamp entry
type crossSafeMD struct {
	Timestamp uint64                               `json:"timestamp"`
	L1Block   eth.BlockRef                         `json:"l1_block"`  // Latest L1 block (highest number)
	L2Blocks  map[uint64]types.DerivedBlockRefPair `json:"l2_blocks"` // chainID -> L2 block pair
}

// ============================================================================
// Package-Level Functions & Constructor
// ============================================================================

// defaultScopeLabel returns the default L1 scope label
// it can be overridden via env SV2_L1_SCOPE
func defaultScopeLabel() eth.BlockLabel {
	switch strings.ToLower(os.Getenv("SV2_L1_SCOPE")) {
	case "unsafe":
		return eth.Unsafe
	case "safe":
		return eth.Safe
	case "finalized":
		return eth.Finalized
	}
	return eth.Safe
}

func NewSupervisor(l log.Logger) *Supervisor {
	s := &Supervisor{log: l.New("service", "supervisor_v2")}
	// initialize shared linker state
	s.l1ScopeLabel = defaultScopeLabel()
	s.checkAccessListEnabled = true
	s.checkAccessListCache = make(checkAccessListCache)

	// default fetcher dials the op-node and returns SyncStatus
	s.fetchSyncStatus = func(ctx context.Context, rpc string) (*eth.SyncStatus, error) {
		cli, err := opclient.NewRPC(ctx, s.log, rpc)
		if err != nil {
			return nil, err
		}
		defer cli.Close()
		roll := sources.NewRollupClient(cli)
		return roll.SyncStatus(ctx)
	}

	// rollback indirection for tests
	s.rollbackFn = s.RollbackChain

	// unique temp dir per instance (can be overridden via SetDataDir or CLI)
	s.dataDir = fmt.Sprintf("%s/sv2-%d-%d", os.TempDir(), os.Getpid(), time.Now().UnixNano())
	// initialize denylist under data dir by default
	s.denylist = NewDenylistStore(filepath.Join(s.dataDir, "denylist.json"), s.log)
	// initialize cross-safe history file path
	s.crossSafeHistoryFile = filepath.Join(s.dataDir, "crossSafeHistory.json")
	// load existing cross-safe history if available
	if err := s.loadCrossSafeHistory(); err != nil {
		s.log.Warn("Failed to load cross-safe history", "function", "NewSupervisor", "error", err)
	}
	go s.ProgressCrossSafe()
	return s
}

// blockAtTimestampFromConfig returns the L2 block number whose timestamp is <= ts, using rollup config.
// It clamps to genesis and accounts for non-zero genesis block numbers.
func blockAtTimestampFromConfig(rcfg *rollup.Config, ts uint64) (uint64, error) {
	if rcfg == nil {
		return 0, fmt.Errorf("nil rollup config")
	}
	if rcfg.BlockTime == 0 {
		return 0, fmt.Errorf("blockTime must be a positive integer")
	}
	genesisTime := rcfg.Genesis.L2Time
	genesisNum := rcfg.Genesis.L2.Number
	if ts <= genesisTime {
		return genesisNum, nil
	}
	return genesisNum + ((ts - genesisTime) / rcfg.BlockTime), nil
}

// ============================================================================
// Cross-Safe History Management
// ============================================================================

// getCurrentCrossSafeTimestamp returns the latest cross-safe timestamp, or 0 if none exists
func (s *Supervisor) getCurrentCrossSafeTimestamp() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.crossSafeHistory) == 0 {
		return 0
	}
	return s.crossSafeHistory[len(s.crossSafeHistory)-1].Timestamp
}

// getLatestCrossSafe returns the latest cross-safe entry, or nil if none exists
func (s *Supervisor) getLatestCrossSafe() *crossSafeMD {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.crossSafeHistory) == 0 {
		return nil
	}
	return &s.crossSafeHistory[len(s.crossSafeHistory)-1]
}

// addCrossSafeEntry adds a new cross-safe entry to the history
func (s *Supervisor) addCrossSafeEntry(entry crossSafeMD) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.crossSafeHistory = append(s.crossSafeHistory, entry)
	if err := s.saveCrossSafeHistory(); err != nil {
		s.log.Warn("Failed to save cross-safe history", "function", "addCrossSafeEntry", "error", err)
	}
}

// setCrossSafeTimestamp sets the cross-safe history to a single entry with the given timestamp (for initialization)
func (s *Supervisor) setCrossSafeTimestamp(timestamp uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.crossSafeHistory = []crossSafeMD{{Timestamp: timestamp}}
	if err := s.saveCrossSafeHistory(); err != nil {
		s.log.Warn("Failed to save cross-safe history", "function", "setCrossSafeTimestamp", "error", err)
	}
}

// pruneLatestCrossSafeEntry removes the latest entry from crossSafeHistory and returns the new latest timestamp
func (s *Supervisor) pruneLatestCrossSafeEntry() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.crossSafeHistory) == 0 {
		return 0
	}
	if len(s.crossSafeHistory) == 1 {
		// If only one entry, clear the history
		s.crossSafeHistory = nil
		if err := s.saveCrossSafeHistory(); err != nil {
			s.log.Warn("Failed to save cross-safe history", "function", "pruneLatestCrossSafeEntry", "error", err)
		}
		return 0
	}
	// Remove the last entry
	s.crossSafeHistory = s.crossSafeHistory[:len(s.crossSafeHistory)-1]
	if err := s.saveCrossSafeHistory(); err != nil {
		s.log.Warn("Failed to save cross-safe history", "function", "pruneLatestCrossSafeEntry", "error", err)
	}
	// Return the new latest timestamp
	return s.crossSafeHistory[len(s.crossSafeHistory)-1].Timestamp
}

// loadCrossSafeHistory loads the cross-safe history from the persistent file
func (s *Supervisor) loadCrossSafeHistory() error {
	if s.crossSafeHistoryFile == "" {
		return nil // no file configured
	}

	data, err := os.ReadFile(s.crossSafeHistoryFile)
	if err != nil {
		if os.IsNotExist(err) {
			// File doesn't exist yet, start with empty history
			s.crossSafeHistory = nil
			return nil
		}
		return fmt.Errorf("failed to read cross-safe history file: %w", err)
	}

	if len(data) == 0 {
		// Empty file, start with empty history
		s.crossSafeHistory = nil
		return nil
	}

	var history []crossSafeMD
	if err := json.Unmarshal(data, &history); err != nil {
		return fmt.Errorf("failed to unmarshal cross-safe history: %w", err)
	}

	s.crossSafeHistory = history
	s.log.Info("Loaded cross-safe history from file", "function", "loadCrossSafeHistory", "entries", len(history), "file", s.crossSafeHistoryFile)
	return nil
}

// saveCrossSafeHistory persists the current cross-safe history to file
func (s *Supervisor) saveCrossSafeHistory() error {
	if s.crossSafeHistoryFile == "" {
		return nil // no file configured
	}

	// Ensure the directory exists
	dir := filepath.Dir(s.crossSafeHistoryFile)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("failed to create directory for cross-safe history file: %w", err)
	}

	data, err := json.Marshal(s.crossSafeHistory)
	if err != nil {
		return fmt.Errorf("failed to marshal cross-safe history: %w", err)
	}

	if err := os.WriteFile(s.crossSafeHistoryFile, data, 0o644); err != nil {
		return fmt.Errorf("failed to write cross-safe history file: %w", err)
	}

	return nil
}

// pruneContainersToTimestamp prunes all containers back to the specified timestamp
func (s *Supervisor) pruneContainersToTimestamp(ctx context.Context, activeChains []eth.ChainID, targetTimestamp uint64) {
	for _, id := range activeChains {
		chainID, ok := id.Uint64()
		if !ok {
			continue
		}

		s.mu.Lock()
		container := s.chains[chainID]
		s.mu.Unlock()

		if container == nil || container.virtualCfg == nil || container.virtualCfg.Rcfg == nil || container.logsDB == nil {
			continue
		}

		// Calculate the target block number for this timestamp
		rcfg := container.virtualCfg.Rcfg
		targetBlockNum, err := rcfg.TargetBlockNumber(targetTimestamp)
		if err != nil {
			s.log.Warn("Failed to calculate target block for rollback", "function", "checkL1Consistency", "chain_id", chainID, "timestamp", targetTimestamp, "error", err)
			continue
		}

		// Rollback the chain to the target block
		if s.rollbackFn != nil {
			if rerr := s.rollbackFn(ctx, chainID, targetBlockNum); rerr != nil {
				s.log.Warn("Chain rollback failed during L1 consistency recovery", "function", "checkL1Consistency", "chain_id", chainID, "target_block", targetBlockNum, "error", rerr)
			} else {
				s.log.Info("Chain rollback completed during L1 consistency recovery", "function", "checkL1Consistency", "chain_id", chainID, "target_block", targetBlockNum)
			}
		}
	}
}

// ============================================================================
// Cross-Safe Progress Loop & Components
// ============================================================================

func (s *Supervisor) ProgressCrossSafe() {
	// configure loop tick duration
	tick := 50 * time.Millisecond
	for {
		s.log.Info("Cross-safe processing cycle started", "function", "ProgressCrossSafe")
		ctx := context.Background()

		// Step 0: snapshot active chains and early-exit if none
		activeChains := s.xsafeSnapshotActiveChains()
		if len(activeChains) == 0 {
			s.log.Info("No active chains available, skipping cross-safe processing", "function", "ProgressCrossSafe")
			time.Sleep(tick)
			continue
		}

		// Step 0.5: Check L1 consistency with latest cross-safe entry
		if !s.checkL1Consistency(ctx, activeChains) {
			// L1 consistency check failed and rollback was performed
			time.Sleep(tick)
			continue
		}

		// Step 1: initialization — set to min genesis if unset
		if !s.initializeCrossSafeTimestamp(activeChains) {
			// Initialization in progress, wait for next tick
			time.Sleep(tick)
			continue
		}

		// Step 2: compute candidate timestamp
		ts := s.computeCandidateTimestamp()

		// Step 2.5: if the candidate timestamp is already recorded, skip this iteration
		if s.getCurrentCrossSafeTimestamp() == ts {
			s.log.Info("Candidate timestamp already processed", "function", "ProgressCrossSafe", "timestamp", ts)
			time.Sleep(tick)
			continue
		}

		// Step 3: obtain L1<>L2 block pairs that meet or precede ts
		pairs, err := s.getBlocksAtTimestamp(ctx, activeChains, ts)
		if err != nil {
			s.log.Info("Required blocks not yet available", "function", "ProgressCrossSafe", "timestamp", ts, "error", err)
			time.Sleep(tick)
			continue
		}
		s.log.Info("Retrieved block pairs for timestamp", "function", "ProgressCrossSafe", "timestamp", ts, "chain_count", len(pairs))

		// Step 4: per-chain target and ingest up to it (manual seals) using pairs
		if !s.xsafeIngestLogsTo(ctx, activeChains, pairs) {
			time.Sleep(tick)
			continue
		}

		// Step 5: get executing messages, validate them, and rollback if needed
		valid := s.validateExecutingMessagesAtTimestamp(ctx, activeChains, ts)

		// Step 6: commit new timestamp with metadata only if validation passed
		if valid {
			s.commitNewTimestamp(ts, pairs)
		} else {
			s.log.Info("Skipping timestamp commit due to validation failure", "function", "ProgressCrossSafe", "timestamp", ts)
		}

		// Step end: wait for next tick
		time.Sleep(tick)
	}
}

// checkL1Consistency verifies that the L1 block from the latest cross-safe entry still exists and matches
// Returns true if consistency check passes or should be skipped, false if rollback is needed
func (s *Supervisor) checkL1Consistency(ctx context.Context, activeChains []eth.ChainID) bool {
	latest := s.getLatestCrossSafe()
	if latest == nil {
		return true
	}

	// Only perform L1 consistency check if the L1Block is initialized (not zero value)
	// Skip check for entries created during initialization that don't have L1Block data yet
	if latest.L1Block.Number == 0 {
		s.log.Info("Skipping L1 consistency check for uninitialized L1 block", "function", "checkL1Consistency")
		return true
	}

	if len(activeChains) == 0 {
		return true
	}

	// Get first active chain for L1 client setup
	var firstChainID uint64
	for _, id := range activeChains {
		if v, ok := id.Uint64(); ok {
			firstChainID = v
			break
		}
	}

	s.mu.Lock()
	firstContainer := s.chains[firstChainID]
	s.mu.Unlock()

	if firstContainer == nil || firstContainer.virtualCfg == nil {
		return true
	}

	l1Cli, l1 := s.EnsureL1Client(ctx, nil, nil, firstContainer.virtualCfg.L1RPC, firstContainer.virtualCfg.Rcfg)
	if l1 == nil {
		return true
	}
	defer func() {
		if l1Cli != nil {
			l1Cli.Close()
		}
	}()

	// Verify the L1 block from latest entry still exists and matches
	expectedL1Block := latest.L1Block
	currentL1Block, err := l1.BlockRefByNumber(ctx, expectedL1Block.Number)
	if err != nil || currentL1Block.Hash != expectedL1Block.Hash {
		s.log.Warn("L1 consistency check failed - L1 block changed, initiating rollback",
			"expected_hash", expectedL1Block.Hash,
			"expected_num", expectedL1Block.Number,
			"current_hash", currentL1Block.Hash,
			"err", err)

		// Rollback procedure:
		// 1. Get timestamp from latest cross-safe entry before pruning
		timestampToPrune := latest.Timestamp

		// 2. Prune denylist entries at or newer than the timestamp
		if s.denylist != nil {
			if err := s.denylist.PruneAtOrNewerThan(timestampToPrune); err != nil {
				s.log.Warn("Failed to prune denylist during rollback", "function", "checkL1Consistency", "timestamp", timestampToPrune, "error", err)
			} else {
				s.log.Info("Pruned denylist entries during rollback", "function", "checkL1Consistency", "timestamp", timestampToPrune)
			}
		}

		// 3. Prune the latest entry from crossSafeHistory
		newLatestTimestamp := s.pruneLatestCrossSafeEntry()
		s.log.Info("Pruned latest cross-safe entry during rollback", "function", "checkL1Consistency", "new_latest_timestamp", newLatestTimestamp)

		// 4. Prune all containers back to the new latest timestamp
		s.pruneContainersToTimestamp(ctx, activeChains, newLatestTimestamp)

		return false
	}

	s.log.Info("L1 consistency check passed", "function", "checkL1Consistency", "l1_block_number", expectedL1Block.Number)
	return true
}

// initializeCrossSafeTimestamp initializes the cross-safe timestamp to minimum genesis if unset
// Returns true if initialization is complete, false if needs to wait for next tick
func (s *Supervisor) initializeCrossSafeTimestamp(activeChains []eth.ChainID) bool {
	ts := s.getCurrentCrossSafeTimestamp()
	s.log.Info("Current cross-safe timestamp", "function", "initializeCrossSafeTimestamp", "timestamp", ts)

	if ts == 0 {
		if minGenesis := s.getMinGenesisTimestamp(activeChains); minGenesis != 0 {
			s.setCrossSafeTimestamp(minGenesis)
			// next tick will try to advance beyond genesis
			return false
		}
	}

	return true
}

// computeCandidateTimestamp computes the next candidate timestamp
func (s *Supervisor) computeCandidateTimestamp() uint64 {
	ts := s.getCurrentCrossSafeTimestamp() + 1
	s.log.Info("Computing candidate timestamp", "function", "computeCandidateTimestamp", "timestamp", ts)
	return ts
}

// validateExecutingMessagesAtTimestamp gets executing messages, validates them, and handles rollback if needed
func (s *Supervisor) validateExecutingMessagesAtTimestamp(ctx context.Context, activeChains []eth.ChainID, ts uint64) bool {
	execByChain := s.getExecutingMessages(activeChains, ts)
	s.log.Info("Retrieved executing messages for validation", "function", "validateExecutingMessagesAtTimestamp", "messages_by_chain", execByChain)

	valid := s.validateExecutingMessages(ctx, activeChains, ts, execByChain)
	if !valid {
		s.log.Warn("Cross-safe validation failed, rollback performed", "function", "validateExecutingMessagesAtTimestamp", "timestamp", ts)
	}

	return valid
}

// commitNewTimestamp commits new timestamp with metadata
func (s *Supervisor) commitNewTimestamp(ts uint64, pairs map[uint64]types.DerivedBlockRefPair) {
	// Find the L1 block with the highest number from all pairs
	var latestL1Block eth.BlockRef
	l2Blocks := make(map[uint64]types.DerivedBlockRefPair)

	for chainID, pair := range pairs {
		// Check if this L1 block has a higher number than our current latest
		if latestL1Block.Number == 0 || pair.Source.Number > latestL1Block.Number {
			latestL1Block = pair.Source
		}
		l2Blocks[chainID] = pair
	}

	newEntry := crossSafeMD{
		Timestamp: ts,
		L1Block:   latestL1Block,
		L2Blocks:  l2Blocks,
	}

	s.addCrossSafeEntry(newEntry)
	s.log.Info("Cross-safe timestamp committed", "function", "commitCrossSafeTimestamp", "timestamp", ts, "l1_block_number", latestL1Block.Number, "l2_chain_count", len(l2Blocks))
}

// ============================================================================
// Cross-Safe Data Retrieval & Processing
// ============================================================================

// openLogsDB initializes the logs DB for a chain.
func (s *Supervisor) openLogsDB(logger log.Logger, chainID uint64, dataDir string) (*logsdb.DB, error) {
	// Ensure base directory exists
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return nil, err
	}
	logsPath := fmt.Sprintf("%s/logs-%d", dataDir, chainID)
	// Use no-op metrics for now; can be replaced with real metrics later.
	logDB, err := logsdb.NewFromFile(logger, logsMetricsNoop{}, eth.ChainIDFromUInt64(chainID), logsPath, true)
	if err != nil {
		return nil, err
	}
	return logDB, nil
}

// ingestRange fetches payload, receipts and appends logs for [start,end] (inclusive).
func (s *Supervisor) ingestRange(ctx context.Context, l2 *sources.L2Client, logs *logsdb.DB, chainID, start, end uint64) error {
	blockCount := end - start + 1
	rangeLogger := s.log.New("operation", "log_ingestion", "chain", chainID, "start_block", start, "end_block", end, "block_count", blockCount)
	rangeLogger.Info("starting log ingestion range")

	totalLogs := uint64(0)
	execMsgCount := uint64(0)
	totalTxs := uint64(0)

	for n := start; n <= end; n++ {
		blockLogger := rangeLogger.New("block", n)

		env, err := l2.PayloadByNumber(ctx, n)
		if err != nil {
			blockLogger.Error("failed to fetch payload", "error", err)
			return err
		}
		ref, err := derive.PayloadToBlockRef(l2.RollupConfig(), env.ExecutionPayload)
		if err != nil {
			blockLogger.Error("failed to convert payload to block ref", "error", err)
			return err
		}
		// Fetch tx receipts to obtain logs per tx
		info, receipts, err := l2.FetchReceiptsByNumber(ctx, n)
		if err != nil {
			blockLogger.Error("failed to fetch receipts", "error", err)
			return err
		}

		totalTxs += uint64(len(receipts))
		blockLogger.Debug("fetched block data", "tx_count", len(receipts), "block_hash", ref.Hash)
		// Collect logs flat in block order
		var allLogs []*ethTypes.Log
		for _, r := range receipts {
			allLogs = append(allLogs, r.Logs...)
		}

		blockLogCount := uint64(len(allLogs))
		totalLogs += blockLogCount
		blockLogger.Debug("collected logs from receipts", "log_count", blockLogCount)

		// Write logs to DB
		// Identify parent block by number-1
		var parent eth.BlockID
		if n > 0 {
			parent = eth.BlockID{Hash: ref.ParentHash, Number: n - 1}
		}

		blockExecMsgCount := uint64(0)
		for i, lg := range allLogs {
			// Try to decode ExecutingMessage; may be nil for non-exec logs
			var exec *types.ExecutingMessage
			if m, err := processors.DecodeExecutingMessageLog(lg); err == nil && m != nil {
				exec = m
				blockExecMsgCount++
				blockLogger.Debug("decoded executing message",
					"log_idx", i,
					"initiating_chain", m.ChainID,
					"block_num", m.BlockNum,
					"timestamp", m.Timestamp)
			}
			if err := logs.AddLog(processors.LogToLogHash(lg), parent, uint32(i), exec); err != nil {
				blockLogger.Error("failed to add log to database", "log_idx", i, "error", err)
				return err
			}
		}

		execMsgCount += blockExecMsgCount
		if blockExecMsgCount > 0 {
			blockLogger.Info("processed executing messages", "exec_msg_count", blockExecMsgCount)
		}
		// Seal block in logs DB
		if err := logs.SealBlock(ref.ParentHash, eth.ToBlockID(info), ref.Time); err != nil {
			blockLogger.Error("failed to seal block in logs database", "error", err)
			return err
		}

		blockLogger.Debug("block sealed successfully", "block_hash", info.Hash, "timestamp", ref.Time)
	}

	rangeLogger.Info("log ingestion range completed",
		"total_logs", totalLogs,
		"total_txs", totalTxs,
		"executing_messages", execMsgCount,
		"avg_logs_per_block", float64(totalLogs)/float64(blockCount))
	return nil
}

func (s *Supervisor) xsafeSnapshotActiveChains() []eth.ChainID {
	s.chainsMu.Lock()
	defer s.chainsMu.Unlock()
	out := make([]eth.ChainID, 0, len(s.chains))
	for chainID := range s.chains {
		out = append(out, eth.ChainIDFromUInt64(chainID))
	}
	return out
}

// getMinGenesisTimestamp computes the minimum L2 genesis timestamp across the provided active chains.
// Returns 0 if none of the chains have a rollup config.
func (s *Supervisor) getMinGenesisTimestamp(activeChains []eth.ChainID) uint64 {
	var minGenesis uint64
	for _, id := range activeChains {
		chainID, ok := id.Uint64()
		if !ok {
			continue
		}

		s.mu.Lock()
		container := s.chains[chainID]
		s.mu.Unlock()

		if container != nil && container.virtualCfg != nil && container.virtualCfg.Rcfg != nil {
			gts := container.virtualCfg.Rcfg.Genesis.L2Time
			if minGenesis == 0 || gts < minGenesis {
				minGenesis = gts
			}
		}
	}
	return minGenesis
}

// getBlocksAtTimestamp returns, for each chain, the (L1,L2) block refs corresponding to the floor
// L2 block at the given timestamp. It first gates on having SafeL2 at least at that block number.
func (s *Supervisor) getBlocksAtTimestamp(ctx context.Context, activeChains []eth.ChainID, ts uint64) (map[uint64]types.DerivedBlockRefPair, error) {
	pairs := make(map[uint64]types.DerivedBlockRefPair, len(activeChains))
	var l1Cli opclient.RPC
	var l1 *sources.L1Client
	for _, id := range activeChains {
		v, ok := id.Uint64()
		if !ok {
			continue
		}

		s.mu.Lock()
		container := s.chains[v]
		s.mu.Unlock()

		if container == nil || container.virtualCfg == nil || container.virtualCfg.Rcfg == nil || container.virtualOpNodeUserRPC == "" {
			return nil, fmt.Errorf("missing container/config/opnode for chain %d", v)
		}
		// Compute expected L2 block number at ts (floor)
		rcfg := container.virtualCfg.Rcfg
		targetNum, err := blockAtTimestampFromConfig(rcfg, ts)
		if err != nil {
			return nil, fmt.Errorf("chain %d: compute target num: %w", v, err)
		}
		// Gate: SafeL2 must be at or beyond targetNum
		st, err := s.fetchSyncStatus(ctx, container.virtualOpNodeUserRPC)
		if err != nil || st == nil {
			return nil, fmt.Errorf("chain %d: fetch sync status: %w", v, err)
		}
		if st.SafeL2.Number < targetNum {
			return nil, fmt.Errorf("chain %d: safe head too low: have %d need %d", v, st.SafeL2.Number, targetNum)
		}
		s.log.Info("Chain safety gate passed", "function", "getBlocksAtTimestamp", "chain_id", v, "safe_block", st.SafeL2.Number, "required_block", targetNum)
		// Ensure clients
		l1Cli, l1 = s.EnsureL1Client(ctx, l1Cli, l1, container.virtualCfg.L1RPC, rcfg)
		l2Cli, err := opclient.NewRPC(ctx, s.log, container.virtualCfg.L2UserRPC)
		if err != nil {
			return nil, fmt.Errorf("chain %d: dial L2: %w", v, err)
		}
		l2, err := sources.NewL2Client(l2Cli, s.log, nil, sources.L2ClientDefaultConfig(rcfg, true))
		if err != nil {
			l2Cli.Close()
			return nil, fmt.Errorf("chain %d: new L2 client: %w", v, err)
		}
		// Fetch L2 and its L1 origin
		env, err := l2.PayloadByNumber(ctx, targetNum)
		if err != nil {
			l2Cli.Close()
			return nil, fmt.Errorf("chain %d: payload by number %d: %w", v, targetNum, err)
		}
		br, derr := derive.PayloadToBlockRef(rcfg, env.ExecutionPayload)
		if derr != nil {
			l2Cli.Close()
			return nil, fmt.Errorf("chain %d: payload to block ref: %w", v, derr)
		}
		l1Ref, e1 := l1.BlockRefByNumber(ctx, br.L1Origin.Number)
		l2Cli.Close()
		if e1 != nil {
			return nil, fmt.Errorf("chain %d: l1 block by number %d: %w", v, br.L1Origin.Number, e1)
		}
		pairs[v] = types.DerivedBlockRefPair{Source: l1Ref, Derived: br.BlockRef()}
	}
	return pairs, nil
}

// xsafeIngestLogsTo manually seals blocks in logsDB up to the provided target pairs.
// Expects pre-resolved handles and computed target blocks. Idempotent: skips blocks already sealed.
func (s *Supervisor) xsafeIngestLogsTo(ctx context.Context, activeChains []eth.ChainID, pairs map[uint64]types.DerivedBlockRefPair) bool {
	ready := true
	// Share a single L1 client across chains
	var l1Cli opclient.RPC
	var l1 *sources.L1Client
	for _, id := range activeChains {
		v, ok := id.Uint64()
		if !ok {
			continue
		}

		s.mu.Lock()
		container := s.chains[v]
		s.mu.Unlock()

		s.log.Info("Processing log ingestion for chain", "function", "xsafeIngestLogsTo", "chain_id", v)
		if container == nil || container.virtualCfg == nil || container.virtualCfg.Rcfg == nil || container.logsDB == nil {
			continue
		}
		targetPair, ok := pairs[v]
		if !ok {
			// No target for this chain in the pair set
			continue
		}
		targetNum := targetPair.Derived.Number
		if blk, ok := container.logsDB.LatestSealedBlock(); ok {
			s.log.Info("Chain logs head before ingestion", "function", "xsafeIngestLogsTo", "chain_id", v, "block_number", blk.Number)
			if blk.Number >= targetNum {
				// Already at or past target; skip
				continue
			}
		}
		rcfg := container.virtualCfg.Rcfg
		s.log.Info("Computed ingestion target", "function", "xsafeIngestLogsTo", "chain_id", v, "target_block", targetNum)

		// Ensure L1 client
		l1Cli, l1 = s.EnsureL1Client(ctx, l1Cli, l1, container.virtualCfg.L1RPC, rcfg)
		if l1 == nil {
			s.log.Warn("Missing L1 client for chain", "function", "xsafeIngestLogsTo", "chain_id", v)
			ready = false
			break
		}

		// Build L2 client (EL user RPC)
		l2Cli, err := opclient.NewRPC(ctx, s.log, container.virtualCfg.L2UserRPC)
		if err != nil {
			ready = false
			break
		}
		l2, err := sources.NewL2Client(l2Cli, s.log, nil, sources.L2ClientDefaultConfig(rcfg, true))
		if err != nil {
			l2Cli.Close()
			ready = false
			break
		}

		// Determine ingest start
		start := targetNum
		if blk, ok := container.logsDB.LatestSealedBlock(); ok {
			start = blk.Number + 1
			if start > targetNum {
				start = targetNum
			}
		}
		s.log.Info("Ingesting log range", "function", "xsafeIngestLogsTo", "chain_id", v, "start_block", start, "end_block", targetNum)
		if err := s.ingestRange(ctx, l2, container.logsDB, v, start, targetNum); err != nil {
			s.log.Warn("Log ingestion failed for chain", "function", "xsafeIngestLogsTo", "chain_id", v, "error", err)
			l2Cli.Close()
			ready = false
			break
		}
		if blk, ok := container.logsDB.LatestSealedBlock(); ok {
			s.log.Info("Chain logs head after ingestion", "function", "xsafeIngestLogsTo", "chain_id", v, "block_number", blk.Number)
		}
		l2Cli.Close()
	}
	return ready
}

// getExecutingMessages returns the executing messages per chain at the target block for ts.
func (s *Supervisor) getExecutingMessages(activeChains []eth.ChainID, ts uint64) map[uint64]map[uint32]*types.ExecutingMessage {
	out := make(map[uint64]map[uint32]*types.ExecutingMessage, len(activeChains))
	s.log.Info("Retrieving executing messages", "function", "getExecutingMessages", "active_chains", activeChains, "timestamp", ts)
	for _, id := range activeChains {
		v, ok := id.Uint64()
		if !ok {
			continue
		}

		s.mu.Lock()
		container := s.chains[v]
		s.mu.Unlock()

		if container == nil || container.virtualCfg == nil || container.virtualCfg.Rcfg == nil || container.logsDB == nil {
			s.log.Info("Skipping validation for chain (missing config/database)", "function", "getExecutingMessages", "chain_id", v)
			continue
		}
		rcfg := container.virtualCfg.Rcfg
		targetNum, err := rcfg.TargetBlockNumber(ts)
		if err != nil {
			s.log.Info("Validation target before genesis", "function", "getExecutingMessages", "chain_id", v, "timestamp", ts)
			continue
		}
		_, logcount, execMsgs, err := container.logsDB.OpenBlock(targetNum)
		s.log.Info("Retrieved messages for chain", "function", "getExecutingMessages", "chain_id", v, "log_count", logcount, "executing_messages", len(execMsgs))
		if err != nil {
			s.log.Info("Failed to open block for validation", "function", "getExecutingMessages", "chain_id", v, "block_number", targetNum, "error", err)
			continue
		}
		if len(execMsgs) == 0 {
			s.log.Info("No executing messages found", "function", "getExecutingMessages", "chain_id", v, "block_number", targetNum)
		}
		for logIdx, msg := range execMsgs {
			if msg == nil {
				continue
			}
			s.log.Info("Found executing message", "function", "getExecutingMessages", "chain_id", v, "block_number", targetNum, "log_index", logIdx, "initiating_chain", msg.ChainID, "initiating_block", msg.BlockNum, "timestamp", msg.Timestamp)
		}
		out[v] = execMsgs
	}
	return out
}

// validateExecutingMessages verifies that each executing message references an initiating log present on the initiating chain.
func (s *Supervisor) validateExecutingMessages(ctx context.Context, activeChains []eth.ChainID, ts uint64, execByChain map[uint64]map[uint32]*types.ExecutingMessage) bool {
	allValid := true
	invalidCount := 0
	totalCount := 0
	// avoid duplicate rollbacks per chain in a single step
	rolledBack := make(map[uint64]bool)
	for _, id := range activeChains {
		v, ok := id.Uint64()
		if !ok {
			continue
		}

		execMsgs := execByChain[v]
		for _, msg := range execMsgs {
			if msg == nil {
				continue
			}
			totalCount++
			if initCID, ok := msg.ChainID.Uint64(); ok {
				s.mu.Lock()
				initContainer := s.chains[initCID]
				s.mu.Unlock()
				if initContainer == nil || initContainer.logsDB == nil {
					s.log.Info("Missing initiating logsDB for validation", "function", "validateExecutingMessages", "initiating_chain", initCID)
					allValid = false
					invalidCount++
					continue
				}
				query := types.ContainsQuery{BlockNum: msg.BlockNum, LogIdx: msg.LogIdx, Timestamp: msg.Timestamp, Checksum: msg.Checksum}

				// Note: this if-block exists to prevent cycles from being validated, by preventing executing messages from pointing to the current timestamp
				// HOWEVER this is technically not to-spec. Executing Messages may point at the current timestamp, so long as they do not form cycles.
				// Rather than adding cycle detection, we are simply failing the block in these conditions.
				// Production software will need to support cycle detection.
				if msg.Timestamp == ts {
					s.log.Warn("Execution validation failed due to potential cycle", "function", "validateExecutingMessages", "executing_chain", v, "initiating_chain", initCID, "timestamp", msg.Timestamp)
					allValid = false
					invalidCount++
					if !rolledBack[v] {
						s.mu.Lock()
						container := s.chains[v]
						s.mu.Unlock()
						if container != nil && container.virtualCfg != nil && container.virtualCfg.Rcfg != nil && container.logsDB != nil {
							if targetNum, terr := container.virtualCfg.Rcfg.TargetBlockNumber(ts); terr == nil {
								if ref, _, _, oerr := container.logsDB.OpenBlock(targetNum); oerr == nil {
									if s.denylist != nil {
										_ = s.denylist.Add(v, ref.Time, ref.Hash.Hex())
										s.log.Info("Cross-safe denylist add", "function", "validateExecutingMessages", "chain_id", v, "block_hash", ref.Hash, "block_number", targetNum)
									}
								}
							}
						}
					}
				} else if _, err := initContainer.logsDB.Contains(query); err != nil {
					s.log.Warn("Execution validation failed", "function", "validateExecutingMessages", "executing_chain", v, "initiating_chain", initCID, "error", err)
					allValid = false
					invalidCount++
					// Side-effects: mark denylist and rollback the executing chain before the block at this ts
					if !rolledBack[v] {
						s.mu.Lock()
						container := s.chains[v]
						s.mu.Unlock()
						if container != nil && container.virtualCfg != nil && container.virtualCfg.Rcfg != nil && container.logsDB != nil {
							if targetNum, terr := container.virtualCfg.Rcfg.TargetBlockNumber(ts); terr == nil {
								if ref, _, _, oerr := container.logsDB.OpenBlock(targetNum); oerr == nil {
									if s.denylist != nil {
										_ = s.denylist.Add(v, ref.Time, ref.Hash.Hex())
										s.log.Info("Cross-safe denylist add", "function", "validateExecutingMessages", "chain_id", v, "block_hash", ref.Hash, "block_number", targetNum)
									}
								}
								to := uint64(0)
								if targetNum > 0 {
									to = targetNum - 1
								}
								if s.rollbackFn != nil {
									if rerr := s.rollbackFn(ctx, v, to); rerr != nil {
										s.log.Warn("Cross-safe rollback failed", "function", "validateExecutingMessages", "chain_id", v, "to_block", to, "error", rerr)
									} else {
										s.log.Info("Chain rollback executed", "function", "validateExecutingMessages", "chain_id", v, "target_block", to)
									}
								}
								rolledBack[v] = true
							}
						}
					}
				} else {
					s.log.Info("Execution validation passed", "function", "validateExecutingMessages", "executing_chain", v, "initiating_chain", initCID)
				}
			}
		}
	}
	s.log.Info("Execution message validation completed", "function", "validateExecutingMessages", "total_messages", totalCount, "invalid_messages", invalidCount, "timestamp", ts)
	return allValid
}

// authorizeFinalityUpdate checks if a finality update is authorized based on the cross-safe history.
// It returns true if the attempted finality timestamp is at or before the latest cross-safe timestamp.
func (s *Supervisor) authorizeFinalityUpdate(timestamp uint64) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	// If no cross-safe history exists, deny finality updates
	if len(s.crossSafeHistory) == 0 {
		s.log.Info("Finality authorization denied: no cross-safe history", "function", "authorizeFinalityUpdate", "timestamp", timestamp)
		return false
	}

	// Get the latest cross-safe entry
	latest := s.getLatestCrossSafe()

	// Check if the timestamp is at or before the latest cross-safe timestamp
	if timestamp > latest.Timestamp {
		s.log.Info("Finality authorization denied: timestamp too recent", "function", "authorizeFinalityUpdate",
			"timestamp", timestamp, "cross_safe_ts", latest.Timestamp)
		return false
	}

	s.log.Info("Finality authorization granted", "function", "authorizeFinalityUpdate",
		"timestamp", timestamp, "cross_safe_ts", latest.Timestamp)
	return true
}

// ============================================================================
// Configuration & Lifecycle Management
// ============================================================================

// getDataDir returns the base data directory for chain DBs
func (s *Supervisor) getDataDir() string { return s.dataDir }

// SetDataDir overrides the base data directory for chain DBs and denylist persistence.
// Should be called before starting any chains or HTTP server.
func (s *Supervisor) SetDataDir(dir string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if dir == "" {
		return
	}
	s.dataDir = dir
	s.denylist = NewDenylistStore(filepath.Join(s.dataDir, "denylist.json"), s.log)
	s.crossSafeHistoryFile = filepath.Join(s.dataDir, "crossSafeHistory.json")
	// load existing cross-safe history from new location if available
	if err := s.loadCrossSafeHistory(); err != nil {
		s.log.Warn("Failed to load cross-safe history after SetDataDir", "function", "SetDataDir", "error", err)
	}
}

func (s *Supervisor) getL1ScopeLabel() eth.BlockLabel {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.l1ScopeLabel
}

// SetL1ScopeLabel overrides the L1 scope label (e.g., eth.Unsafe in tests).
func (s *Supervisor) SetL1ScopeLabel(label eth.BlockLabel) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.l1ScopeLabel = label
}

// EnableOpNodeProxy toggles the /opnode/ reverse proxy in the HTTP handler.
func (s *Supervisor) EnableOpNodeProxy(v bool) { s.enableOpNodeProxy = v }

func (s *Supervisor) Stop() {
	// Stop all chains
	s.mu.Lock()
	chains := make(map[uint64]*ChainContainer)
	for id, container := range s.chains {
		chains[id] = container
	}
	s.mu.Unlock()

	for chainID := range chains {
		s.RemoveChain(chainID)
	}
}

// ============================================================================
// Client Management
// ============================================================================

// EnsureL1Client lazily initializes the L1 client using the given RPC URL.
func (s *Supervisor) EnsureL1Client(ctx context.Context, l1Cli opclient.RPC, l1 *sources.L1Client, l1RPC string, rcfg *rollup.Config) (opclient.RPC, *sources.L1Client) {
	if l1 != nil {
		return l1Cli, l1
	}
	if l1Cli == nil {
		if c, e := opclient.NewRPC(ctx, s.log, l1RPC); e == nil {
			l1Cli = c
		}
	}
	if l1Cli != nil {
		if l1Client, e := sources.NewL1Client(l1Cli, s.log, nil, sources.L1ClientDefaultConfig(rcfg, true, sources.RPCKindStandard)); e == nil {
			l1 = l1Client
		}
	}
	return l1Cli, l1
}

// crossFinalizedFromDBOrFallback returns 0 since cross DBs were removed in v2.
// Kept for API compatibility.
func (s *Supervisor) crossFinalizedFromDBOrFallback() uint64 {
	return 0
}
