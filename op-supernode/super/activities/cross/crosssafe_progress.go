package cross

import (
	"context"
	"fmt"
	"time"

	ethTypes "github.com/ethereum/go-ethereum/core/types"

	"github.com/ethereum-optimism/optimism/op-node/rollup/derive"
	opclient "github.com/ethereum-optimism/optimism/op-service/client"
	"github.com/ethereum-optimism/optimism/op-service/eth"
	"github.com/ethereum-optimism/optimism/op-service/sources"
	logsdb "github.com/ethereum-optimism/optimism/op-supervisor/supervisor/backend/db/logs"
	"github.com/ethereum-optimism/optimism/op-supervisor/supervisor/backend/processors"
	"github.com/ethereum-optimism/optimism/op-supervisor/supervisor/types"
)

// ============================================================================
// Cross-Safe Progress Loop & Components
// ============================================================================

func (s *CrossService) ProgressCrossSafe() {
	defer close(s.done)

	// configure loop tick duration
	tick := 50 * time.Millisecond
	for {
		select {
		case <-s.stopCh:
			return
		default:
		}

		s.log.Info("xsafe: loop start")
		ctx := context.Background()

		// Step 0: snapshot active chains and early-exit if none
		activeChains := s.xsafeSnapshotActiveChains()
		if len(activeChains) == 0 {
			s.log.Info("xsafe: no active chains; skipping")
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
			s.log.Info("xsafe: candidate timestamp already recorded, skipping")
			time.Sleep(tick)
			continue
		}

		// Step 3: obtain L1<>L2 block pairs that meet or precede ts
		pairs, err := s.getBlocksAtTimestamp(ctx, activeChains, ts)
		if err != nil {
			s.log.Info("xsafe: blocks not ready", "err", err)
			time.Sleep(tick)
			continue
		}
		s.log.Info("xsafe: blocks at ts", "ts", ts, "chains", len(pairs))

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
			s.log.Info("xsafe: skipping timestamp commit due to validation failure", "ts", ts)
		}

		// Step end: wait for next tick
		time.Sleep(tick)
	}
}

// Supporting methods for ProgressCrossSafe

func (s *CrossService) xsafeSnapshotActiveChains() []eth.ChainID {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]eth.ChainID, 0, len(s.chains))
	for chainID := range s.chains {
		out = append(out, eth.ChainIDFromUInt64(chainID))
	}
	return out
}

// checkL1Consistency verifies that the L1 block from the latest cross-safe entry still exists and matches
// Returns true if consistency check passes or should be skipped, false if rollback is needed
func (s *CrossService) checkL1Consistency(ctx context.Context, activeChains []eth.ChainID) bool {
	latest := s.getLatestCrossSafe()
	if latest == nil {
		return true
	}

	// Only perform L1 consistency check if the L1Block is initialized (not zero value)
	// Skip check for entries created during initialization that don't have L1Block data yet
	if latest.L1Block.Number == 0 {
		s.log.Info("xsafe: skipping L1 consistency check for uninitialized L1Block")
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

	if firstContainer == nil || firstContainer.VirtualCfg == nil {
		return true
	}

	l1Cli, l1 := s.EnsureL1Client(ctx, nil, nil, firstContainer.VirtualCfg.L1RPC, firstContainer.VirtualCfg.Rcfg)
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
		s.log.Warn("xsafe: L1 consistency check failed - L1 block changed, rolling back",
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
				s.log.Warn("xsafe: failed to prune denylist", "timestamp", timestampToPrune, "err", err)
			} else {
				s.log.Info("xsafe: pruned denylist entries at or newer than timestamp", "timestamp", timestampToPrune)
			}
		}

		// 3. Prune the latest entry from crossSafeHistory
		newLatestTimestamp := s.pruneLatestCrossSafeEntry()
		s.log.Info("xsafe: pruned latest cross-safe entry", "new_latest_ts", newLatestTimestamp)

		// 4. Prune all containers back to the new latest timestamp
		s.pruneContainersToTimestamp(ctx, activeChains, newLatestTimestamp)

		return false
	}

	s.log.Info("xsafe: L1 consistency check passed", "l1_block", expectedL1Block.Number)
	return true
}

// initializeCrossSafeTimestamp initializes the cross-safe timestamp to minimum genesis if unset
// Returns true if initialization is complete, false if needs to wait for next tick
func (s *CrossService) initializeCrossSafeTimestamp(activeChains []eth.ChainID) bool {
	ts := s.getCurrentCrossSafeTimestamp()
	s.log.Info("xsafe: current timestamp", "ts", ts)

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
func (s *CrossService) computeCandidateTimestamp() uint64 {
	ts := s.getCurrentCrossSafeTimestamp() + 1
	s.log.Info("xsafe: candidate next timestamp", "ts", ts)
	return ts
}

// validateExecutingMessagesAtTimestamp gets executing messages, validates them, and handles rollback if needed
func (s *CrossService) validateExecutingMessagesAtTimestamp(ctx context.Context, activeChains []eth.ChainID, ts uint64) bool {
	execByChain := s.getExecutingMessages(activeChains, ts)
	s.log.Info("xsafe: executing messages", "execByChain", execByChain)

	valid := s.validateExecutingMessages(ctx, activeChains, ts, execByChain)
	if !valid {
		s.log.Info("xsafe: validation detected issues (rollback was handled in validateExecutingMessages and it should honestly happen at a higher level idk bro)")
	}

	return valid
}

// commitNewTimestamp commits new timestamp with metadata
func (s *CrossService) commitNewTimestamp(ts uint64, pairs map[uint64]types.DerivedBlockRefPair) {
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
	s.log.Info("xsafe: committed new timestamp", "ts", ts, "l1_block", latestL1Block.Number, "l2_chains", len(l2Blocks))
}

// pruneContainersToTimestamp prunes all containers back to the specified timestamp
func (s *CrossService) pruneContainersToTimestamp(ctx context.Context, activeChains []eth.ChainID, targetTimestamp uint64) {
	for _, id := range activeChains {
		chainID, ok := id.Uint64()
		if !ok {
			continue
		}

		s.mu.Lock()
		container := s.chains[chainID]
		s.mu.Unlock()

		if container == nil || container.VirtualCfg == nil || container.VirtualCfg.Rcfg == nil {
			continue
		}

		logsDB := s.GetLogsDB(chainID)
		if logsDB == nil {
			continue
		}

		// Calculate the target block number for this timestamp
		rcfg := container.VirtualCfg.Rcfg
		targetBlockNum, err := rcfg.TargetBlockNumber(targetTimestamp)
		if err != nil {
			s.log.Warn("xsafe: failed to calculate target block for rollback", "chain", chainID, "ts", targetTimestamp, "err", err)
			continue
		}

		// Rollback the chain to the target block
		if s.rollbackFn != nil {
			if rerr := s.rollbackFn(ctx, chainID, targetBlockNum); rerr != nil {
				s.log.Warn("xsafe: rollback failed during L1 consistency recovery", "chain", chainID, "to_block", targetBlockNum, "err", rerr)
			} else {
				s.log.Info("xsafe: rollback executed during L1 consistency recovery", "chain", chainID, "to_block", targetBlockNum)
			}
		}
	}
}

// getMinGenesisTimestamp computes the minimum L2 genesis timestamp across the provided active chains.
// Returns 0 if none of the chains have a rollup config.
func (s *CrossService) getMinGenesisTimestamp(activeChains []eth.ChainID) uint64 {
	var minGenesis uint64
	for _, id := range activeChains {
		chainID, ok := id.Uint64()
		if !ok {
			continue
		}

		s.mu.Lock()
		container := s.chains[chainID]
		s.mu.Unlock()

		if container != nil && container.VirtualCfg != nil && container.VirtualCfg.Rcfg != nil {
			gts := container.VirtualCfg.Rcfg.Genesis.L2Time
			if minGenesis == 0 || gts < minGenesis {
				minGenesis = gts
			}
		}
	}
	return minGenesis
}

// getBlocksAtTimestamp returns, for each chain, the (L1,L2) block refs corresponding to the floor
// L2 block at the given timestamp. It first gates on having SafeL2 at least at that block number.
func (s *CrossService) getBlocksAtTimestamp(ctx context.Context, activeChains []eth.ChainID, ts uint64) (map[uint64]types.DerivedBlockRefPair, error) {
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

		if container == nil || container.VirtualCfg == nil || container.VirtualCfg.Rcfg == nil || container.VirtualOpNodeUserRPC == "" {
			return nil, fmt.Errorf("missing container/config/opnode for chain %d", v)
		}
		// Compute expected L2 block number at ts (floor)
		rcfg := container.VirtualCfg.Rcfg
		targetNum, err := blockAtTimestampFromConfig(rcfg, ts)
		if err != nil {
			return nil, fmt.Errorf("chain %d: compute target num: %w", v, err)
		}
		// Gate: SafeL2 must be at or beyond targetNum
		st, err := s.fetchSyncStatus(ctx, container.VirtualOpNodeUserRPC)
		if err != nil || st == nil {
			return nil, fmt.Errorf("chain %d: fetch sync status: %w", v, err)
		}
		if st.SafeL2.Number < targetNum {
			return nil, fmt.Errorf("chain %d: safe head too low: have %d need %d", v, st.SafeL2.Number, targetNum)
		}
		s.log.Info("xsafe: gate ok", "chain", v, "safe_num", st.SafeL2.Number, "need_num", targetNum)
		// Ensure clients
		l1Cli, l1 = s.EnsureL1Client(ctx, l1Cli, l1, container.VirtualCfg.L1RPC, rcfg)
		l2Cli, err := opclient.NewRPC(ctx, s.log, container.VirtualCfg.L2UserRPC)
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
func (s *CrossService) xsafeIngestLogsTo(ctx context.Context, activeChains []eth.ChainID, pairs map[uint64]types.DerivedBlockRefPair) bool {
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

		s.log.Info("xsafe: xsafeIngestLogsTo", "chain", v, "container", container)
		if container == nil || container.VirtualCfg == nil || container.VirtualCfg.Rcfg == nil {
			continue
		}

		logsDB := s.GetLogsDB(v)
		if logsDB == nil {
			continue
		}
		targetPair, ok := pairs[v]
		if !ok {
			// No target for this chain in the pair set
			continue
		}
		targetNum := targetPair.Derived.Number
		if blk, ok := logsDB.LatestSealedBlock(); ok {
			s.log.Info("xsafe: logs head before", "chain", v, "num", blk.Number)
			if blk.Number >= targetNum {
				// Already at or past target; skip
				continue
			}
		}
		rcfg := container.VirtualCfg.Rcfg
		s.log.Info("xsafe: target computed", "chain", v, "target", targetNum)

		// Ensure L1 client
		l1Cli, l1 = s.EnsureL1Client(ctx, l1Cli, l1, container.VirtualCfg.L1RPC, rcfg)
		if l1 == nil {
			s.log.Info("xsafe: missing L1 client", "chain", v)
			ready = false
			break
		}

		// Build L2 client (EL user RPC)
		l2Cli, err := opclient.NewRPC(ctx, s.log, container.VirtualCfg.L2UserRPC)
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
		if blk, ok := logsDB.LatestSealedBlock(); ok {
			start = blk.Number + 1
			if start > targetNum {
				start = targetNum
			}
		}
		s.log.Info("xsafe: ingest range", "chain", v, "start", start, "end", targetNum)
		if err := s.ingestRange(ctx, l2, logsDB, start, targetNum); err != nil {
			s.log.Info("xsafe: ingest failed", "chain", v, "err", err)
			l2Cli.Close()
			ready = false
			break
		}
		if blk, ok := logsDB.LatestSealedBlock(); ok {
			s.log.Info("xsafe: logs head after", "chain", v, "num", blk.Number)
		}
		l2Cli.Close()
	}
	return ready
}

// ingestRange fetches payload, receipts and appends logs for [start,end] (inclusive).
func (s *CrossService) ingestRange(ctx context.Context, l2 *sources.L2Client, logs *logsdb.DB, start, end uint64) error {
	for n := start; n <= end; n++ {
		env, err := l2.PayloadByNumber(ctx, n)
		if err != nil {
			return err
		}
		ref, err := derive.PayloadToBlockRef(l2.RollupConfig(), env.ExecutionPayload)
		if err != nil {
			return err
		}
		// Fetch tx receipts to obtain logs per tx
		info, receipts, err := l2.FetchReceiptsByNumber(ctx, n)
		if err != nil {
			return err
		}
		// Collect logs flat in block order
		var allLogs []*ethTypes.Log
		for _, r := range receipts {
			allLogs = append(allLogs, r.Logs...)
		}
		// Write logs to DB
		// Identify parent block by number-1
		var parent eth.BlockID
		if n > 0 {
			parent = eth.BlockID{Hash: ref.ParentHash, Number: n - 1}
		}
		for i, lg := range allLogs {
			// Try to decode ExecutingMessage; may be nil for non-exec logs
			var exec *types.ExecutingMessage
			if m, err := processors.DecodeExecutingMessageLog(lg); err == nil && m != nil {
				exec = m
			}
			if err := logs.AddLog(processors.LogToLogHash(lg), parent, uint32(i), exec); err != nil {
				return err
			}

		}
		// Seal block in logs DB
		if err := logs.SealBlock(ref.ParentHash, eth.ToBlockID(info), ref.Time); err != nil {
			return err
		}
	}
	return nil
}

// getExecutingMessages returns the executing messages per chain at the target block for ts.
func (s *CrossService) getExecutingMessages(activeChains []eth.ChainID, ts uint64) map[uint64]map[uint32]*types.ExecutingMessage {
	out := make(map[uint64]map[uint32]*types.ExecutingMessage, len(activeChains))
	s.log.Info("xsafe: getExecutingMessages", "activeChains", activeChains, "ts", ts)
	for _, id := range activeChains {
		v, ok := id.Uint64()
		if !ok {
			continue
		}

		s.mu.Lock()
		container := s.chains[v]
		s.mu.Unlock()

		if container == nil || container.VirtualCfg == nil || container.VirtualCfg.Rcfg == nil {
			s.log.Info("xsafe: validation skip (missing cfg)", "chain", v)
			continue
		}

		logsDB := s.GetLogsDB(v)
		if logsDB == nil {
			s.log.Info("xsafe: validation skip (missing logsDB)", "chain", v)
			continue
		}
		rcfg := container.VirtualCfg.Rcfg
		targetNum, err := rcfg.TargetBlockNumber(ts)
		if err != nil {
			s.log.Info("xsafe: validation target before genesis", "chain", v, "ts", ts)
			continue
		}
		_, logcount, execMsgs, err := logsDB.OpenBlock(targetNum)
		s.log.Info("xsafe: getExecutingMessages", "chain", v, "logcount", logcount, "execMsgs", execMsgs)
		if err != nil {
			s.log.Info("xsafe: validation open block failed", "chain", v, "block", targetNum, "err", err)
			continue
		}
		if len(execMsgs) == 0 {
			s.log.Info("xsafe: validation no executing messages", "chain", v, "block", targetNum)
		}
		for logIdx, msg := range execMsgs {
			if msg == nil {
				continue
			}
			s.log.Info("xsafe: exec found", "chain", v, "block", targetNum, "logIdx", logIdx, "init_chain", msg.ChainID, "init_block", msg.BlockNum, "init_log", msg.LogIdx, "ts", msg.Timestamp)
		}
		out[v] = execMsgs
	}
	return out
}

// validateExecutingMessages verifies that each executing message references an initiating log present on the initiating chain.
func (s *CrossService) validateExecutingMessages(ctx context.Context, activeChains []eth.ChainID, ts uint64, execByChain map[uint64]map[uint32]*types.ExecutingMessage) bool {
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

				initLogsDB := s.GetLogsDB(initCID)
				if initContainer == nil || initLogsDB == nil {
					s.log.Info("xsafe: validation missing initiating logsDB", "init_chain", initCID)
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
					s.log.Info("xsafe: exec validation failed due to *potential* cycle", "exec_chain", v, "init_chain", initCID, "timestamp", msg.Timestamp)
					allValid = false
					invalidCount++
					if !rolledBack[v] {
						s.mu.Lock()
						container := s.chains[v]
						s.mu.Unlock()

						execLogsDB := s.GetLogsDB(v)
						if container != nil && container.VirtualCfg != nil && container.VirtualCfg.Rcfg != nil && execLogsDB != nil {
							if targetNum, terr := container.VirtualCfg.Rcfg.TargetBlockNumber(ts); terr == nil {
								if ref, _, _, oerr := execLogsDB.OpenBlock(targetNum); oerr == nil {
									if s.denylist != nil {
										_ = s.denylist.Add(v, ref.Time, ref.Hash.Hex())
										s.log.Info("xsafe: denylist add", "chain", v, "block", ref.Hash, "num", targetNum)
									}
								}
							}
						}
					}
				} else if _, err := initLogsDB.Contains(query); err != nil {
					s.log.Info("xsafe: exec validation failed", "exec_chain", v, "init_chain", initCID, "err", err)
					allValid = false
					invalidCount++
					// Side-effects: mark denylist and rollback the executing chain before the block at this ts
					if !rolledBack[v] {
						s.mu.Lock()
						container := s.chains[v]
						s.mu.Unlock()

						execLogsDB := s.GetLogsDB(v)
						if container != nil && container.VirtualCfg != nil && container.VirtualCfg.Rcfg != nil && execLogsDB != nil {
							if targetNum, terr := container.VirtualCfg.Rcfg.TargetBlockNumber(ts); terr == nil {
								if ref, _, _, oerr := execLogsDB.OpenBlock(targetNum); oerr == nil {
									if s.denylist != nil {
										_ = s.denylist.Add(v, ref.Time, ref.Hash.Hex())
										s.log.Info("xsafe: denylist add", "chain", v, "block", ref.Hash, "num", targetNum)
									}
								}
								to := uint64(0)
								if targetNum > 0 {
									to = targetNum - 1
								}
								if s.rollbackFn != nil {
									if rerr := s.rollbackFn(ctx, v, to); rerr != nil {
										s.log.Warn("xsafe: rollback failed", "chain", v, "to", to, "err", rerr)
									} else {
										s.log.Info("xsafe: rollback executed", "chain", v, "to", to)
									}
								}
								rolledBack[v] = true
							}
						}
					}
				} else {
					s.log.Info("xsafe: exec validation ok", "exec_chain", v, "init_chain", initCID)
				}
			}
		}
	}
	s.log.Info("xsafe: exec validation summary", "total", totalCount, "invalid", invalidCount)
	return allValid
}
