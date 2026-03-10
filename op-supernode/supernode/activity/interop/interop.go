package interop

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ethereum-optimism/optimism/op-service/eth"
	"github.com/ethereum-optimism/optimism/op-supernode/flags"
	"github.com/ethereum-optimism/optimism/op-supernode/supernode/activity"
	cc "github.com/ethereum-optimism/optimism/op-supernode/supernode/chain_container"
	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/log"
	"github.com/urfave/cli/v2"
)

// Compile-time interface conformance assertions.
var (
	_                  activity.RunnableActivity     = (*Interop)(nil)
	_                  activity.VerificationActivity = (*Interop)(nil)
	backoffPeriod                                    = 1 * time.Second // backoff when chains aren't ready
	errorBackoffPeriod                               = 2 * time.Second // backoff on errors
)

// InteropActivationTimestampFlag is the CLI flag for the interop activation timestamp.
var InteropActivationTimestampFlag = &cli.Uint64Flag{
	Name:  "interop.activation-timestamp",
	Usage: "The timestamp at which interop should start",
	Value: 0,
}

func init() {
	flags.RegisterActivityFlags(InteropActivationTimestampFlag)
}

// chainsReadyResult holds the parallel query results from checkChainsReady.
type chainsReadyResult struct {
	blocks  map[eth.ChainID]eth.BlockID // L2 blocks at the timestamp
	l1Heads map[eth.ChainID]eth.BlockID // per-chain L1 inclusion heads
}

// RoundObservation is a consistent snapshot of the current round's state,
// captured upfront so the decision function operates on immutable data.
type RoundObservation struct {
	LastVerifiedTS *uint64
	LastVerified   *VerifiedResult
	NextTimestamp  uint64
	ChainsReady    bool
	BlocksAtTS     map[eth.ChainID]eth.BlockID
	L1Heads        map[eth.ChainID]eth.BlockID
	L1Consistent   bool
	Paused         bool
}

// Decision represents the outcome of the pure decision function.
type Decision int

const (
	DecisionWait       Decision = iota
	DecisionAdvance
	DecisionInvalidate
	DecisionRewind
	DecisionConflict
)

// StepOutput combines a decision with the verification result (if any).
type StepOutput struct {
	Decision Decision
	Result   Result
}

// queuedReset represents a reset that will be applied at the start of the next cycle.
type queuedReset struct {
	chainID          eth.ChainID
	timestamp        uint64
	invalidatedBlock eth.BlockRef
}

// Interop is a VerificationActivity that can also run background work as a RunnableActivity.
type Interop struct {
	log                 log.Logger
	chains              map[eth.ChainID]cc.ChainContainer
	activationTimestamp uint64
	dataDir             string

	verifiedDB *VerifiedDB
	logsDBs    map[eth.ChainID]LogsDB

	mu      sync.RWMutex
	ctx     context.Context
	cancel  context.CancelFunc
	started bool

	currentL1 eth.BlockID

	verifyFn func(ts uint64, blocksAtTimestamp map[eth.ChainID]eth.BlockID) (Result, error)

	// cycleVerifyFn handles same-timestamp cycle verification.
	// It is called after verifyFn, and its results are merged.
	// Set to verifyCycleMessages by default in New().
	cycleVerifyFn func(ts uint64, blocksAtTimestamp map[eth.ChainID]eth.BlockID) (Result, error)

	// pauseAtTimestamp is used for integration test control only.
	// When non-zero, the activity will return early without processing
	// if the next timestamp to process is >= this value.
	pauseAtTimestamp atomic.Uint64

	l1Source      l1ByNumberSource
	l1Checker     *byNumberConsistencyChecker
	pendingResets []queuedReset
}

func (i *Interop) Name() string {
	return "interop"
}

// New constructs a new Interop activity.
func New(
	log log.Logger,
	activationTimestamp uint64,
	chains map[eth.ChainID]cc.ChainContainer,
	dataDir string,
	l1Source l1ByNumberSource,
) *Interop {
	verifiedDB, err := OpenVerifiedDB(dataDir)
	if err != nil {
		log.Error("failed to open verified DB", "err", err)
		return nil
	}

	// Initialize logsDBs for each chain
	logsDBs := make(map[eth.ChainID]LogsDB)
	for chainID := range chains {
		logsDB, err := openLogsDB(log, chainID, dataDir)
		if err != nil {
			log.Error("failed to open logs DB for chain", "chainID", chainID, "err", err)
			// Clean up already created logsDBs
			for _, db := range logsDBs {
				_ = db.Close()
			}
			_ = verifiedDB.Close()
			return nil
		}
		logsDBs[chainID] = logsDB
	}

	i := &Interop{
		log:                 log,
		chains:              chains,
		verifiedDB:          verifiedDB,
		logsDBs:             logsDBs,
		dataDir:             dataDir,
		activationTimestamp: activationTimestamp,
	}
	// default to using the verifyInteropMessages function
	// (can be overridden by tests)
	i.verifyFn = i.verifyInteropMessages
	i.cycleVerifyFn = i.verifyCycleMessages
	i.l1Source = l1Source
	i.l1Checker = newByNumberConsistencyChecker(l1Source)
	return i
}

// Start begins the Interop activity background loop and blocks until ctx is canceled.
func (i *Interop) Start(ctx context.Context) error {
	i.mu.Lock()
	if i.started {
		i.mu.Unlock()
		<-ctx.Done()
		return ctx.Err()
	}
	i.ctx, i.cancel = context.WithCancel(ctx)
	i.started = true
	i.mu.Unlock()

	// Startup recovery: prune and trim stale state first, then replay the
	// invalidation WAL. This order ensures replayed deny entries aren't
	// immediately pruned by the stale-entry cleanup.
	i.pruneStaleDenyListEntries()
	i.trimLogsDBToVerifiedFrontier()
	if err := i.replayPendingInvalidations(); err != nil {
		i.log.Error("failed to replay pending invalidations", "err", err)
	}

	for {
		select {
		case <-i.ctx.Done():
			return i.ctx.Err()
		default:
			madeProgress, err := i.progressAndRecord()
			if err != nil {
				// Error: back off before next attempt
				i.log.Error("failed to progress and record interop", "err", err)
				time.Sleep(errorBackoffPeriod)
				continue
			}
			if !madeProgress {
				// Chains not ready, back off before next attempt
				time.Sleep(backoffPeriod)
			}
			// Otherwise: immediately ready for next iteration (aggressive catch-up)
		}
	}
}

// Stop stops the Interop activity.
func (i *Interop) Stop(ctx context.Context) error {
	i.mu.Lock()
	defer i.mu.Unlock()
	if i.cancel != nil {
		i.cancel()
	}
	// Close all logsDBs
	for chainID, db := range i.logsDBs {
		if err := db.Close(); err != nil {
			i.log.Error("failed to close logs DB", "chainID", chainID, "err", err)
		}
	}
	if i.verifiedDB != nil {
		return i.verifiedDB.Close()
	}
	return nil
}

// PauseAt sets a timestamp at which the interop activity should pause.
// When the activity encounters this timestamp or any later timestamp, it returns early without processing.
// Uses >= check so that if the activity is already beyond the pause point, it will still stop.
// This function is for integration test control only.
// Pass 0 to clear the pause (equivalent to calling Resume).
func (i *Interop) PauseAt(ts uint64) {
	i.pauseAtTimestamp.Store(ts)
	i.log.Info("interop pause set", "pauseAtTimestamp", ts)
}

// Resume clears any pause timestamp, allowing normal processing to continue.
// This function is for integration test control only.
func (i *Interop) Resume() {
	i.pauseAtTimestamp.Store(0)
	i.log.Info("interop pause cleared")
}

// Decide is a pure function that determines the next action based on observations
// and an optional verification result. No side effects, no I/O.
func Decide(obs RoundObservation, verified *Result) StepOutput {
	if obs.Paused {
		return StepOutput{Decision: DecisionWait}
	}
	if !obs.ChainsReady {
		return StepOutput{Decision: DecisionWait}
	}
	if !obs.L1Consistent {
		return StepOutput{Decision: DecisionRewind}
	}
	if verified == nil || verified.IsEmpty() {
		return StepOutput{Decision: DecisionWait}
	}
	if !verified.IsValid() {
		return StepOutput{Decision: DecisionInvalidate, Result: *verified}
	}
	return StepOutput{Decision: DecisionAdvance, Result: *verified}
}

// progressAndRecord attempts to progress interop and record the result.
// Returns (madeProgress, error) where madeProgress indicates if we advanced the verified timestamp.
func (i *Interop) progressAndRecord() (bool, error) {
	i.applyPendingResets()

	obs, err := i.observeRound()
	if err != nil {
		return false, err
	}

	// If the observation alone determines the outcome, skip expensive verification.
	early := Decide(obs, nil)
	if early.Decision != DecisionWait || !obs.ChainsReady {
		return i.executeDecision(early, obs)
	}

	result, err := i.verify(obs.NextTimestamp, obs.BlocksAtTS)
	if err != nil {
		return false, err
	}

	output := Decide(obs, &result)
	return i.executeDecision(output, obs)
}

// observeRound captures a consistent snapshot of the current round state.
// All reads happen here; the decision function operates on this snapshot.
func (i *Interop) observeRound() (RoundObservation, error) {
	var obs RoundObservation

	lastTS, initialized := i.verifiedDB.LastTimestamp()
	if initialized {
		ts := lastTS
		obs.LastVerifiedTS = &ts
		result, err := i.verifiedDB.Get(lastTS)
		if err != nil {
			return obs, fmt.Errorf("failed to read last verified result: %w", err)
		}
		obs.LastVerified = &result
		obs.NextTimestamp = lastTS + 1
	} else {
		obs.NextTimestamp = i.activationTimestamp
	}

	if pauseTS := i.pauseAtTimestamp.Load(); pauseTS != 0 && obs.NextTimestamp >= pauseTS {
		obs.Paused = true
		return obs, nil
	}

	ready, err := i.checkChainsReady(obs.NextTimestamp)
	if err != nil {
		if errors.Is(err, ethereum.NotFound) {
			obs.ChainsReady = false
			return obs, nil
		}
		return obs, err
	}
	obs.ChainsReady = true
	obs.BlocksAtTS = ready.blocks
	obs.L1Heads = ready.l1Heads

	// Check that all frontier L1 heads AND the accepted L1 head are on the same canonical fork.
	obs.L1Consistent = true
	if i.l1Checker != nil {
		heads := make([]eth.BlockID, 0, len(obs.L1Heads)+1)
		if obs.LastVerified != nil {
			heads = append(heads, obs.LastVerified.L1Inclusion)
		}
		for _, l1 := range obs.L1Heads {
			heads = append(heads, l1)
		}
		same, err := i.l1Checker.SameL1Chain(i.ctx, heads)
		if err != nil {
			return obs, fmt.Errorf("L1 consistency check: %w", err)
		}
		obs.L1Consistent = same
	}

	return obs, nil
}

// verify runs the heavy I/O: log loading, message verification, and cycle detection.
func (i *Interop) verify(ts uint64, blocksAtTS map[eth.ChainID]eth.BlockID) (Result, error) {
	if err := i.loadLogs(ts); err != nil {
		if errors.Is(err, ErrPreviousTimestampNotSealed) {
			i.log.Info("logsDB not ready (likely after reset), returning early", "timestamp", ts, "err", err)
			return Result{}, nil
		}
		if errors.Is(err, ErrStaleLogsDB) {
			i.log.Warn("stale logsDB detected, trimming to verified frontier", "timestamp", ts, "err", err)
			i.trimLogsDBToVerifiedFrontier()
			return Result{}, nil
		}
		return Result{}, fmt.Errorf("failed to load logs: %w", err)
	}

	result, err := i.verifyFn(ts, blocksAtTS)
	if err != nil {
		return Result{}, err
	}

	cycleResult, err := i.cycleVerifyFn(ts, blocksAtTS)
	if err != nil {
		return Result{}, fmt.Errorf("cycle verification failed: %w", err)
	}

	if len(cycleResult.InvalidHeads) > 0 {
		if result.InvalidHeads == nil {
			result.InvalidHeads = make(map[eth.ChainID]eth.BlockID)
		}
		for chainID, invalidBlock := range cycleResult.InvalidHeads {
			result.InvalidHeads[chainID] = invalidBlock
		}
	}

	return result, nil
}

// executeDecision applies the side effects of a decision.
func (i *Interop) executeDecision(output StepOutput, obs RoundObservation) (bool, error) {
	switch output.Decision {
	case DecisionRewind:
		if obs.LastVerifiedTS == nil {
			return false, nil
		}
		if err := i.rewindAccepted(*obs.LastVerifiedTS); err != nil {
			return false, fmt.Errorf("rewind accepted: %w", err)
		}
		i.mu.Lock()
		i.currentL1 = eth.BlockID{}
		i.mu.Unlock()
		return false, nil

	case DecisionInvalidate:
		// Persist intended invalidations before executing them (write-ahead log).
		// If the process crashes mid-execution, pending entries survive for replay on restart.
		pending := make([]PendingInvalidation, 0, len(output.Result.InvalidHeads))
		for chainID, blockID := range output.Result.InvalidHeads {
			pending = append(pending, PendingInvalidation{
				ChainID:   chainID,
				BlockID:   blockID,
				Timestamp: output.Result.Timestamp,
			})
		}
		sort.Slice(pending, func(i, j int) bool {
			return pending[i].ChainID.Cmp(pending[j].ChainID) < 0
		})
		if err := i.verifiedDB.SetPendingInvalidations(pending); err != nil {
			return false, fmt.Errorf("persist pending invalidations: %w", err)
		}
		var failedAny bool
		for _, p := range pending {
			if err := i.invalidateBlock(p.ChainID, p.BlockID, p.Timestamp); err != nil {
				i.log.Error("invalidation failed, WAL preserved for retry on restart",
					"chain", p.ChainID, "block", p.BlockID, "err", err)
				failedAny = true
			}
		}
		if failedAny {
			return false, fmt.Errorf("one or more invalidations failed, WAL preserved")
		}
		if err := i.verifiedDB.ClearPendingInvalidations(); err != nil {
			return false, fmt.Errorf("clear pending invalidations: %w", err)
		}
		return false, nil

	case DecisionAdvance:
		// If a reset arrived during this iteration, skip the commit.
		i.mu.RLock()
		hasPendingResets := len(i.pendingResets) > 0
		i.mu.RUnlock()
		if hasPendingResets {
			i.log.Warn("aborting commit due to pending reset during iteration",
				"timestamp", output.Result.Timestamp)
			return false, nil
		}

		if err := i.commitVerifiedResult(output.Result.Timestamp, output.Result.ToVerifiedResult()); err != nil {
			return false, fmt.Errorf("commit verified result: %w", err)
		}
		i.log.Info("committed verified result", "timestamp", output.Result.Timestamp)
		i.mu.Lock()
		i.currentL1 = output.Result.L1Inclusion
		i.mu.Unlock()
		return true, nil

	case DecisionWait:
		localL1, err := i.collectCurrentL1()
		if err != nil {
			// Non-fatal: just keep existing currentL1
			i.log.Debug("failed to collect current L1 on wait", "err", err)
			return false, nil
		}
		i.mu.Lock()
		i.currentL1 = localL1
		i.mu.Unlock()
		return false, nil

	case DecisionConflict:
		return false, nil
	}

	return false, nil
}

// rewindAccepted rolls back the last verified timestamp and all dependent state:
// verifiedDB entry, deny-list entries, logsDBs, and chain engines (if deny entries were pruned).
func (i *Interop) rewindAccepted(lastTS uint64) error {
	i.log.Warn("rewinding accepted state due to drift", "timestamp", lastTS)

	if _, err := i.verifiedDB.Rewind(lastTS); err != nil {
		return fmt.Errorf("rewind verifiedDB: %w", err)
	}

	// Prune deny-list entries from the rewound decision. Chains with pruned
	// entries need an engine reset so their VNs re-derive without the removed entries.
	sortedChainIDs := make([]eth.ChainID, 0, len(i.chains))
	for chainID := range i.chains {
		sortedChainIDs = append(sortedChainIDs, chainID)
	}
	sort.Slice(sortedChainIDs, func(a, b int) bool {
		return sortedChainIDs[a].Cmp(sortedChainIDs[b]) < 0
	})

	rewindTargetTS, hasPrev := i.verifiedDB.LastTimestamp()
	for _, chainID := range sortedChainIDs {
		chain := i.chains[chainID]
		removed, err := chain.PruneDeniedAtOrAfterTimestamp(lastTS)
		if err != nil {
			i.log.Error("failed to prune deny list on rewind", "chain", chainID, "err", err)
			continue
		}
		if len(removed) > 0 && hasPrev {
			i.log.Info("resetting chain engine after deny-list prune",
				"chain", chainID, "prunedEntries", len(removed), "rewindTo", rewindTargetTS)
			if err := chain.RewindEngine(i.ctx, rewindTargetTS, eth.BlockRef{}); err != nil {
				i.log.Error("failed to reset chain engine on rewind", "chain", chainID, "err", err)
			}
		}
	}

	// Rewind logsDBs to the previous verified frontier.
	if !hasPrev {
		for chainID, db := range i.logsDBs {
			if err := db.Clear(&noopInvalidator{}); err != nil {
				i.log.Error("failed to clear logsDB on full rewind", "chain", chainID, "err", err)
			}
		}
		return nil
	}

	prevResult, err := i.verifiedDB.Get(rewindTargetTS)
	if err != nil {
		// Leave logsDBs as-is; loadLogs will detect stale data on the next round.
		i.log.Error("failed to read previous verified result for logsDB rewind", "err", err)
		return nil
	}

	for chainID, db := range i.logsDBs {
		expectedHead, ok := prevResult.L2Heads[chainID]
		if !ok {
			continue // chain not in previous result — leave as-is
		}
		latestBlock, hasBlocks := db.LatestSealedBlock()
		if !hasBlocks {
			continue // already empty
		}
		if latestBlock == expectedHead {
			continue // already at the right state
		}
		if latestBlock.Number < expectedHead.Number {
			continue // logsDB is behind — it will catch up naturally via loadLogs
		}
		// logsDB is ahead or has wrong hash at same height — rewind to expected head
		i.log.Info("rewinding logsDB to previous verified head",
			"chain", chainID, "from", latestBlock, "to", expectedHead)
		if err := db.Rewind(&noopInvalidator{}, expectedHead); err != nil {
			// Leave as-is rather than clearing (which causes liveness failure).
			// loadLogs stale detection will catch hash mismatches on the next round.
			i.log.Error("failed to rewind logsDB, leaving as-is",
				"chain", chainID, "err", err)
		}
	}

	return nil
}

// replayPendingInvalidations replays any incomplete invalidations from a previous crash.
func (i *Interop) replayPendingInvalidations() error {
	pending, err := i.verifiedDB.GetPendingInvalidations()
	if err != nil {
		return fmt.Errorf("get pending invalidations: %w", err)
	}
	if len(pending) == 0 {
		return nil
	}
	i.log.Warn("replaying pending invalidations from previous crash", "count", len(pending))
	var failedAny bool
	for _, p := range pending {
		if err := i.invalidateBlock(p.ChainID, p.BlockID, p.Timestamp); err != nil {
			i.log.Error("failed to replay invalidation, WAL preserved",
				"chain", p.ChainID, "block", p.BlockID, "err", err)
			failedAny = true
		}
	}
	if failedAny {
		return fmt.Errorf("one or more invalidation replays failed, WAL preserved")
	}
	return i.verifiedDB.ClearPendingInvalidations()
}

// pruneStaleDenyListEntries removes deny-list entries whose DecisionTimestamp
// exceeds the current verified frontier (left over from incomplete rewinds).
func (i *Interop) pruneStaleDenyListEntries() {
	lastTS, initialized := i.verifiedDB.LastTimestamp()
	if !initialized {
		return
	}
	for chainID, chain := range i.chains {
		removed, err := chain.PruneDeniedAtOrAfterTimestamp(lastTS + 1)
		if err != nil {
			i.log.Error("failed to prune stale deny entries on startup", "chain", chainID, "err", err)
		}
		if len(removed) > 0 {
			i.log.Warn("pruned stale deny entries on startup",
				"chain", chainID, "prunedHeights", len(removed), "lastVerifiedTS", lastTS)
		}
	}
}

// trimLogsDBToVerifiedFrontier rewinds any logsDB whose head is beyond or
// inconsistent with the verified frontier (stale data from incomplete rounds).
func (i *Interop) trimLogsDBToVerifiedFrontier() {
	lastTS, initialized := i.verifiedDB.LastTimestamp()

	for chainID, db := range i.logsDBs {
		latestBlock, hasBlocks := db.LatestSealedBlock()
		if !hasBlocks {
			continue
		}

		if !initialized {
			// No verified state but logsDB has data — clear it
			if err := db.Clear(&noopInvalidator{}); err != nil {
				i.log.Error("failed to clear logsDB during trim", "chain", chainID, "err", err)
			}
			continue
		}

		result, err := i.verifiedDB.Get(lastTS)
		if err != nil {
			i.log.Error("failed to read verified result during logsDB trim, leaving as-is",
				"chain", chainID, "timestamp", lastTS, "err", err)
			continue
		}
		expectedHead, ok := result.L2Heads[chainID]
		if !ok {
			continue // chain not in verified result — leave as-is
		}

		if latestBlock.Number > expectedHead.Number ||
			(latestBlock.Number == expectedHead.Number && latestBlock.Hash != expectedHead.Hash) {
			i.log.Info("trimming logsDB to verified frontier",
				"chain", chainID,
				"logsDBHead", latestBlock,
				"verifiedHead", expectedHead,
			)
			if err := db.Rewind(&noopInvalidator{}, expectedHead); err != nil {
				// Leave as-is rather than clearing.
				i.log.Error("failed to rewind logsDB during trim, leaving as-is",
					"chain", chainID, "err", err)
			}
		}
	}
}

// collectCurrentL1 collects the current L1 head of all chains,
// which is the minimum L1 head of all the derivation pipelines in Chain Containers
func (i *Interop) collectCurrentL1() (eth.BlockID, error) {
	var currentL1 eth.BlockID
	first := true
	for _, chain := range i.chains {
		status, err := chain.SyncStatus(i.ctx)
		if err != nil {
			return eth.BlockID{}, fmt.Errorf("chain %s not ready: %w", chain.ID(), err)
		}
		block := status.CurrentL1
		if first || block.Number < currentL1.Number {
			currentL1 = block.ID()
			first = false
		}
	}
	return currentL1, nil
}

// checkChainsReady checks if all chains are ready to process the next timestamp.
// Queries all chains in parallel for better performance.
// Returns both the L2 blocks at the timestamp and the L1 inclusion heads.
func (i *Interop) checkChainsReady(ts uint64) (chainsReadyResult, error) {
	type result struct {
		chainID eth.ChainID
		blockID eth.BlockID
		l1Head  eth.BlockID
		err     error
	}

	results := make(chan result, len(i.chains))

	// Query all chains in parallel
	for _, chain := range i.chains {
		go func(c cc.ChainContainer) {
			// Use OptimisticAt as the single atomic source for both L2 block and L1 head.
			// This avoids a TOCTOU race between separate LocalSafeBlockAtTimestamp and OptimisticAt calls.
			l2Block, l1Block, err := c.OptimisticAt(i.ctx, ts)
			if err != nil {
				results <- result{chainID: c.ID(), err: fmt.Errorf("chain %s not ready for timestamp %d: %w", c.ID(), ts, err)}
				return
			}
			results <- result{chainID: c.ID(), blockID: l2Block, l1Head: l1Block}
		}(chain)
	}

	// Collect all results before returning so every goroutine completes before the
	// next call spawns a new batch, preventing accumulation of in-flight RPC calls.
	ready := chainsReadyResult{
		blocks:  make(map[eth.ChainID]eth.BlockID),
		l1Heads: make(map[eth.ChainID]eth.BlockID),
	}
	var firstErr error
	for range i.chains {
		r := <-results
		if r.err != nil {
			if firstErr == nil {
				firstErr = r.err
			}
		} else {
			ready.blocks[r.chainID] = r.blockID
			ready.l1Heads[r.chainID] = r.l1Head
		}
	}
	if firstErr != nil {
		return chainsReadyResult{}, firstErr
	}

	return ready, nil
}

func (i *Interop) commitVerifiedResult(timestamp uint64, verifiedResult VerifiedResult) error {
	return i.verifiedDB.Commit(verifiedResult)
}

// CurrentL1 returns the L1 block which has been fully considered for interop,
// whether or not it advanced the verified timestamp.
func (i *Interop) CurrentL1() eth.BlockID {
	i.mu.RLock()
	defer i.mu.RUnlock()
	return i.currentL1
}

// VerifiedAtTimestamp returns whether the data is verified at the given timestamp.
// For timestamps before the activation timestamp, this returns true since interop
// wasn't active yet and verification proceeds automatically.
// For timestamps at or after the activation timestamp, this checks the verifiedDB.
func (i *Interop) VerifiedAtTimestamp(ts uint64) (bool, error) {
	// Timestamps before the activation timestamp are considered verified
	// because interop wasn't active yet
	if ts < i.activationTimestamp {
		return true, nil
	}
	return i.verifiedDB.Has(ts)
}

// LatestVerifiedL3Block returns the latest L2 block which has been verified,
// along with the timestamp at which it was verified.
func (i *Interop) LatestVerifiedL2Block(chainID eth.ChainID) (eth.BlockID, uint64) {
	emptyBlock := eth.BlockID{}
	ts, ok := i.verifiedDB.LastTimestamp()
	if !ok {
		return emptyBlock, 0
	}
	res, err := i.verifiedDB.Get(ts)
	if err != nil {
		return emptyBlock, 0
	}
	head, ok := res.L2Heads[chainID]
	if !ok {
		return emptyBlock, 0
	}
	return head, ts
}

// VerifiedBlockAtL1 returns the verified L2 block and timestamp
// which guarantees that the verified data at that pauseAtTimestamp
// originates from or before the supplied L1 block.
func (i *Interop) VerifiedBlockAtL1(chainID eth.ChainID, l1Block eth.L1BlockRef) (eth.BlockID, uint64) {
	// If L1 block is empty/zero (e.g. during startup before FinalizedL1 is set),
	// no verified result can match, so return early.
	if l1Block == (eth.L1BlockRef{}) {
		return eth.BlockID{}, 0
	}

	// Get the last verified timestamp
	lastTs, ok := i.verifiedDB.LastTimestamp()
	if !ok {
		return eth.BlockID{}, 0
	}

	// Search backwards from the last timestamp to find the latest result
	// where the L1 inclusion block is at or below the supplied L1 block number.
	// Stop at activationTimestamp — no verified results exist before that.
	lowerBound := i.activationTimestamp
	for ts := lastTs; ts >= lowerBound && ts <= lastTs; ts-- {
		result, err := i.verifiedDB.Get(ts)
		if err != nil {
			// Timestamp might not exist (due to gaps or rewinds), continue searching
			continue
		}

		// Check if this result's L1 inclusion is at or below the supplied L1 block number
		if result.L1Inclusion.Number <= l1Block.Number {
			// Found a finalized result, return the L2 head for this chain
			head, ok := result.L2Heads[chainID]
			if !ok {
				return eth.BlockID{}, 0
			}
			return head, ts
		}
	}

	// No verified block found
	return eth.BlockID{}, 0
}

// Reset is called when a chain container resets due to an invalidated block.
// It queues the reset for application at the start of the next cycle.
func (i *Interop) Reset(chainID eth.ChainID, timestamp uint64, invalidatedBlock eth.BlockRef) {
	i.mu.Lock()
	defer i.mu.Unlock()

	i.log.Warn("reset queued",
		"chainID", chainID,
		"timestamp", timestamp,
		"invalidatedBlock", invalidatedBlock,
	)

	i.pendingResets = append(i.pendingResets, queuedReset{
		chainID:          chainID,
		timestamp:        timestamp,
		invalidatedBlock: invalidatedBlock,
	})
	// If not started yet, apply immediately (for pre-start resets during construction)
	if !i.started {
		i.applyPendingResetsLocked()
	}
}

// applyPendingResets drains queued resets at the start of each cycle.
func (i *Interop) applyPendingResets() {
	i.mu.Lock()
	defer i.mu.Unlock()
	i.applyPendingResetsLocked()
}

func (i *Interop) applyPendingResetsLocked() {
	if len(i.pendingResets) == 0 {
		return
	}
	for _, reset := range i.pendingResets {
		i.applyResetLocked(reset)
	}
	i.pendingResets = nil
}

func (i *Interop) applyResetLocked(reset queuedReset) {
	// Empty BlockRef signals a reset triggered by rewindAccepted's engine reset,
	// which has already handled the state changes. Skip to avoid double-processing.
	if reset.invalidatedBlock == (eth.BlockRef{}) {
		i.log.Info("ignoring controller-originated reset callback",
			"chainID", reset.chainID, "timestamp", reset.timestamp)
		return
	}

	db, ok := i.logsDBs[reset.chainID]
	if !ok {
		i.log.Error("logsDB not found for reset", "chainID", reset.chainID)
		return
	}
	i.resetLogsDB(reset.chainID, db, reset.invalidatedBlock)
	i.resetVerifiedDB(reset.timestamp)
	i.currentL1 = eth.BlockID{}
}

// invalidateBlock notifies the chain container to add the block to the denylist
// and potentially rewind if the chain is currently using that block.
func (i *Interop) invalidateBlock(chainID eth.ChainID, blockID eth.BlockID, decisionTimestamp uint64) error {
	chain, ok := i.chains[chainID]
	if !ok {
		return fmt.Errorf("chain %s not found", chainID)
	}
	_, err := chain.InvalidateBlock(i.ctx, blockID.Number, blockID.Hash, decisionTimestamp)
	return err
}

// resetLogsDB rewinds or clears the logsDB for a chain to the block before the invalidated block.
// The invalidatedBlock provides the block info directly, avoiding RPC calls during reset.
func (i *Interop) resetLogsDB(chainID eth.ChainID, db LogsDB, invalidatedBlock eth.BlockRef) {
	// The target block is the parent of the invalidated block
	targetBlockID := eth.BlockID{
		Hash:   invalidatedBlock.ParentHash,
		Number: invalidatedBlock.Number - 1,
	}

	i.log.Info("resetLogsDB: computing target from invalidated block",
		"chainID", chainID,
		"invalidatedBlock", invalidatedBlock.Number,
		"targetBlock", targetBlockID.Number,
	)

	// Check the first block in the logsDB to decide whether to clear or rewind
	firstBlock, err := db.FirstSealedBlock()
	if err != nil {
		// If logsDB is empty or has an error, clear it
		i.log.Info("logsDB appears empty or errored, clearing", "chainID", chainID, "err", err)
		if clearErr := db.Clear(&noopInvalidator{}); clearErr != nil {
			i.log.Error("failed to clear logsDB", "chainID", chainID, "err", clearErr)
		}
		return
	}

	if firstBlock.Number > targetBlockID.Number {
		i.log.Info("logsDB is to be cleared", "chainID", chainID, "firstBlock", firstBlock.Number, "targetBlock", targetBlockID.Number)
		if err := db.Clear(&noopInvalidator{}); err != nil {
			i.log.Error("failed to clear logsDB", "chainID", chainID, "err", err)
		}
	} else {
		i.log.Info("logsDB is to be rewound", "chainID", chainID, "targetBlock", targetBlockID.Number, "firstBlock", firstBlock.Number)
		if err := db.Rewind(&noopInvalidator{}, targetBlockID); err != nil {
			i.log.Error("failed to rewind logsDB", "chainID", chainID, "err", err)
		}
	}
}

// resetVerifiedDB removes any verified results after the given timestamp.
func (i *Interop) resetVerifiedDB(timestamp uint64) {
	if i.verifiedDB == nil {
		return
	}

	deleted, err := i.verifiedDB.RewindAfter(timestamp)
	if err != nil {
		i.log.Error("failed to rewind verifiedDB",
			"timestamp", timestamp,
			"err", err,
		)
	}
	if deleted {
		// This is unexpected - we shouldn't have verified results at timestamps
		// that are being reset. Log an error for visibility.
		i.log.Error("UNEXPECTED: verified results were deleted on reset",
			"timestamp", timestamp,
		)
	}
}
