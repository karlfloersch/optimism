package interop

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ethereum-optimism/optimism/op-service/eth"
	"github.com/ethereum-optimism/optimism/op-supernode/flags"
	"github.com/ethereum-optimism/optimism/op-supernode/supernode/activity"
	interopcontroller "github.com/ethereum-optimism/optimism/op-supernode/supernode/activity/interop/controller"
	interopengine "github.com/ethereum-optimism/optimism/op-supernode/supernode/activity/interop/engine"
	interopstore "github.com/ethereum-optimism/optimism/op-supernode/supernode/activity/interop/store"
	cc "github.com/ethereum-optimism/optimism/op-supernode/supernode/chain_container"
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

// Interop is a VerificationActivity that can also run background work as a RunnableActivity.
type Interop struct {
	log                 log.Logger
	chains              map[eth.ChainID]cc.ChainContainer
	activationTimestamp uint64
	dataDir             string

	logsDBs       map[eth.ChainID]LogsDB
	pendingResets []queuedReset

	mu      sync.RWMutex
	ctx     context.Context
	cancel  context.CancelFunc
	started bool

	currentL1 eth.BlockID

	verifyFn func(ts uint64, blocksAtTimestamp map[eth.ChainID]eth.BlockID) (Result, error)

	// cycleVerifyFn handles same-timestamp cycle verification.
	// It is called after verifyFn in progressInterop, and its results are merged.
	// Set to verifyCycleMessages by default in New().
	cycleVerifyFn func(ts uint64, blocksAtTimestamp map[eth.ChainID]eth.BlockID) (Result, error)

	// pauseAtTimestamp is used for integration test control only.
	// When non-zero, progressInterop will return early without processing
	// if the next timestamp to process is >= this value.
	pauseAtTimestamp atomic.Uint64

	stateStore *interopstore.Store
	controller *interopcontroller.Controller
	engine     *interopengine.Engine
}

type queuedReset struct {
	chainID          eth.ChainID
	timestamp        uint64
	invalidatedBlock eth.BlockRef
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

	stateStore, err := interopstore.Open(dataDir)
	if err != nil {
		for _, db := range logsDBs {
			_ = db.Close()
		}
		_ = verifiedDB.Close()
		log.Error("failed to open interop state store", "err", err)
		return nil
	}

	engine, err := interopengine.New(interopengine.Config{ActivationTimestamp: activationTimestamp})
	if err != nil {
		for _, db := range logsDBs {
			_ = db.Close()
		}
		_ = stateStore.Close()
		_ = verifiedDB.Close()
		log.Error("failed to initialize interop engine", "err", err)
		return nil
	}

	i := &Interop{
		log:                 log,
		chains:              chains,
		logsDBs:             logsDBs,
		stateStore:          stateStore,
		dataDir:             dataDir,
		activationTimestamp: activationTimestamp,
		engine:              engine,
	}
	// default to using the verifyInteropMessages function
	// (can be overridden by tests)
	i.verifyFn = i.verifyInteropMessages
	i.cycleVerifyFn = i.verifyCycleMessages
	if err := importVerifiedDBIfNeeded(activationTimestamp, verifiedDB, stateStore); err != nil {
		for _, db := range logsDBs {
			_ = db.Close()
		}
		_ = stateStore.Close()
		_ = verifiedDB.Close()
		log.Error("failed to import legacy interop state", "err", err)
		return nil
	}
	if err := verifiedDB.Close(); err != nil {
		for _, db := range logsDBs {
			_ = db.Close()
		}
		_ = stateStore.Close()
		log.Error("failed to close legacy verified DB after import", "err", err)
		return nil
	}
	i.controller = interopcontroller.New(
		activationTimestamp,
		engine,
		stateStore,
		&runtimeObservationSource{interop: i},
		&runtimeEvidenceResolver{interop: i},
		&runtimeVerifier{interop: i},
		&runtimeEffectRunner{interop: i},
	)
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
	if i.stateStore != nil {
		return i.stateStore.Close()
	}
	return nil
}

// PauseAt sets a timestamp at which the interop activity should pause.
// When progressInterop encounters this timestamp or any later timestamp, it returns early without processing.
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

// progressAndRecord attempts to progress interop and record the result.
// Returns (madeProgress, error) where madeProgress indicates if we advanced the verified timestamp.
func (i *Interop) progressAndRecord() (bool, error) {
	if err := i.applyPendingResets(); err != nil {
		return false, err
	}

	// Check the L1s of each chain prior to performing interop
	localCurrentL1, err := i.collectCurrentL1()
	if err != nil {
		i.log.Error("failed to collect current L1", "err", err)
		return false, err
	}

	step, err := i.controller.Step(i.ctx)
	if err != nil {
		i.log.Error("failed to progress interop controller", "err", err)
		return false, err
	}
	i.mu.Lock()
	switch step.Outcome {
	case interopengine.OutcomeAdvance:
		if step.NewState.Accepted != nil {
			i.currentL1 = step.NewState.Accepted.L1Inclusion
		}
	case interopengine.OutcomeWait:
		i.currentL1 = localCurrentL1
	case interopengine.OutcomeRewind:
		i.currentL1 = eth.BlockID{}
	case interopengine.OutcomeNoOp, interopengine.OutcomeConflict:
		// Preserve the previous currentL1 on invalidation/conflict paths.
	}
	i.mu.Unlock()
	return step.Outcome == interopengine.OutcomeAdvance, nil
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

// checkChainsReady checks if all chains are ready to process the next timestamp.
// Queries all chains in parallel for better performance.
func (i *Interop) checkChainsReady(ts uint64) (map[eth.ChainID]eth.BlockID, error) {
	type result struct {
		chainID eth.ChainID
		blockID eth.BlockID
		err     error
	}

	results := make(chan result, len(i.chains))

	// Query all chains in parallel
	for _, chain := range i.chains {
		go func(c cc.ChainContainer) {
			block, err := c.LocalSafeBlockAtTimestamp(i.ctx, ts)
			if err != nil {
				results <- result{chainID: c.ID(), err: fmt.Errorf("chain %s not ready for timestamp %d: %w", c.ID(), ts, err)}
				return
			}
			results <- result{chainID: c.ID(), blockID: block.ID()}
		}(chain)
	}

	// Collect all results before returning so every goroutine completes before the
	// next call spawns a new batch, preventing accumulation of in-flight RPC calls.
	blocksAtTimestamp := make(map[eth.ChainID]eth.BlockID)
	var firstErr error
	for range i.chains {
		r := <-results
		if r.err != nil {
			if firstErr == nil {
				firstErr = r.err
			}
		} else {
			blocksAtTimestamp[r.chainID] = r.blockID
		}
	}
	if firstErr != nil {
		return nil, firstErr
	}

	return blocksAtTimestamp, nil
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
// For timestamps at or after the activation timestamp, this checks the state store.
func (i *Interop) VerifiedAtTimestamp(ts uint64) (bool, error) {
	// Timestamps before the activation timestamp are considered verified
	// because interop wasn't active yet
	if ts < i.activationTimestamp {
		return true, nil
	}
	state, err := i.loadStateForRead()
	if err != nil {
		return false, err
	}
	if state.LastValidatedTS == nil || *state.LastValidatedTS < ts {
		return false, nil
	}
	_, ok := state.AcceptedHistory[ts]
	return ok, nil
}

// LatestVerifiedL3Block returns the latest L2 block which has been verified,
// along with the timestamp at which it was verified.
func (i *Interop) LatestVerifiedL2Block(chainID eth.ChainID) (eth.BlockID, uint64) {
	emptyBlock := eth.BlockID{}
	state, err := i.loadStateForRead()
	if err != nil || state.LastValidatedTS == nil {
		return emptyBlock, 0
	}
	snapshot, ok := state.AcceptedHistory[*state.LastValidatedTS]
	if !ok {
		return emptyBlock, 0
	}
	head, ok := snapshot.L2Heads[chainID]
	if !ok {
		return emptyBlock, 0
	}
	return head, snapshot.Timestamp
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

	state, err := i.loadStateForRead()
	if err != nil || state.LastValidatedTS == nil {
		return eth.BlockID{}, 0
	}

	// Search backwards from the last timestamp to find the latest result
	// where the L1 inclusion block is at or below the supplied L1 block number
	for ts := *state.LastValidatedTS + 1; ts > 0; ts-- {
		snapshot, ok := state.AcceptedHistory[ts-1]
		if !ok {
			continue
		}

		// Check if this result's L1 inclusion is at or below the supplied L1 block number
		if snapshot.L1Inclusion.Number <= l1Block.Number {
			// Found a finalized result, return the L2 head for this chain
			head, ok := snapshot.L2Heads[chainID]
			if !ok {
				return eth.BlockID{}, 0
			}
			return head, snapshot.Timestamp
		}
	}

	// No verified block found
	return eth.BlockID{}, 0
}

func (i *Interop) loadStateForRead() (interopengine.InteropState, error) {
	if i.stateStore != nil {
		return i.stateStore.Load()
	}
	return interopengine.InteropState{}, nil
}

// Reset is called when a chain container resets due to an invalidated block.
// It prunes logs/effective interop state for that chain at and after the timestamp.
// The invalidatedBlock contains the block info that triggered the reset.
func (i *Interop) Reset(chainID eth.ChainID, timestamp uint64, invalidatedBlock eth.BlockRef) {
	i.mu.Lock()
	defer i.mu.Unlock()

	i.log.Warn("Reset called",
		"chainID", chainID,
		"timestamp", timestamp,
		"invalidatedBlock", invalidatedBlock,
	)

	i.pendingResets = append(i.pendingResets, queuedReset{
		chainID:          chainID,
		timestamp:        timestamp,
		invalidatedBlock: invalidatedBlock,
	})
	if !i.started {
		i.applyPendingResetsLocked()
	}
}

func (i *Interop) applyPendingResets() error {
	i.mu.Lock()
	defer i.mu.Unlock()
	i.applyPendingResetsLocked()
	return nil
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
	if reset.invalidatedBlock == (eth.BlockRef{}) {
		i.log.Info("ignoring controller-originated reset callback", "chainID", reset.chainID, "timestamp", reset.timestamp)
		return
	}

	db, dbOk := i.logsDBs[reset.chainID]
	if !dbOk {
		i.log.Error("logsDB not found for reset", "chainID", reset.chainID)
		return
	}

	i.resetLogsDB(reset.chainID, db, reset.invalidatedBlock)
	i.resetStateStore(reset.timestamp)

	// Reset the currentL1 to force re-evaluation
	i.currentL1 = eth.BlockID{}
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

	if err := i.rewindLogsDBToHead(chainID, db, targetBlockID); err != nil {
		i.log.Error("failed to rewind logsDB to target head", "chainID", chainID, "targetBlock", targetBlockID, "err", err)
	}
}

// resetStateStore removes accepted interop state after the given timestamp.
func (i *Interop) resetStateStore(timestamp uint64) {
	if i.stateStore == nil {
		return
	}
	state, err := i.stateStore.Load()
	if err != nil {
		i.log.Error("failed to load interop state for reset",
			"timestamp", timestamp,
			"err", err,
		)
		return
	}

	if state.Accepted == nil || state.Accepted.Timestamp <= timestamp {
		return
	}

	var nextAccepted *interopengine.AcceptedSnapshot
	var nextValidated *uint64
	for ts := range state.AcceptedHistory {
		if ts > timestamp {
			delete(state.AcceptedHistory, ts)
		}
	}
	for ts, snapshot := range state.AcceptedHistory {
		if ts <= timestamp && (nextValidated == nil || ts > *nextValidated) {
			snapshotCopy := snapshot
			nextAccepted = &snapshotCopy
			validatedTS := ts
			nextValidated = &validatedTS
		}
	}
	if nextAccepted == nil {
		state.Accepted = nil
		state.LastValidatedTS = nil
		state.DeniedByTS = map[uint64][]interopengine.DeniedDecision{}
	} else {
		state.Accepted = nextAccepted
		state.LastValidatedTS = nextValidated
		for ts := range state.DeniedByTS {
			if ts > timestamp {
				delete(state.DeniedByTS, ts)
			}
		}
	}
	if err := i.stateStore.Commit(state); err != nil {
		i.log.Error("failed to commit interop state reset",
			"timestamp", timestamp,
			"err", err,
		)
	}
}
