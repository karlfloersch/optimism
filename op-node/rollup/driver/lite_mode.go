package driver

import (
	"context"
	"fmt"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/log"

	"github.com/ethereum-optimism/optimism/op-node/rollup"
	"github.com/ethereum-optimism/optimism/op-node/rollup/engine"
	"github.com/ethereum-optimism/optimism/op-service/eth"
	"github.com/ethereum-optimism/optimism/op-service/event"
)

// L2Source is the interface for querying L2 blocks and payloads from remote/local EL
type L2Source interface {
	L2BlockRefByLabel(ctx context.Context, label eth.BlockLabel) (eth.L2BlockRef, error)
	L2BlockRefByNumber(ctx context.Context, num uint64) (eth.L2BlockRef, error)
	L2BlockRefByNumberHeaderOnly(ctx context.Context, num uint64) (eth.L2BlockRef, error)
	PayloadByNumber(ctx context.Context, number uint64) (*eth.ExecutionPayloadEnvelope, error)
}

// EngineCtrl provides methods to update heads
type EngineCtrl interface {
	PromoteFinalized(ctx context.Context, ref eth.L2BlockRef)
	SafeL2Head() eth.L2BlockRef
	Finalized() eth.L2BlockRef
}

// LiteModeSync handles safe/finalized head progression by polling an external RPC
type LiteModeSync struct {
	log          log.Logger
	ctx          context.Context
	remoteEL     L2Source
	localEL      L2Source
	engine       EngineCtrl
	emitter      event.Emitter
	cfg          *rollup.Config
	pollInterval time.Duration
	closeCh      chan struct{}
}

// NewLiteModeSync creates a new lite mode sync component
func NewLiteModeSync(
	ctx context.Context,
	log log.Logger,
	cfg *rollup.Config,
	remoteEL L2Source,
	localEL L2Source,
	eng EngineCtrl,
	emitter event.Emitter,
	pollInterval time.Duration,
) *LiteModeSync {
	return &LiteModeSync{
		log:          log,
		ctx:          ctx,
		remoteEL:     remoteEL,
		localEL:      localEL,
		engine:       eng,
		emitter:      emitter,
		cfg:          cfg,
		pollInterval: pollInterval,
		closeCh:      make(chan struct{}),
	}
}

// Start begins the sync loop
func (lm *LiteModeSync) Start() {
	lm.log.Info("Starting lite mode sync", "poll_interval", lm.pollInterval)
	//  Note: Initial sync is handled in the sync loop itself to ensure
	// it happens after the engine controller has loaded the finalized head
	go lm.syncLoop()
}

// Close stops the sync loop
func (lm *LiteModeSync) Close() {
	select {
	case <-lm.closeCh:
		// Already closed
		return
	default:
		close(lm.closeCh)
	}
}

// syncLoop is the main sync loop that runs on a timer
func (lm *LiteModeSync) syncLoop() {
	ticker := time.NewTicker(lm.pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-lm.ctx.Done():
			lm.log.Info("Lite mode sync loop stopped (context done)")
			return
		case <-lm.closeCh:
			lm.log.Info("Lite mode sync loop stopped (close requested)")
			return
		case <-ticker.C:
			if err := lm.syncStep(); err != nil {
				lm.log.Error("Lite mode sync step failed", "err", err)
			}
		}
	}
}

// syncStep performs one iteration of the sync loop
func (lm *LiteModeSync) syncStep() error {
	// Step 1: Update finalized head (may also catch up safe head)
	if err := lm.updateFinalized(); err != nil {
		return fmt.Errorf("failed to update finalized head: %w", err)
	}

	// Step 2: Find and import next safe block
	if err := lm.findAndImportNextSafe(); err != nil {
		return fmt.Errorf("failed to import next safe block: %w", err)
	}

	// Note: Unsafe head is managed by CL sync (P2P gossip) and safe head promotion

	return nil
}
// findAndImportNextSafe walks backward from remote safe to find where it connects to our chain
func (lm *LiteModeSync) findAndImportNextSafe() error {
	localSafe := lm.engine.SafeL2Head()
	localFinalized := lm.engine.Finalized()

	// Fetch what the remote considers safe
	remoteSafe, err := lm.remoteEL.L2BlockRefByLabel(lm.ctx, eth.Safe)
	if err != nil {
		return fmt.Errorf("failed to fetch remote safe head: %w", err)
	}

	// Don't move safe below finalized
	if remoteSafe.Number < localFinalized.Number {
		return nil
	}

	// If we're already at the target, nothing to do
	if localSafe.Number == remoteSafe.Number && localSafe.Hash == remoteSafe.Hash {
		return nil
	}

	// Determine starting point for backward walk
	var startNum uint64
	if remoteSafe.Number < localSafe.Number {
		// Reorg case: remote is behind, start from remoteSafe
		startNum = remoteSafe.Number
	} else {
		// Forward progress: remote is ahead, start from next block
		// This avoids walking backward through hundreds of blocks
		startNum = localSafe.Number + 1
	}

	// Walk backward from startNum until we find where it connects to our chain
	for currentNum := startNum; currentNum > localFinalized.Number; currentNum-- {
		// Fetch the remote block header (optimization: no full transaction data)
		remoteBlock, err := lm.remoteEL.L2BlockRefByNumberHeaderOnly(lm.ctx, currentNum)
		if err != nil {
			continue // Remote block unavailable, keep walking back
		}

		// Get the parent block number
		parentNum := currentNum - 1
		if parentNum < localFinalized.Number {
			return fmt.Errorf("remote safe chain diverged below finalized (at block %d)", currentNum)
		}

		// Get the parent block hash from our local chain
		// We should have all blocks between finalized and safe, so if we don't find it, that's an error
		localParentHash, found := lm.getLocalBlockHash(parentNum)
		if !found {
			return fmt.Errorf("missing local block %d during safe head sync (safe=%d, finalized=%d)",
				parentNum, localSafe.Number, localFinalized.Number)
		}

		// Check if this remote block builds on our local parent
		if remoteBlock.ParentHash == localParentHash {
			// Found the connection point! Now fetch full block data and insert
			remoteBlockRef, err := lm.remoteEL.L2BlockRefByNumber(lm.ctx, currentNum)
			if err != nil {
				return fmt.Errorf("failed to fetch full block data for insertion: %w", err)
			}
			return lm.insertAndPromoteBlock(currentNum, remoteBlockRef)
		}
		// Hash mismatch - keep walking back
	}

	// Walked all the way back to finalized without finding connection
	return fmt.Errorf("remote safe chain diverged from local chain above finalized")
}

// getLocalBlockHash returns the hash of a local block by number.
// It checks in-memory heads first (safe, finalized) before querying the local EL.
// Returns (hash, true) if found locally, (zero, false) if not found.
func (lm *LiteModeSync) getLocalBlockHash(num uint64) (common.Hash, bool) {
	localSafe := lm.engine.SafeL2Head()
	localFinalized := lm.engine.Finalized()

	// Fast path: check in-memory heads first
	if num == localSafe.Number {
		return localSafe.Hash, true
	}
	if num == localFinalized.Number {
		return localFinalized.Hash, true
	}

	// Slow path: block is between finalized and safe, fetch from local EL
	block, err := lm.localEL.L2BlockRefByNumberHeaderOnly(lm.ctx, num)
	if err == nil {
		return block.Hash, true
	}

	return common.Hash{}, false
}

// insertAndPromoteBlock fetches the payload and emits an event to process it
// This follows the same pattern as normal derivation - emit PayloadProcessEvent
func (lm *LiteModeSync) insertAndPromoteBlock(blockNum uint64, blockRef eth.L2BlockRef) error {
	// Fetch the full payload from remote
	payload, err := lm.remoteEL.PayloadByNumber(lm.ctx, blockNum)
	if err != nil {
		return fmt.Errorf("failed to fetch payload for block %d: %w", blockNum, err)
	}

	// Emit PayloadProcessEvent with Concluding=true to promote it to safe
	// The EngineController will handle inserting via NewPayload and promoting to safe
	// This is the same pattern used by normal derivation
	lm.emitter.Emit(lm.ctx, engine.PayloadProcessEvent{
		Concluding:   true, // This block should become safe
		DerivedFrom:  eth.L1BlockRef{}, // Dummy L1 origin for lite mode
		BuildStarted: time.Now(),
		Envelope:     payload,
		Ref:          blockRef,
	})

	lm.log.Info("Lite mode: emitted payload for processing",
		"number", blockRef.Number,
		"hash", blockRef.Hash,
	)

	return nil
}

// updateFinalized updates the finalized head if remote is ahead
func (lm *LiteModeSync) updateFinalized() error {
	// Fetch remote finalized head
	remoteFin, err := lm.remoteEL.L2BlockRefByLabel(lm.ctx, eth.Finalized)
	if err != nil {
		return fmt.Errorf("failed to fetch remote finalized head: %w", err)
	}

	localFin := lm.engine.Finalized()

	// Only update if remote is ahead
	if remoteFin.Number <= localFin.Number {
		return nil
	}

	// Fetch local block at remote finalized number
	localBlock, err := lm.localEL.L2BlockRefByNumber(lm.ctx, remoteFin.Number)
	if err != nil {
		// Block doesn't exist locally yet - will be imported by safe head progression
		return nil
	}

	// Check if hashes match
	if localBlock.Hash != remoteFin.Hash {
		lm.log.Warn("Finalized hash mismatch - reorg detected",
			"number", remoteFin.Number,
			"local_hash", localBlock.Hash,
			"remote_hash", remoteFin.Hash,
		)
		// Don't promote - will resolve as safe head progresses
		return nil
	}

	// Promote to finalized (no error return)
	lm.engine.PromoteFinalized(lm.ctx, remoteFin)

	lm.log.Info("Lite mode: promoted block to finalized",
		"number", remoteFin.Number,
		"hash", remoteFin.Hash,
	)

	// If safe head is behind finalized after this update, catch it up
	// This can happen when finalized jumps ahead (e.g., epoch boundary)
	// while safe is still progressing block-by-block
	localSafe := lm.engine.SafeL2Head()
	if localSafe.Number < remoteFin.Number {
		lm.log.Info("Lite mode: catching up safe head to finalized",
			"old_safe", localSafe.Number,
			"finalized", remoteFin.Number)
		// Emit LocalSafeUpdateEvent to promote safe to finalized
		// This triggers the same flow as normal derivation
		lm.emitter.Emit(lm.ctx, engine.LocalSafeUpdateEvent{
			Ref:    remoteFin,
			Source: eth.L1BlockRef{}, // Dummy L1 origin for lite mode
		})
	}

	return nil
}
