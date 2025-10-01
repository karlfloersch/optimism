package driver

import (
	"context"
	"fmt"
	"time"

	"github.com/ethereum/go-ethereum/log"

	"github.com/ethereum-optimism/optimism/op-node/rollup"
	"github.com/ethereum-optimism/optimism/op-service/eth"
)

// L2Source is the interface for querying L2 blocks and payloads from remote/local EL
type L2Source interface {
	L2BlockRefByLabel(ctx context.Context, label eth.BlockLabel) (eth.L2BlockRef, error)
	L2BlockRefByNumber(ctx context.Context, num uint64) (eth.L2BlockRef, error)
	PayloadByNumber(ctx context.Context, number uint64) (*eth.ExecutionPayloadEnvelope, error)
}

// EngineCtrl provides methods to insert blocks and update heads
type EngineCtrl interface {
	InsertUnsafePayload(ctx context.Context, envelope *eth.ExecutionPayloadEnvelope, ref eth.L2BlockRef) error
	PromoteSafe(ctx context.Context, ref eth.L2BlockRef, l1Origin eth.L1BlockRef)
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
	engine EngineCtrl,
	pollInterval time.Duration,
) *LiteModeSync {
	return &LiteModeSync{
		log:          log,
		ctx:          ctx,
		remoteEL:     remoteEL,
		localEL:      localEL,
		engine:       engine,
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

	// Step 3: Update unsafe head from remote
	if err := lm.updateUnsafe(); err != nil {
		return fmt.Errorf("failed to update unsafe head: %w", err)
	}

	return nil
}


// updateUnsafe is a no-op in lite mode - we don't actively pull unsafe blocks
// Instead, unsafe blocks come from CL sync (P2P gossip) or are promoted from safe
func (lm *LiteModeSync) updateUnsafe() error {
	// In lite mode, we focus on safe/finalized head progression
	// The unsafe head will be managed by:
	// 1. CL sync (if enabled) receiving unsafe blocks via P2P
	// 2. Safe head promotion automatically updating unsafe head
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

	// Optimization: For forward progress (common case), try the next block first
	// This avoids walking backward through hundreds of blocks
	startNum := localSafe.Number + 1
	if remoteSafe.Number < localSafe.Number {
		// Reorg case: start from remoteSafe and walk backward
		startNum = remoteSafe.Number
	}

	// Walk backward from startNum until we find where it connects to our chain
	for currentNum := startNum; currentNum > localFinalized.Number; currentNum-- {
		// Don't try to import beyond what remote considers safe
		if currentNum > remoteSafe.Number {
			continue
		}

		// Fetch the remote block at this height
		remoteBlock, err := lm.remoteEL.L2BlockRefByNumber(lm.ctx, currentNum)
		if err != nil {
			continue // Remote block unavailable, keep walking back
		}

		// Get the parent block number
		parentNum := currentNum - 1
		if parentNum < localFinalized.Number {
			return fmt.Errorf("remote safe chain diverged below finalized (at block %d)", currentNum)
		}

		// Try to get the local parent block
		var localParent eth.L2BlockRef
		var haveParent bool
		if parentNum == localSafe.Number {
			localParent = localSafe
			haveParent = true
		} else if parentNum == localFinalized.Number {
			localParent = localFinalized
			haveParent = true
		} else {
			// Parent is between finalized and safe - try to fetch from local EL
			localParentFromEL, err := lm.localEL.L2BlockRefByNumber(lm.ctx, parentNum)
			if err == nil {
				localParent = localParentFromEL
				haveParent = true
			}
			// If we don't have it locally, keep walking back
		}

		if !haveParent {
			continue
		}

		// Check if this remote block builds on our local parent
		if remoteBlock.ParentHash == localParent.Hash {
			// Found the connection point! Insert this block and promote to safe
			return lm.insertAndPromoteBlock(currentNum, remoteBlock)
		}
		// Hash mismatch - keep walking back
	}

	// Walked all the way back to finalized without finding connection
	return fmt.Errorf("remote safe chain diverged from local chain above finalized")
}

// insertAndPromoteBlock fetches the payload, inserts as unsafe, and promotes to safe
func (lm *LiteModeSync) insertAndPromoteBlock(blockNum uint64, blockRef eth.L2BlockRef) error {
	// Fetch the full payload from remote
	payload, err := lm.remoteEL.PayloadByNumber(lm.ctx, blockNum)
	if err != nil {
		return fmt.Errorf("failed to fetch payload for block %d: %w", blockNum, err)
	}

	// Insert as unsafe
	if err := lm.engine.InsertUnsafePayload(lm.ctx, payload, blockRef); err != nil {
		return fmt.Errorf("failed to insert unsafe payload for block %d: %w", blockNum, err)
	}

	// Promote to safe with dummy L1 origin (no error return)
	dummyL1Origin := eth.L1BlockRef{}
	lm.engine.PromoteSafe(lm.ctx, blockRef, dummyL1Origin)

	lm.log.Info("Lite mode: imported and promoted block to safe",
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
		dummyL1Origin := eth.L1BlockRef{}
		lm.engine.PromoteSafe(lm.ctx, remoteFin, dummyL1Origin)
	}

	return nil
}
