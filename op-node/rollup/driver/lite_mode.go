package driver

import (
	"context"
	"fmt"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/log"

	"github.com/ethereum-optimism/optimism/op-node/rollup"
	"github.com/ethereum-optimism/optimism/op-service/eth"
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
	InsertUnsafePayload(ctx context.Context, envelope *eth.ExecutionPayloadEnvelope, ref eth.L2BlockRef) error
	PromoteSafe(ctx context.Context, ref eth.L2BlockRef, source eth.L1BlockRef)
	PromoteFinalized(ctx context.Context, ref eth.L2BlockRef)
	SafeL2Head() eth.L2BlockRef
	Finalized() eth.L2BlockRef
}

// LiteModeSync handles safe/finalized head progression by polling an external RPC
type LiteModeSync struct {
	log      log.Logger
	ctx      context.Context
	remoteEL L2Source
	localEL  L2Source
	engine   EngineCtrl
	cfg      *rollup.Config
}

// NewLiteModeSync creates a new lite mode sync component
func NewLiteModeSync(
	ctx context.Context,
	log log.Logger,
	cfg *rollup.Config,
	remoteEL L2Source,
	localEL L2Source,
	eng EngineCtrl,
) *LiteModeSync {
	return &LiteModeSync{
		log:      log,
		ctx:      ctx,
		remoteEL: remoteEL,
		localEL:  localEL,
		engine:   eng,
		cfg:      cfg,
	}
}

// SyncStep performs one iteration of sync by polling the remote RPC for safe/finalized heads.
// Returns (madeProgress, error) where madeProgress indicates if we actually synced new blocks.
func (lm *LiteModeSync) SyncStep() (bool, error) {
	madeProgress := false

	// Step 1: Update finalized head (may also catch up safe head)
	finalizedProgress, err := lm.updateFinalized()
	if err != nil {
		return false, fmt.Errorf("failed to update finalized head: %w", err)
	}
	madeProgress = madeProgress || finalizedProgress

	// Step 2: Find and import next safe block
	safeProgress, err := lm.findAndImportNextSafe()
	if err != nil {
		return false, fmt.Errorf("failed to import next safe block: %w", err)
	}
	madeProgress = madeProgress || safeProgress

	// Note: Unsafe head is managed by CL sync (P2P gossip) and safe head promotion

	return madeProgress, nil
}
// findAndImportNextSafe walks backward from remote safe to find where it connects to our chain.
// Returns (madeProgress, error) where madeProgress indicates if we imported a new block.
func (lm *LiteModeSync) findAndImportNextSafe() (bool, error) {
	localSafe := lm.engine.SafeL2Head()
	localFinalized := lm.engine.Finalized()

	// Fetch what the remote considers safe
	remoteSafe, err := lm.remoteEL.L2BlockRefByLabel(lm.ctx, eth.Safe)
	if err != nil {
		return false, fmt.Errorf("failed to fetch remote safe head: %w", err)
	}

	// Don't move safe below finalized
	if remoteSafe.Number < localFinalized.Number {
		return false, fmt.Errorf("remote safe head is behind local finalized head (remote_safe=%d, local_finalized=%d)",
			remoteSafe.Number, localFinalized.Number)
	}

	// If we're already at the target, nothing to do
	if localSafe.Number == remoteSafe.Number && localSafe.Hash == remoteSafe.Hash {
		return false, nil
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
			return false, fmt.Errorf("remote safe chain diverged below finalized (at block %d)", currentNum)
		}

		// Get the parent block hash from our local chain
		// We should have all blocks between finalized and safe, so if we don't find it, that's an error
		localParentHash, found := lm.getLocalBlockHash(parentNum)
		if !found {
			return false, fmt.Errorf("missing local block %d during safe head sync (safe=%d, finalized=%d)",
				parentNum, localSafe.Number, localFinalized.Number)
		}

		// Check if this remote block builds on our local parent
		if remoteBlock.ParentHash == localParentHash {
			// Found the connection point! Now fetch full block data and insert
			remoteBlockRef, err := lm.remoteEL.L2BlockRefByNumber(lm.ctx, currentNum)
			if err != nil {
				return false, fmt.Errorf("failed to fetch full block data for insertion: %w", err)
			}
			if err := lm.insertAndPromoteBlock(currentNum, remoteBlockRef); err != nil {
				return false, err
			}
			return true, nil // Successfully imported a block
		}
		// Hash mismatch - keep walking back
	}

	// Walked all the way back to finalized without finding connection
	return false, fmt.Errorf("remote safe chain diverged from local chain above finalized")
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

// insertAndPromoteBlock fetches the payload and inserts it via the engine
func (lm *LiteModeSync) insertAndPromoteBlock(blockNum uint64, blockRef eth.L2BlockRef) error {
	// Fetch the full payload from remote
	payload, err := lm.remoteEL.PayloadByNumber(lm.ctx, blockNum)
	if err != nil {
		return fmt.Errorf("failed to fetch payload for block %d: %w", blockNum, err)
	}

	// Insert the payload as unsafe first
	if err := lm.engine.InsertUnsafePayload(lm.ctx, payload, blockRef); err != nil {
		return fmt.Errorf("failed to insert payload for block %d: %w", blockNum, err)
	}

	lm.log.Info("Lite mode: inserted payload",
		"number", blockRef.Number,
		"hash", blockRef.Hash,
	)

	// Promote to safe (dummy L1 origin for lite mode)
	lm.engine.PromoteSafe(lm.ctx, blockRef, eth.L1BlockRef{})

	lm.log.Info("Lite mode: promoted block to safe",
		"number", blockRef.Number,
		"hash", blockRef.Hash,
	)

	return nil
}

// updateFinalized updates the finalized head if remote is ahead.
// Returns (madeProgress, error) where madeProgress indicates if we updated finalized.
func (lm *LiteModeSync) updateFinalized() (bool, error) {
	// Fetch remote finalized head
	remoteFin, err := lm.remoteEL.L2BlockRefByLabel(lm.ctx, eth.Finalized)
	if err != nil {
		return false, fmt.Errorf("failed to fetch remote finalized head: %w", err)
	}

	localFin := lm.engine.Finalized()

	// Only update if remote is ahead
	if remoteFin.Number <= localFin.Number {
		return false, nil
	}

	// Fetch local block at remote finalized number
	localBlock, err := lm.localEL.L2BlockRefByNumber(lm.ctx, remoteFin.Number)
	if err != nil {
		// Block doesn't exist locally yet - will be imported by safe head progression
		return false, nil
	}

	// Check if hashes match
	if localBlock.Hash != remoteFin.Hash {
		lm.log.Warn("Finalized hash mismatch - reorg detected",
			"number", remoteFin.Number,
			"local_hash", localBlock.Hash,
			"remote_hash", remoteFin.Hash,
		)
		// Don't promote - will resolve as safe head progresses
		return false, nil
	}

	// Promote to finalized (no error return)
	lm.engine.PromoteFinalized(lm.ctx, remoteFin)

	lm.log.Info("Lite mode: promoted block to finalized",
		"number", remoteFin.Number,
		"hash", remoteFin.Hash,
	)

	madeProgress := true

	// If safe head is behind finalized after this update, catch it up
	// This can happen when finalized jumps ahead (e.g., epoch boundary)
	// while safe is still progressing block-by-block
	localSafe := lm.engine.SafeL2Head()
	if localSafe.Number < remoteFin.Number {
		lm.log.Info("Lite mode: catching up safe head to finalized",
			"old_safe", localSafe.Number,
			"finalized", remoteFin.Number)

		// Promote safe to match finalized
		lm.engine.PromoteSafe(lm.ctx, remoteFin, eth.L1BlockRef{})
		lm.log.Info("Lite mode: promoted safe head to match finalized",
			"number", remoteFin.Number,
			"hash", remoteFin.Hash)
	}

	return madeProgress, nil
}
