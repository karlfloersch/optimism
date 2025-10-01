package driver

import (
	"context"
	"errors"
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

// RPCClient provides access to the underlying RPC client for custom calls
type RPCClient interface {
	CallContext(ctx context.Context, result interface{}, method string, args ...interface{}) error
}

// EngineCtrl provides methods to insert blocks and update heads
type EngineCtrl interface {
	InsertUnsafePayload(ctx context.Context, envelope *eth.ExecutionPayloadEnvelope, ref eth.L2BlockRef) error
	PromoteSafe(ctx context.Context, ref eth.L2BlockRef, l1Origin eth.L1BlockRef) error
	PromoteFinalized(ctx context.Context, ref eth.L2BlockRef) error
	SafeL2Head() eth.L2BlockRef
	FinalizedL2Head() eth.L2BlockRef
}

// LiteModeSync handles safe/finalized head progression by polling an external RPC
type LiteModeSync struct {
	log          log.Logger
	ctx          context.Context
	remoteEL     L2Source
	remoteRPC    RPCClient
	localEL      L2Source
	localRPC     RPCClient
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
	remoteRPC RPCClient,
	localEL L2Source,
	localRPC RPCClient,
	engine EngineCtrl,
	pollInterval time.Duration,
) *LiteModeSync {
	return &LiteModeSync{
		log:          log,
		ctx:          ctx,
		remoteEL:     remoteEL,
		remoteRPC:    remoteRPC,
		localEL:      localEL,
		localRPC:     localRPC,
		engine:       engine,
		cfg:          cfg,
		pollInterval: pollInterval,
		closeCh:      make(chan struct{}),
	}
}

// Start begins the sync loop
func (lm *LiteModeSync) Start() {
	lm.log.Info("Starting lite mode sync", "poll_interval", lm.pollInterval)
	go lm.syncLoop()
}

// Close stops the sync loop
func (lm *LiteModeSync) Close() {
	close(lm.closeCh)
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
	// Step 1: Check if local EL is syncing
	syncing, err := lm.isELSyncing()
	if err != nil {
		return fmt.Errorf("failed to check EL sync status: %w", err)
	}
	if syncing {
		lm.log.Debug("Skipping lite mode sync - local EL is syncing")
		return nil
	}

	// Step 2: Find and import next safe block
	if err := lm.findAndImportNextSafe(); err != nil {
		return fmt.Errorf("failed to import next safe block: %w", err)
	}

	// Step 3: Update finalized head
	if err := lm.updateFinalized(); err != nil {
		return fmt.Errorf("failed to update finalized head: %w", err)
	}

	return nil
}

// isELSyncing checks if the local execution layer is syncing
func (lm *LiteModeSync) isELSyncing() (bool, error) {
	var result interface{}
	err := lm.localRPC.CallContext(lm.ctx, &result, "eth_syncing")
	if err != nil {
		return false, err
	}
	// eth_syncing returns false when not syncing, or a sync status object when syncing
	if result == nil || result == false {
		return false, nil
	}
	return true, nil
}

// findAndImportNextSafe walks back both chains to find common ancestor and imports next block
func (lm *LiteModeSync) findAndImportNextSafe() error {
	localSafe := lm.engine.SafeL2Head()
	currentNum := localSafe.Number + 1

	for {
		// Try to fetch the remote block at currentNum
		remoteBlock, err := lm.remoteEL.L2BlockRefByNumber(lm.ctx, currentNum)
		if err != nil {
			// If remote block doesn't exist, walk back
			if currentNum == 0 {
				return errors.New("reached genesis without finding common ancestor")
			}
			currentNum--
			continue
		}

		// Fetch local parent block (at currentNum - 1)
		if currentNum == 0 {
			return errors.New("cannot fetch parent of genesis block")
		}
		localParent, err := lm.localEL.L2BlockRefByNumber(lm.ctx, currentNum-1)
		if err != nil {
			// This should never happen - indicates corrupted local state
			return fmt.Errorf("local block missing at height %d (should exist): %w", currentNum-1, err)
		}

		// Check if parent hashes match
		if remoteBlock.ParentHash == localParent.Hash {
			// Found common ancestor! Import this block and promote to safe
			return lm.insertAndPromoteBlock(currentNum, remoteBlock)
		}

		// Hash mismatch - walk back both chains
		if currentNum == 0 {
			return errors.New("reached genesis without finding common ancestor")
		}
		currentNum--
	}
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

	// Promote to safe with dummy L1 origin
	dummyL1Origin := eth.L1BlockRef{}
	if err := lm.engine.PromoteSafe(lm.ctx, blockRef, dummyL1Origin); err != nil {
		return fmt.Errorf("failed to promote safe for block %d: %w", blockNum, err)
	}

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

	localFin := lm.engine.FinalizedL2Head()

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

	// Promote to finalized
	if err := lm.engine.PromoteFinalized(lm.ctx, remoteFin); err != nil {
		return fmt.Errorf("failed to promote finalized for block %d: %w", remoteFin.Number, err)
	}

	lm.log.Info("Lite mode: promoted block to finalized",
		"number", remoteFin.Number,
		"hash", remoteFin.Hash,
	)

	return nil
}
