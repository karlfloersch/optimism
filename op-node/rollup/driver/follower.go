package driver

import (
	"context"

	"github.com/ethereum/go-ethereum/log"

	"github.com/ethereum-optimism/optimism/op-node/p2p"
	"github.com/ethereum-optimism/optimism/op-node/rollup"
	"github.com/ethereum-optimism/optimism/op-node/rollup/derive"
	"github.com/ethereum-optimism/optimism/op-node/rollup/engine"
	"github.com/ethereum-optimism/optimism/op-service/eth"
	"github.com/ethereum-optimism/optimism/op-service/event"
)

// FollowerModeDeriver processes received safe heads from P2P gossip
// and applies them to the engine controller in follower mode
type FollowerModeDeriver struct {
	log     log.Logger
	cfg     *rollup.Config
	engine  EngineController
	emitter event.Emitter
}

var _ event.Deriver = (*FollowerModeDeriver)(nil)

func NewFollowerModeDeriver(log log.Logger, cfg *rollup.Config, engine EngineController) *FollowerModeDeriver {
	return &FollowerModeDeriver{
		log:    log,
		cfg:    cfg,
		engine: engine,
	}
}

func (f *FollowerModeDeriver) AttachEmitter(em event.Emitter) {
	f.emitter = em
}

func (f *FollowerModeDeriver) OnEvent(ctx context.Context, ev event.Event) bool {
	switch x := ev.(type) {
	case p2p.ReceivedSafeHeadEvent:
		return f.processSafeHead(ctx, x)
	default:
		return false
	}
}

// processSafeHead validates and applies a received safe head to the engine
func (f *FollowerModeDeriver) processSafeHead(ctx context.Context, ev p2p.ReceivedSafeHeadEvent) bool {
	payload := ev.Envelope.ExecutionPayload

	f.log.Info("Follower mode: received safe head from P2P",
		"hash", payload.BlockHash,
		"number", payload.BlockNumber,
		"peer", ev.From)

	// Convert payload to L2 block ref for validation
	ref, err := f.payloadToBlockRef(payload)
	if err != nil {
		f.log.Warn("Failed to convert payload to block ref", "hash", payload.BlockHash, "err", err)
		return true
	}

	// Validate that this safe head builds on our current safe head
	currentSafe := f.engine.SafeL2Head()
	if !f.validateSafeHeadProgression(ref, currentSafe) {
		f.log.Warn("Invalid safe head progression, ignoring",
			"received", ref.ID(),
			"currentSafe", currentSafe.ID())
		return true
	}

	// Apply the safe head to the engine
	f.log.Info("Follower mode: applying safe head to engine",
		"hash", ref.Hash,
		"number", ref.Number)

	f.engine.SetSafeHead(ref)

	// Emit events to notify other components
	f.emitter.Emit(ctx, engine.SafeDerivedEvent{
		Safe:   ref,
		Source: eth.L1BlockRef{}, // No L1 source in follower mode
	})

	f.emitter.Emit(ctx, engine.CrossSafeUpdateEvent{
		CrossSafe: f.engine.SafeL2Head(),
		LocalSafe: f.engine.SafeL2Head(), // Use SafeL2Head for both since we don't have separate local/cross in follower mode
	})

	return true
}

// validateSafeHeadProgression ensures the received safe head builds properly on current safe head
func (f *FollowerModeDeriver) validateSafeHeadProgression(received eth.L2BlockRef, currentSafe eth.L2BlockRef) bool {
	// If we have no current safe head, accept any valid block as the first safe head
	if currentSafe == (eth.L2BlockRef{}) {
		f.log.Info("No current safe head, accepting first safe head", "hash", received.Hash)
		return true
	}

	// The received safe head should either:
	// 1. Be the next block (currentSafe.Number + 1)
	// 2. Or be the same block (reorg/duplicate)
	if received.Number == currentSafe.Number+1 {
		// Next block - validate parent hash
		if received.ParentHash != currentSafe.Hash {
			f.log.Warn("Safe head parent hash mismatch",
				"received_parent", received.ParentHash,
				"current_safe", currentSafe.Hash)
			return false
		}
		return true
	} else if received.Number == currentSafe.Number {
		// Same block number - could be reorg or duplicate
		if received.Hash == currentSafe.Hash {
			f.log.Debug("Received duplicate safe head, ignoring", "hash", received.Hash)
			return false // Don't process duplicates
		} else {
			f.log.Warn("Potential reorg: received different safe head at same height",
				"received", received.ID(),
				"current", currentSafe.ID())
			// For now, reject reorgs in follower mode to keep it simple
			// In production, you might want more sophisticated reorg handling
			return false
		}
	} else if received.Number <= currentSafe.Number {
		// Old block - reject
		f.log.Debug("Received old safe head, ignoring",
			"received_number", received.Number,
			"current_safe_number", currentSafe.Number)
		return false
	} else {
		// Gap in blocks - this could be valid if we're catching up
		// but for simplicity, require sequential progression
		f.log.Warn("Gap in safe head progression",
			"received_number", received.Number,
			"current_safe_number", currentSafe.Number,
			"gap", received.Number-currentSafe.Number)
		return false
	}
}

// payloadToBlockRef converts an execution payload to an L2 block reference
func (f *FollowerModeDeriver) payloadToBlockRef(payload *eth.ExecutionPayload) (eth.L2BlockRef, error) {
	// For follower mode, we construct a minimal L2BlockRef
	// In a full implementation, you might want to extract L1 origin from the payload
	return eth.L2BlockRef{
		Hash:       payload.BlockHash,
		Number:     uint64(payload.BlockNumber),
		ParentHash: payload.ParentHash,
		Time:       uint64(payload.Timestamp),
		// L1Origin and SequenceNumber would need to be extracted from deposit tx
		// For now, using zero values for simplicity
		L1Origin:       eth.BlockID{},
		SequenceNumber: 0,
	}, nil
}

// NoOpDerivationPipeline is a no-op implementation of DerivationPipeline for follower mode
type NoOpDerivationPipeline struct{}

var _ DerivationPipeline = (*NoOpDerivationPipeline)(nil)

func (n *NoOpDerivationPipeline) Reset() {
	// No-op: follower mode doesn't use derivation pipeline
	log.Info("FOLLOWER MODE: NoOpDerivationPipeline.Reset() called")
}

func (n *NoOpDerivationPipeline) Step(ctx context.Context, pendingSafeHead eth.L2BlockRef) (*derive.AttributesWithParent, error) {
	// No-op: follower mode doesn't derive from L1
	log.Info("FOLLOWER MODE: NoOpDerivationPipeline.Step() called", "pendingSafeHead", pendingSafeHead.Number)
	return nil, nil
}

func (n *NoOpDerivationPipeline) Origin() eth.L1BlockRef {
	// No-op: follower mode doesn't track L1 origin
	return eth.L1BlockRef{}
}

func (n *NoOpDerivationPipeline) DerivationReady() bool {
	// Always ready (no-op)
	return true
}

func (n *NoOpDerivationPipeline) ConfirmEngineReset() {
	// No-op: follower mode doesn't use derivation pipeline
}
