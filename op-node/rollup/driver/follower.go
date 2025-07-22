package driver

import (
	"context"
	"fmt"
	"time"

	"github.com/ethereum/go-ethereum/log"

	"github.com/ethereum-optimism/optimism/op-node/p2p"
	"github.com/ethereum-optimism/optimism/op-node/rollup"
	"github.com/ethereum-optimism/optimism/op-node/rollup/derive"
	"github.com/ethereum-optimism/optimism/op-node/rollup/engine"
	"github.com/ethereum-optimism/optimism/op-service/eth"
	"github.com/ethereum-optimism/optimism/op-service/event"
)

// FollowerModeDeriver processes received safe heads from P2P gossip
// and applies them to the engine controller in follower mode with reorg support
type FollowerModeDeriver struct {
	log     log.Logger
	cfg     *rollup.Config
	engine  *engine.EngineController
	emitter event.Emitter
	metrics FollowerModeMetrics

	// Reorg handling state
	lastValidSafeHead eth.L2BlockRef // Last known good safe head for rollback
	reorgDepth        uint64         // Maximum reorg depth to handle
	gossipTimeout     time.Duration  // Timeout for gossip reception
	lastGossipTime    time.Time      // Last time we received valid gossip
	fallbackMode      bool           // Whether to fallback to normal derivation
}

// FollowerModeMetrics defines metrics for follower mode monitoring
type FollowerModeMetrics interface {
	// Safe head gossip metrics
	RecordSafeHeadGossipReceived()
	RecordSafeHeadApplied(blockNumber uint64)
	RecordSafeHeadIgnored(reason string)

	// Reorg metrics
	RecordSafeHeadReorg(reorgDepth uint64)
	RecordReorgFailure()

	// Gap and timeout metrics
	RecordGapDetected(gapSize uint64)
	RecordGossipTimeout()
	RecordFallbackActivated(reason string)

	// Performance metrics
	RecordSafeHeadProcessingTime(duration time.Duration)
}

var _ event.Deriver = (*FollowerModeDeriver)(nil)

func NewFollowerModeDeriver(log log.Logger, cfg *rollup.Config, engine *engine.EngineController, metrics FollowerModeMetrics) *FollowerModeDeriver {
	return &FollowerModeDeriver{
		log:            log,
		cfg:            cfg,
		engine:         engine,
		metrics:        metrics,
		reorgDepth:     10,               // Handle reorgs up to 10 blocks deep
		gossipTimeout:  60 * time.Second, // Fallback after 60s without gossip
		lastGossipTime: time.Now(),
	}
}

func (f *FollowerModeDeriver) AttachEmitter(em event.Emitter) {
	f.emitter = em
}

func (f *FollowerModeDeriver) OnEvent(ctx context.Context, ev event.Event) bool {
	switch x := ev.(type) {
	case p2p.ReceivedSafeHeadEvent:
		return f.processSafeHead(ctx, x)
	case FollowerModeTimeoutEvent:
		return f.handleTimeout(ctx)
	default:
		return false
	}
}

// FollowerModeTimeoutEvent is emitted when follower mode times out without gossip
type FollowerModeTimeoutEvent struct{}

func (ev FollowerModeTimeoutEvent) String() string {
	return "follower-mode-timeout"
}

// processSafeHead validates and applies a received safe head to the engine with reorg support
func (f *FollowerModeDeriver) processSafeHead(ctx context.Context, ev p2p.ReceivedSafeHeadEvent) bool {
	startTime := time.Now()
	defer func() {
		f.metrics.RecordSafeHeadProcessingTime(time.Since(startTime))
	}()

	payload := ev.Envelope.ExecutionPayload
	f.lastGossipTime = time.Now()
	f.metrics.RecordSafeHeadGossipReceived()

	f.log.Info("Follower mode: received safe head from P2P",
		"hash", payload.BlockHash,
		"number", payload.BlockNumber,
		"peer", ev.From)

	// Convert payload to L2 block ref for validation
	ref, err := f.payloadToBlockRef(payload)
	if err != nil {
		f.log.Warn("Failed to convert payload to block ref", "hash", payload.BlockHash, "err", err)
		f.metrics.RecordSafeHeadIgnored("conversion_failed")
		return true
	}

	// Validate and handle the safe head with reorg support
	action, err := f.validateSafeHeadWithReorg(ref)
	if err != nil {
		f.log.Warn("Safe head validation failed", "ref", ref.ID(), "err", err)
		f.metrics.RecordSafeHeadIgnored("validation_failed")
		return true
	}

	switch action {
	case SafeHeadActionApply:
		return f.applySafeHead(ctx, ref)
	case SafeHeadActionReorg:
		return f.handleSafeHeadReorg(ctx, ref)
	case SafeHeadActionIgnore:
		f.log.Debug("Ignoring safe head", "ref", ref.ID())
		f.metrics.RecordSafeHeadIgnored("duplicate_or_old")
		return true
	case SafeHeadActionRequest:
		return f.requestMissingSafeHeads(ctx, ref)
	default:
		f.log.Warn("Unknown safe head action", "action", action)
		f.metrics.RecordSafeHeadIgnored("unknown_action")
		return true
	}
}

// SafeHeadAction defines what to do with a received safe head
type SafeHeadAction int

const (
	SafeHeadActionApply   SafeHeadAction = iota // Apply the safe head normally
	SafeHeadActionReorg                         // Handle a reorg situation
	SafeHeadActionIgnore                        // Ignore (duplicate/old)
	SafeHeadActionRequest                       // Request missing blocks
)

// validateSafeHeadWithReorg determines the appropriate action for a received safe head
func (f *FollowerModeDeriver) validateSafeHeadWithReorg(received eth.L2BlockRef) (SafeHeadAction, error) {
	currentSafe := f.engine.SafeL2Head()

	// If we have no current safe head, accept any valid block as the first safe head
	if currentSafe == (eth.L2BlockRef{}) {
		f.log.Info("No current safe head, accepting first safe head", "hash", received.Hash)
		f.lastValidSafeHead = received
		return SafeHeadActionApply, nil
	}

	// Case 1: Next sequential block - normal progression
	if received.Number == currentSafe.Number+1 {
		if received.ParentHash != currentSafe.Hash {
			f.log.Warn("Safe head parent hash mismatch - potential reorg",
				"received_parent", received.ParentHash,
				"current_safe", currentSafe.Hash,
				"received", received.ID(),
				"current", currentSafe.ID())
			return SafeHeadActionReorg, nil
		}
		f.lastValidSafeHead = received
		return SafeHeadActionApply, nil
	}

	// Case 2: Same block number - reorg or duplicate
	if received.Number == currentSafe.Number {
		if received.Hash == currentSafe.Hash {
			f.log.Debug("Received duplicate safe head, ignoring", "hash", received.Hash)
			return SafeHeadActionIgnore, nil
		} else {
			f.log.Warn("Safe head reorg detected at same height",
				"received", received.ID(),
				"current", currentSafe.ID())
			return SafeHeadActionReorg, nil
		}
	}

	// Case 3: Old block - could be late reorg notification
	if received.Number < currentSafe.Number {
		reorgDepth := currentSafe.Number - received.Number
		if reorgDepth <= f.reorgDepth {
			f.log.Warn("Potential deep reorg detected",
				"received", received.ID(),
				"current", currentSafe.ID(),
				"reorg_depth", reorgDepth)
			return SafeHeadActionReorg, nil
		} else {
			f.log.Debug("Received old safe head beyond reorg depth, ignoring",
				"received_number", received.Number,
				"current_safe_number", currentSafe.Number,
				"reorg_depth", reorgDepth)
			return SafeHeadActionIgnore, nil
		}
	}

	// Case 4: Future block - gap in progression
	gap := received.Number - currentSafe.Number
	if gap <= 5 { // Allow small gaps for catch-up
		f.log.Info("Gap in safe head progression, requesting missing blocks",
			"received_number", received.Number,
			"current_safe_number", currentSafe.Number,
			"gap", gap)
		return SafeHeadActionRequest, nil
	} else {
		f.log.Warn("Large gap in safe head progression",
			"received_number", received.Number,
			"current_safe_number", currentSafe.Number,
			"gap", gap)
		return SafeHeadActionIgnore, fmt.Errorf("gap too large: %d blocks", gap)
	}
}

// applySafeHead applies a safe head to the engine normally
func (f *FollowerModeDeriver) applySafeHead(ctx context.Context, ref eth.L2BlockRef) bool {
	f.log.Info("Follower mode: applying safe head to engine",
		"hash", ref.Hash,
		"number", ref.Number)

	// Backup current safe head before changing it
	currentSafe := f.engine.SafeL2Head()
	if currentSafe != (eth.L2BlockRef{}) {
		f.engine.SetBackupSafeL2Head(currentSafe, false)
		f.log.Debug("Backed up current safe head before applying new one",
			"backup_safe", currentSafe.ID(),
			"new_safe", ref.ID())
	}

	f.engine.SetSafeHead(ref)
	f.emitSafeHeadEvents(ctx, ref)
	f.metrics.RecordSafeHeadApplied(ref.Number)
	return true
}

// handleSafeHeadReorg handles reorg situations in follower mode
func (f *FollowerModeDeriver) handleSafeHeadReorg(ctx context.Context, newRef eth.L2BlockRef) bool {
	currentSafe := f.engine.SafeL2Head()

	f.log.Warn("Follower mode: handling safe head reorg",
		"old_safe", currentSafe.ID(),
		"new_safe", newRef.ID())

	// Step 1: Backup current safe head for potential rollback
	f.engine.SetBackupSafeL2Head(currentSafe, true)
	f.log.Debug("Backed up current safe head for reorg",
		"backup_safe", currentSafe.ID())

	// Step 2: Attempt to apply the new safe head via execution engine
	f.log.Info("Attempting to apply new safe head via execution engine",
		"new_safe", newRef.ID())

	f.engine.SetSafeHead(newRef)

	// Step 3: Try to update the execution engine with forkchoice
	// This will validate that the execution engine accepts the reorg
	// If it fails, the backup reorg mechanism will restore the previous safe head
	f.emitter.Emit(ctx, engine.TryBackupSafeReorgEvent{})

	// Step 4: Update our tracking state
	f.lastValidSafeHead = newRef

	// Step 5: Emit reorg event for monitoring and record metrics
	reorgDepth := uint64(0)
	if currentSafe.Number > newRef.Number {
		reorgDepth = currentSafe.Number - newRef.Number
	} else {
		reorgDepth = newRef.Number - currentSafe.Number
	}

	f.emitter.Emit(ctx, FollowerModeReorgEvent{
		OldSafe:    currentSafe,
		NewSafe:    newRef,
		RollbackTo: newRef, // We're directly applying the new ref
		ReorgDepth: reorgDepth,
	})
	f.metrics.RecordSafeHeadReorg(reorgDepth)

	return true
}

// FollowerModeReorgEvent is emitted when a reorg is handled in follower mode
type FollowerModeReorgEvent struct {
	OldSafe    eth.L2BlockRef
	NewSafe    eth.L2BlockRef
	RollbackTo eth.L2BlockRef
	ReorgDepth uint64
}

func (ev FollowerModeReorgEvent) String() string {
	return "follower-mode-reorg"
}

// findCommonAncestor finds the common ancestor between two block refs
func (f *FollowerModeDeriver) findCommonAncestor(ctx context.Context, oldRef, newRef eth.L2BlockRef) (eth.L2BlockRef, error) {
	// For simplicity, rollback to the last known good safe head
	// In production, you might want to query the engine for the actual common ancestor
	if f.lastValidSafeHead != (eth.L2BlockRef{}) && f.lastValidSafeHead.Number < oldRef.Number {
		f.log.Info("Using last valid safe head as rollback point",
			"last_valid", f.lastValidSafeHead.ID())
		return f.lastValidSafeHead, nil
	}

	// If no last valid safe head, rollback to finalized head as safe fallback
	finalized := f.engine.SafeL2Head() // Changed from f.engine.Finalized() to f.engine.SafeL2Head()
	f.log.Info("Using finalized head as rollback point",
		"finalized", finalized.ID())
	return finalized, nil
}

// handleReorgFailure handles situations where reorg recovery fails
func (f *FollowerModeDeriver) handleReorgFailure(ctx context.Context, err error) bool {
	f.log.Error("Follower mode: reorg handling failed, considering fallback", "err", err)

	// Record failure metrics and emit event for monitoring
	f.metrics.RecordReorgFailure()
	f.emitter.Emit(ctx, FollowerModeReorgFailureEvent{
		Err: err,
	})

	// Consider enabling fallback mode
	if !f.fallbackMode {
		f.log.Warn("Enabling fallback mode due to reorg failure")
		f.fallbackMode = true
		f.metrics.RecordFallbackActivated("reorg_failure")
		f.emitter.Emit(ctx, FollowerModeFallbackEvent{
			Reason: fmt.Sprintf("reorg failure: %v", err),
		})
	}

	return true
}

// FollowerModeReorgFailureEvent is emitted when reorg handling fails
type FollowerModeReorgFailureEvent struct {
	Err error
}

func (ev FollowerModeReorgFailureEvent) String() string {
	return "follower-mode-reorg-failure"
}

// FollowerModeFallbackEvent is emitted when follower mode enables fallback
type FollowerModeFallbackEvent struct {
	Reason string
}

func (ev FollowerModeFallbackEvent) String() string {
	return "follower-mode-fallback"
}

// requestMissingSafeHeads handles gaps in safe head progression
func (f *FollowerModeDeriver) requestMissingSafeHeads(ctx context.Context, targetRef eth.L2BlockRef) bool {
	currentSafe := f.engine.SafeL2Head()
	gap := targetRef.Number - currentSafe.Number

	f.log.Info("Follower mode: requesting missing safe heads",
		"current", currentSafe.ID(),
		"target", targetRef.ID(),
		"gap", gap)

	// Record gap metrics and emit event for monitoring
	f.metrics.RecordGapDetected(gap)
	f.emitter.Emit(ctx, FollowerModeGapDetectedEvent{
		CurrentSafe: currentSafe,
		TargetSafe:  targetRef,
		Gap:         gap,
	})

	return true
}

// FollowerModeGapDetectedEvent is emitted when gaps are detected in safe head progression
type FollowerModeGapDetectedEvent struct {
	CurrentSafe eth.L2BlockRef
	TargetSafe  eth.L2BlockRef
	Gap         uint64
}

func (ev FollowerModeGapDetectedEvent) String() string {
	return "follower-mode-gap-detected"
}

// handleTimeout handles gossip timeout in follower mode
func (f *FollowerModeDeriver) handleTimeout(ctx context.Context) bool {
	timeSinceLastGossip := time.Since(f.lastGossipTime)

	if timeSinceLastGossip > f.gossipTimeout {
		f.log.Warn("Follower mode: gossip timeout exceeded",
			"timeout", f.gossipTimeout,
			"since_last_gossip", timeSinceLastGossip)

		// Record timeout metrics
		f.metrics.RecordGossipTimeout()

		if !f.fallbackMode {
			f.log.Warn("Enabling fallback mode due to gossip timeout")
			f.fallbackMode = true
			f.metrics.RecordFallbackActivated("gossip_timeout")
			f.emitter.Emit(ctx, FollowerModeFallbackEvent{
				Reason: fmt.Sprintf("gossip timeout: %v", timeSinceLastGossip),
			})
		}
	}

	return true
}

// emitSafeHeadEvents emits standard safe head events after changes
func (f *FollowerModeDeriver) emitSafeHeadEvents(ctx context.Context, ref eth.L2BlockRef) {
	// Emit events to notify other components
	f.emitter.Emit(ctx, engine.SafeDerivedEvent{
		Safe:   ref,
		Source: eth.L1BlockRef{}, // No L1 source in follower mode
	})

	f.emitter.Emit(ctx, engine.CrossSafeUpdateEvent{
		CrossSafe: ref,
		LocalSafe: ref, // In follower mode, local and cross safe are the same
	})
}

// NoOpFollowerModeMetrics provides a no-op implementation of FollowerModeMetrics
type NoOpFollowerModeMetrics struct{}

var _ FollowerModeMetrics = (*NoOpFollowerModeMetrics)(nil)

func (m *NoOpFollowerModeMetrics) RecordSafeHeadGossipReceived()                       {}
func (m *NoOpFollowerModeMetrics) RecordSafeHeadApplied(blockNumber uint64)            {}
func (m *NoOpFollowerModeMetrics) RecordSafeHeadIgnored(reason string)                 {}
func (m *NoOpFollowerModeMetrics) RecordSafeHeadReorg(reorgDepth uint64)               {}
func (m *NoOpFollowerModeMetrics) RecordReorgFailure()                                 {}
func (m *NoOpFollowerModeMetrics) RecordGapDetected(gapSize uint64)                    {}
func (m *NoOpFollowerModeMetrics) RecordGossipTimeout()                                {}
func (m *NoOpFollowerModeMetrics) RecordFallbackActivated(reason string)               {}
func (m *NoOpFollowerModeMetrics) RecordSafeHeadProcessingTime(duration time.Duration) {}

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
