package engine

import (
	"context"
	"errors"
	"fmt"

	"github.com/ethereum/go-ethereum/log"

	"github.com/ethereum-optimism/optimism/op-node/rollup"
	"github.com/ethereum-optimism/optimism/op-node/rollup/derive"
	"github.com/ethereum-optimism/optimism/op-service/eth"
	"github.com/ethereum-optimism/optimism/op-service/event"
)

// EngineControllerInterface defines the interface we need from EngineController
// This allows for easier testing and mocking
type EngineControllerInterface interface {
	TryUpdateEngine(ctx context.Context) error
	InsertUnsafePayload(ctx context.Context, envelope *eth.ExecutionPayloadEnvelope, ref eth.L2BlockRef) error
	UnsafeL2Head() eth.L2BlockRef

	// Config access for payload processing
	Config() *rollup.Config

	// Additional methods for state promotions
	SetUnsafeHead(ref eth.L2BlockRef)
	SetPendingSafeL2Head(ref eth.L2BlockRef)
	PendingSafeL2Head() eth.L2BlockRef
	SetLocalSafeHead(ref eth.L2BlockRef)
	LocalSafeL2Head() eth.L2BlockRef
	SetSafeHead(ref eth.L2BlockRef)
	SafeL2Head() eth.L2BlockRef
	SetFinalizedHead(ref eth.L2BlockRef)
	Finalized() eth.L2BlockRef

	// Cross-chain head methods for interop
	SetCrossUnsafeHead(ref eth.L2BlockRef)
	CrossUnsafeL2Head() eth.L2BlockRef
	SetBackupUnsafeL2Head(ref eth.L2BlockRef, pending bool)
}

// EngineStateManager replaces the massive EngDeriver.OnEvent() switch statement
// with clean, debuggable, imperative method calls.
//
// 🎯 ARCHITECTURAL BENEFITS:
// - Call stack debugging instead of event trace correlation
// - Synchronous execution instead of async event timing
// - Clear method boundaries instead of 500+ line switch statement
// - Fail-fast error handling instead of silent failures
type EngineStateManager struct {
	controller EngineControllerInterface
	log        log.Logger

	// 🛡️ DEFENSIVE: Fail-fast on unhandled cases
	strictMode bool

	// External event handlers for backward compatibility
	// These will be removed once all consumers are migrated
	externalHandlers map[string]EngineEventHandler
}

// EngineEventHandler interface for external event consumers
// This provides backward compatibility during migration
type EngineEventHandler interface {
	HandleEngineEvent(ctx context.Context, eventType string, data interface{}) error
}

// NewEngineStateManager creates a new defensive engine state manager
func NewEngineStateManager(controller EngineControllerInterface, log log.Logger) *EngineStateManager {
	return &EngineStateManager{
		controller:       controller,
		log:              log,
		strictMode:       true, // 🚨 DEFENSIVE: Default to strict mode
		externalHandlers: make(map[string]EngineEventHandler),
	}
}

// SetStrictMode enables/disables fail-fast behavior
// When true: panics on unhandled events
// When false: logs errors and continues
func (e *EngineStateManager) SetStrictMode(strict bool) {
	e.strictMode = strict
	e.log.Info("EngineStateManager strict mode", "enabled", strict)
}

// RegisterExternalHandler registers a handler for external consumers
// This provides backward compatibility during migration
func (e *EngineStateManager) RegisterExternalHandler(eventType string, handler EngineEventHandler) {
	e.externalHandlers[eventType] = handler
	e.log.Info("Registered external engine event handler", "event_type", eventType)
}

// 🔥 HIGH-FREQUENCY ENGINE OPERATIONS (Internal Only - Safe to Replace)

// TryUpdateEngine replaces TryUpdateEngineEvent (795x frequency!)
// This is the #1 most frequent event - massive performance impact
func (e *EngineStateManager) TryUpdateEngine(ctx context.Context) error {
	e.log.Debug("Processing TryUpdateEngine imperatively (was event-driven)")

	// Call the actual engine controller method
	if err := e.controller.TryUpdateEngine(ctx); err != nil {
		// Handle the same error types as the original event handler
		if errors.Is(err, ErrNoFCUNeeded) {
			// This is expected, not an error
			return nil
		}

		if errors.Is(err, derive.ErrReset) {
			// Emit reset event for external consumers
			e.notifyExternalHandlers(ctx, "ResetEvent", err)
			return err
		}

		if errors.Is(err, derive.ErrTemporary) {
			// Emit temporary error event for external consumers
			e.notifyExternalHandlers(ctx, "EngineTemporaryErrorEvent", err)
			return err
		}

		// Critical error - this was unexpected
		criticalErr := fmt.Errorf("unexpected TryUpdateEngine error type: %w", err)
		e.notifyExternalHandlers(ctx, "CriticalErrorEvent", criticalErr)
		return criticalErr
	}

	return nil
}

// ProcessUnsafePayload replaces ProcessUnsafePayloadEvent
// Handles unsafe payload insertion with defensive validation
func (e *EngineStateManager) ProcessUnsafePayload(ctx context.Context, envelope *eth.ExecutionPayloadEnvelope) error {
	e.log.Debug("Processing unsafe payload imperatively", "payload", envelope.ExecutionPayload.BlockHash)

	// Convert payload to block ref (same logic as original event handler)
	ref, err := derive.PayloadToBlockRef(e.controller.Config(), envelope.ExecutionPayload)
	if err != nil {
		e.log.Error("failed to decode L2 block ref from payload", "err", err)
		return fmt.Errorf("failed to decode block ref: %w", err)
	}

	// Avoid re-processing the same unsafe payload (same logic as original)
	if ref.BlockRef().ID() == e.controller.UnsafeL2Head().BlockRef().ID() {
		e.log.Debug("Skipping already processed unsafe payload", "ref", ref)
		return nil
	}

	// Insert the unsafe payload
	if err := e.controller.InsertUnsafePayload(ctx, envelope, ref); err != nil {
		e.log.Info("failed to insert payload", "ref", ref,
			"txs", len(envelope.ExecutionPayload.Transactions), "err", err)

		// Same error handling as original event handler
		if errors.Is(err, derive.ErrReset) {
			e.notifyExternalHandlers(ctx, "ResetEvent", err)
			return err
		}

		if errors.Is(err, derive.ErrTemporary) {
			e.notifyExternalHandlers(ctx, "EngineTemporaryErrorEvent", err)
			return err
		}

		// Critical error
		criticalErr := fmt.Errorf("unexpected InsertUnsafePayload error type: %w", err)
		e.notifyExternalHandlers(ctx, "CriticalErrorEvent", criticalErr)
		return criticalErr
	}

	e.log.Debug("Successfully processed unsafe payload", "ref", ref)
	return nil
}

// 🎯 ENGINE STATE PROMOTION METHODS (Core State Machine)

// PromoteToUnsafe replaces PromoteUnsafeEvent
// Promotes a block to unsafe status
func (e *EngineStateManager) PromoteToUnsafe(ctx context.Context, ref eth.L2BlockRef) error {
	e.log.Debug("Promoting block to unsafe", "ref", ref)

	// Backup unsafeHead when new block is not built on original unsafe head (same logic as original)
	if e.controller.UnsafeL2Head().Number >= ref.Number {
		e.controller.SetBackupUnsafeL2Head(e.controller.UnsafeL2Head(), false)
	}

	// Set the new unsafe head
	e.controller.SetUnsafeHead(ref)

	// Notify external consumers (backward compatibility) - UnsafeUpdateEvent has external consumers
	unsafeUpdate := struct {
		Ref eth.L2BlockRef
	}{Ref: ref}
	e.notifyExternalHandlers(ctx, "UnsafeUpdateEvent", unsafeUpdate)

	return nil
}

// PromoteToPendingSafe replaces PromotePendingSafeEvent
// Promotes a block to pending-safe status
func (e *EngineStateManager) PromoteToPendingSafe(ctx context.Context, ref eth.L2BlockRef, source eth.L1BlockRef, concluding bool) error {
	e.log.Debug("Promoting block to pending-safe", "ref", ref, "source", source, "concluding", concluding)

	// Only promote if not already stale (same logic as original)
	if ref.Number > e.controller.PendingSafeL2Head().Number {
		e.log.Debug("Updating pending safe", "pending_safe", ref, "local_safe", e.controller.LocalSafeL2Head(), "unsafe", e.controller.UnsafeL2Head(), "concluding", concluding)
		e.controller.SetPendingSafeL2Head(ref)

		// Notify external consumers (backward compatibility) - PendingSafeUpdateEvent has external consumers
		updateData := struct {
			PendingSafe eth.L2BlockRef
			Unsafe      eth.L2BlockRef
		}{
			PendingSafe: e.controller.PendingSafeL2Head(),
			Unsafe:      e.controller.UnsafeL2Head(),
		}
		e.notifyExternalHandlers(ctx, "PendingSafeUpdateEvent", updateData)
	}

	// If concluding and eligible, promote to local safe (same logic as original)
	if concluding && ref.Number > e.controller.LocalSafeL2Head().Number {
		return e.PromoteToLocalSafe(ctx, ref, source)
	}

	return nil
}

// PromoteToLocalSafe replaces PromoteLocalSafeEvent
// Promotes a block to local-safe status
func (e *EngineStateManager) PromoteToLocalSafe(ctx context.Context, ref eth.L2BlockRef, source eth.L1BlockRef) error {
	e.log.Debug("Updating local safe", "local_safe", ref, "safe", e.controller.SafeL2Head(), "unsafe", e.controller.UnsafeL2Head())

	// Set the local safe head
	e.controller.SetLocalSafeHead(ref)

	// Notify external consumers (backward compatibility) - LocalSafeUpdateEvent has external consumers
	updateData := struct {
		Ref    eth.L2BlockRef
		Source eth.L1BlockRef
	}{
		Ref:    ref,
		Source: source,
	}
	e.notifyExternalHandlers(ctx, "LocalSafeUpdateEvent", updateData)

	return nil
}

// PromoteToSafe replaces PromoteSafeEvent
// Promotes a block to safe status
func (e *EngineStateManager) PromoteToSafe(ctx context.Context, ref eth.L2BlockRef, source eth.L1BlockRef) error {
	e.log.Debug("Updating safe", "safe", ref, "unsafe", e.controller.UnsafeL2Head())

	// Set the safe head
	e.controller.SetSafeHead(ref)

	// Notify external consumers (backward compatibility) - SafeDerivedEvent has external consumers
	safeDerivedData := struct {
		Safe   eth.L2BlockRef
		Source eth.L1BlockRef
	}{
		Safe:   ref,
		Source: source,
	}
	e.notifyExternalHandlers(ctx, "SafeDerivedEvent", safeDerivedData)

	// CrossSafeUpdateEvent has external consumers
	crossSafeData := struct {
		CrossSafe eth.L2BlockRef
		LocalSafe eth.L2BlockRef
	}{
		CrossSafe: e.controller.SafeL2Head(),
		LocalSafe: e.controller.LocalSafeL2Head(),
	}
	e.notifyExternalHandlers(ctx, "CrossSafeUpdateEvent", crossSafeData)

	// Update cross unsafe head if stale (same logic as original)
	if ref.Number > e.controller.CrossUnsafeL2Head().Number {
		e.log.Debug("Cross Unsafe Head is stale, updating to match cross safe", "cross_unsafe", e.controller.CrossUnsafeL2Head(), "cross_safe", ref)
		e.controller.SetCrossUnsafeHead(ref)

		// CrossUnsafeUpdateEvent has external consumers
		crossUnsafeData := struct {
			CrossUnsafe eth.L2BlockRef
			LocalUnsafe eth.L2BlockRef
		}{
			CrossUnsafe: ref,
			LocalUnsafe: e.controller.UnsafeL2Head(),
		}
		e.notifyExternalHandlers(ctx, "CrossUnsafeUpdateEvent", crossUnsafeData)
	}

	// Try to apply the forkchoice changes (same as original)
	return e.TryUpdateEngine(ctx)
}

// PromoteToFinalized replaces PromoteFinalizedEvent
// Promotes a block to finalized status
func (e *EngineStateManager) PromoteToFinalized(ctx context.Context, ref eth.L2BlockRef) error {
	e.log.Debug("Promoting block to finalized", "ref", ref)

	// Validation checks (same as original)
	if ref.Number < e.controller.Finalized().Number {
		return fmt.Errorf("cannot rewind finality, ref: %v, finalized: %v", ref, e.controller.Finalized())
	}
	if ref.Number > e.controller.SafeL2Head().Number {
		return fmt.Errorf("block must be safe before it can be finalized, ref: %v, safe: %v", ref, e.controller.SafeL2Head())
	}

	// Set the finalized head
	e.controller.SetFinalizedHead(ref)

	// Notify external consumers (backward compatibility)
	finalizedData := struct {
		Ref eth.L2BlockRef
	}{Ref: ref}
	e.notifyExternalHandlers(ctx, "FinalizedUpdateEvent", finalizedData)

	// Try to apply the forkchoice changes (same as original)
	return e.TryUpdateEngine(ctx)
}

// RequestForkchoiceUpdate handles ForkchoiceRequestEvent by emitting ForkchoiceUpdateEvent
// with current engine state. This replaces the event-driven ForkchoiceRequestEvent handling.
func (e *EngineStateManager) RequestForkchoiceUpdate(ctx context.Context, emitter event.Emitter) error {
	e.log.Debug("Requesting forkchoice update imperatively")

	// Get current engine state (same logic as ForkchoiceRequestEvent handler)
	unsafeHead := e.controller.UnsafeL2Head()
	safeHead := e.controller.SafeL2Head()
	finalizedHead := e.controller.Finalized()

	// Emit the ForkchoiceUpdateEvent (same behavior as original event handler)
	forkchoiceUpdate := ForkchoiceUpdateEvent{
		UnsafeL2Head:    unsafeHead,
		SafeL2Head:      safeHead,
		FinalizedL2Head: finalizedHead,
	}

	e.log.Debug("Emitting forkchoice update",
		"unsafe", unsafeHead,
		"safe", safeHead,
		"finalized", finalizedHead)

	// This still emits an event, but the request is now imperative
	emitter.Emit(ctx, forkchoiceUpdate)
	return nil
}

// 🛡️ DEFENSIVE HELPER METHODS

// notifyExternalHandlers provides backward compatibility by calling external event handlers
// This will be removed once all consumers are migrated to imperative calls
func (e *EngineStateManager) notifyExternalHandlers(ctx context.Context, eventType string, data interface{}) {
	if handler, exists := e.externalHandlers[eventType]; exists {
		if err := handler.HandleEngineEvent(ctx, eventType, data); err != nil {
			if e.strictMode {
				panic(fmt.Sprintf("DEFENSIVE: External handler for %s failed: %v", eventType, err))
			}
			e.log.Error("External engine event handler failed", "event_type", eventType, "error", err)
		}
	}
}

// validateConfiguration performs defensive validation of the state manager setup
func (e *EngineStateManager) validateConfiguration() error {
	if e.controller == nil {
		return fmt.Errorf("DEFENSIVE: EngineController is nil")
	}

	if e.log == nil {
		return fmt.Errorf("DEFENSIVE: Logger is nil")
	}

	// TODO: Add more defensive validations as we implement more methods

	return nil
}

// GetStats returns statistics about the state manager for debugging
func (e *EngineStateManager) GetStats() map[string]interface{} {
	return map[string]interface{}{
		"strict_mode":       e.strictMode,
		"external_handlers": len(e.externalHandlers),
		"controller_set":    e.controller != nil,
	}
}
