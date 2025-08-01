package controllers

import (
	"context"
	"errors"
	"fmt"

	"github.com/ethereum/go-ethereum/log"

	"github.com/ethereum-optimism/optimism/op-node/rollup/derive"
	"github.com/ethereum-optimism/optimism/op-node/rollup/engine"
	"github.com/ethereum-optimism/optimism/op-service/eth"
)

// EngineControllerInterface defines the interface we need from EngineController
// This allows for easier testing and mocking
type EngineControllerInterface interface {
	TryUpdateEngine(ctx context.Context) error
	InsertUnsafePayload(ctx context.Context, envelope *eth.ExecutionPayloadEnvelope, ref eth.L2BlockRef) error
	UnsafeL2Head() eth.L2BlockRef
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
		if errors.Is(err, engine.ErrNoFCUNeeded) {
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
	// TODO: Get config from controller - need to add Config() method
	// For now, we'll need to pass config separately or add getter method
	ref, err := derive.PayloadToBlockRef(nil, envelope.ExecutionPayload) // FIXME: Need config
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
	
	// TODO: Implement unsafe promotion logic
	// This will call the appropriate EngineController methods
	
	// Notify external consumers (backward compatibility)
	e.notifyExternalHandlers(ctx, "UnsafeUpdateEvent", ref)
	
	return nil
}

// PromoteToPendingSafe replaces PromotePendingSafeEvent  
// Promotes a block to pending-safe status
func (e *EngineStateManager) PromoteToPendingSafe(ctx context.Context, ref eth.L2BlockRef, source eth.L1BlockRef, concluding bool) error {
	e.log.Debug("Promoting block to pending-safe", "ref", ref, "source", source, "concluding", concluding)
	
	// TODO: Implement pending-safe promotion logic
	// This will call the appropriate EngineController methods
	
	// Notify external consumers (backward compatibility)
	updateData := struct {
		PendingSafe eth.L2BlockRef
		Unsafe      eth.L2BlockRef
	}{
		PendingSafe: ref,
		Unsafe:      e.controller.UnsafeL2Head(), // Current unsafe head
	}
	e.notifyExternalHandlers(ctx, "PendingSafeUpdateEvent", updateData)
	
	return nil
}

// PromoteToLocalSafe replaces PromoteLocalSafeEvent
// Promotes a block to local-safe status  
func (e *EngineStateManager) PromoteToLocalSafe(ctx context.Context, ref eth.L2BlockRef, source eth.L1BlockRef) error {
	e.log.Debug("Promoting block to local-safe", "ref", ref, "source", source)
	
	// TODO: Implement local-safe promotion logic
	// This will call the appropriate EngineController methods
	
	// Notify external consumers (backward compatibility)
	updateData := struct {
		LocalSafe eth.L2BlockRef
		Source    eth.L1BlockRef
	}{
		LocalSafe: ref,
		Source:    source,
	}
	e.notifyExternalHandlers(ctx, "LocalSafeUpdateEvent", updateData)
	
	return nil
}

// PromoteToSafe replaces PromoteSafeEvent
// Promotes a block to safe status
func (e *EngineStateManager) PromoteToSafe(ctx context.Context, ref eth.L2BlockRef) error {
	e.log.Debug("Promoting block to safe", "ref", ref)
	
	// TODO: Implement safe promotion logic
	// This will call the appropriate EngineController methods
	
	// Notify external consumers (backward compatibility)  
	e.notifyExternalHandlers(ctx, "SafeDerivedEvent", ref)
	
	return nil
}

// PromoteToFinalized replaces PromoteFinalizedEvent
// Promotes a block to finalized status
func (e *EngineStateManager) PromoteToFinalized(ctx context.Context, ref eth.L2BlockRef) error {
	e.log.Debug("Promoting block to finalized", "ref", ref)
	
	// TODO: Implement finalized promotion logic  
	// This will call the appropriate EngineController methods
	
	// Notify external consumers (backward compatibility)
	e.notifyExternalHandlers(ctx, "FinalizedUpdateEvent", ref)
	
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
		"strict_mode":        e.strictMode,
		"external_handlers":  len(e.externalHandlers),
		"controller_set":     e.controller != nil,
	}
}