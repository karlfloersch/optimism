package controllers

import (
	"context"

	"github.com/ethereum/go-ethereum/log"

	"github.com/ethereum-optimism/optimism/op-node/rollup/clsync"
	"github.com/ethereum-optimism/optimism/op-node/rollup/engine"
	"github.com/ethereum-optimism/optimism/op-node/rollup/finality"
	"github.com/ethereum-optimism/optimism/op-node/rollup/sequencing"
	"github.com/ethereum-optimism/optimism/op-node/rollup/status"
	"github.com/ethereum-optimism/optimism/op-service/eth"
)

// This file demonstrates how to integrate ForkchoiceController into EngineController
// This is a DEMO - the actual integration would modify engine_controller.go directly

// EngineControllerWithForkchoiceController demonstrates the modified engine controller
type EngineControllerWithForkchoiceController struct {
	*engine.EngineController // Embed original controller

	// Replace event emission with direct controller calls
	forkchoiceController *ForkchoiceController
	log                  log.Logger
}

// NewEngineControllerWithForkchoiceController creates an engine controller that uses
// imperative forkchoice handling instead of events
func NewEngineControllerWithForkchoiceController(
	original *engine.EngineController,
	forkchoiceController *ForkchoiceController,
	log log.Logger,
) *EngineControllerWithForkchoiceController {
	return &EngineControllerWithForkchoiceController{
		EngineController:     original,
		forkchoiceController: forkchoiceController,
		log:                  log,
	}
}

// ProcessForkchoiceUpdateSuccess handles a successful forkchoice update
// This replaces the event emission pattern in the original TryUpdateEngine method
func (e *EngineControllerWithForkchoiceController) ProcessForkchoiceUpdateSuccess(
	ctx context.Context,
	unsafeHead, safeHead, finalizedHead eth.L2BlockRef,
) error {
	e.log.Debug("Processing successful forkchoice update imperatively",
		"unsafe", unsafeHead,
		"safe", safeHead,
		"finalized", finalizedHead,
	)

	// Instead of emitting ForkchoiceUpdateEvent, call the controller directly
	update := ForkchoiceUpdate{
		UnsafeL2Head:    unsafeHead,
		SafeL2Head:      safeHead,
		FinalizedL2Head: finalizedHead,
	}

	// This single call replaces the event emission and all the async event handling
	return e.forkchoiceController.ProcessForkchoiceUpdate(ctx, update)
}

// DEMO: This shows the pattern we would apply to the original TryUpdateEngine method:
//
// BEFORE (in engine_controller.go line 363-368):
// ```
// if fcRes.PayloadStatus.Status == eth.ExecutionValid {
//     e.emitter.Emit(ctx, ForkchoiceUpdateEvent{
//         UnsafeL2Head:    e.unsafeHead,
//         SafeL2Head:      e.safeHead,
//         FinalizedL2Head: e.finalizedHead,
//     })
// }
// ```
//
// AFTER:
// ```
// if fcRes.PayloadStatus.Status == eth.ExecutionValid {
//     if err := e.ProcessForkchoiceUpdateSuccess(ctx, e.unsafeHead, e.safeHead, e.finalizedHead); err != nil {
//         e.log.Error("Failed to process forkchoice update", "error", err)
//         // Continue execution - this maintains the same error handling as events
//     }
// }
// ```

// Integration Example: This demonstrates how to set up all the handlers in the node initialization
func ExampleForkchoiceControllerSetup(
	sequencer *sequencing.Sequencer,
	clsync *clsync.CLSync,
	statusTracker *status.StatusTracker,
	finalizer *finality.Finalizer,
	originSelector *sequencing.L1OriginSelector,
	log log.Logger,
) *ForkchoiceController {
	// Create all the adapters
	handlers := []ForkchoiceHandler{
		NewSequencerForkchoiceAdapter(sequencer),
		NewCLSyncForkchoiceAdapter(clsync),
		NewStatusTrackerForkchoiceAdapter(statusTracker),
		NewFinalizerForkchoiceAdapter(finalizer),
		NewL1OriginSelectorForkchoiceAdapter(originSelector),
	}

	// Create the controller with all handlers
	forkchoiceController := NewForkchoiceController(log, handlers...)

	log.Info("ForkchoiceController initialized",
		"handlers", len(handlers),
		"replacing_events", "ForkchoiceUpdateEvent",
	)

	return forkchoiceController
}
