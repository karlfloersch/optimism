package controllers

import (
	"context"

	"github.com/ethereum/go-ethereum/log"

	"github.com/ethereum-optimism/optimism/op-node/rollup/engine"
	"github.com/ethereum-optimism/optimism/op-service/eth"
)

// ForkchoiceHandler represents any component that needs to handle forkchoice updates
type ForkchoiceHandler interface {
	HandleForkchoiceUpdate(ctx context.Context, update ForkchoiceUpdate) error
}

// ForkchoiceUpdate represents the data from a successful forkchoice update
type ForkchoiceUpdate struct {
	UnsafeL2Head    eth.L2BlockRef
	SafeL2Head      eth.L2BlockRef
	FinalizedL2Head eth.L2BlockRef
}

// ForkchoiceController manages forkchoice updates imperatively instead of through events
type ForkchoiceController struct {
	log log.Logger

	// All components that need to handle forkchoice updates
	handlers []ForkchoiceHandler
}

// NewForkchoiceController creates a new controller with registered handlers
func NewForkchoiceController(log log.Logger, handlers ...ForkchoiceHandler) *ForkchoiceController {
	return &ForkchoiceController{
		log:      log,
		handlers: handlers,
	}
}

// RegisterHandler adds a new forkchoice handler
func (fc *ForkchoiceController) RegisterHandler(handler ForkchoiceHandler) {
	fc.handlers = append(fc.handlers, handler)
}

// ProcessForkchoiceUpdate handles a successful forkchoice update by calling all handlers directly
// This replaces the event emission pattern with direct imperative calls
func (fc *ForkchoiceController) ProcessForkchoiceUpdate(ctx context.Context, update ForkchoiceUpdate) error {
	fc.log.Debug("Processing forkchoice update imperatively",
		"unsafe", update.UnsafeL2Head,
		"safe", update.SafeL2Head,
		"finalized", update.FinalizedL2Head,
		"handlers", len(fc.handlers),
	)

	// Call each handler directly instead of emitting events
	for i, handler := range fc.handlers {
		if err := handler.HandleForkchoiceUpdate(ctx, update); err != nil {
			fc.log.Error("Forkchoice handler failed",
				"handler_index", i,
				"error", err,
				"unsafe", update.UnsafeL2Head,
			)
			// Continue processing other handlers even if one fails
			// This maintains the same behavior as event emission
		}
	}

	fc.log.Debug("Completed forkchoice update processing",
		"handlers_called", len(fc.handlers),
		"unsafe", update.UnsafeL2Head,
	)
	return nil
}

// ConvertFromEvent converts the old ForkchoiceUpdateEvent to our new structure
// This helps during the migration phase
func ConvertFromEvent(event engine.ForkchoiceUpdateEvent) ForkchoiceUpdate {
	return ForkchoiceUpdate{
		UnsafeL2Head:    event.UnsafeL2Head,
		SafeL2Head:      event.SafeL2Head,
		FinalizedL2Head: event.FinalizedL2Head,
	}
}

// ConvertToEvent converts our structure back to the old event format
// This helps during the migration phase when some components still expect events
func (update ForkchoiceUpdate) ConvertToEvent() engine.ForkchoiceUpdateEvent {
	return engine.ForkchoiceUpdateEvent{
		UnsafeL2Head:    update.UnsafeL2Head,
		SafeL2Head:      update.SafeL2Head,
		FinalizedL2Head: update.FinalizedL2Head,
	}
}
