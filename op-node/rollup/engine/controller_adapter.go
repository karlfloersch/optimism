package engine

import (
	"context"

	"github.com/ethereum-optimism/optimism/op-node/rollup"
	"github.com/ethereum-optimism/optimism/op-service/eth"
)

// EngineControllerAdapter adapts the EngineController to implement EngineControllerInterface
// This allows us to use the real EngineController with the EngineStateManager without import cycles
type EngineControllerAdapter struct {
	controller *EngineController
	config     *rollup.Config
}

// NewEngineControllerAdapter creates a new adapter for the engine controller
func NewEngineControllerAdapter(controller *EngineController, config *rollup.Config) *EngineControllerAdapter {
	return &EngineControllerAdapter{
		controller: controller,
		config:     config,
	}
}

// TryUpdateEngine implements EngineControllerInterface
func (a *EngineControllerAdapter) TryUpdateEngine(ctx context.Context) error {
	return a.controller.TryUpdateEngine(ctx)
}

// InsertUnsafePayload implements EngineControllerInterface
func (a *EngineControllerAdapter) InsertUnsafePayload(ctx context.Context, envelope *eth.ExecutionPayloadEnvelope, ref eth.L2BlockRef) error {
	return a.controller.InsertUnsafePayload(ctx, envelope, ref)
}

// UnsafeL2Head implements EngineControllerInterface
func (a *EngineControllerAdapter) UnsafeL2Head() eth.L2BlockRef {
	return a.controller.UnsafeL2Head()
}

// Config implements EngineControllerInterface
func (a *EngineControllerAdapter) Config() *rollup.Config {
	return a.config
}

// SetUnsafeHead implements EngineControllerInterface
func (a *EngineControllerAdapter) SetUnsafeHead(ref eth.L2BlockRef) {
	a.controller.SetUnsafeHead(ref)
}

// SetPendingSafeL2Head implements EngineControllerInterface
func (a *EngineControllerAdapter) SetPendingSafeL2Head(ref eth.L2BlockRef) {
	a.controller.SetPendingSafeL2Head(ref)
}

// PendingSafeL2Head implements EngineControllerInterface
func (a *EngineControllerAdapter) PendingSafeL2Head() eth.L2BlockRef {
	return a.controller.PendingSafeL2Head()
}

// SetLocalSafeHead implements EngineControllerInterface
func (a *EngineControllerAdapter) SetLocalSafeHead(ref eth.L2BlockRef) {
	a.controller.SetLocalSafeHead(ref)
}

// LocalSafeL2Head implements EngineControllerInterface
func (a *EngineControllerAdapter) LocalSafeL2Head() eth.L2BlockRef {
	return a.controller.LocalSafeL2Head()
}

// SetSafeHead implements EngineControllerInterface
func (a *EngineControllerAdapter) SetSafeHead(ref eth.L2BlockRef) {
	a.controller.SetSafeHead(ref)
}

// SafeL2Head implements EngineControllerInterface
func (a *EngineControllerAdapter) SafeL2Head() eth.L2BlockRef {
	return a.controller.SafeL2Head()
}

// SetFinalizedHead implements EngineControllerInterface
func (a *EngineControllerAdapter) SetFinalizedHead(ref eth.L2BlockRef) {
	a.controller.SetFinalizedHead(ref)
}

// Finalized implements EngineControllerInterface
func (a *EngineControllerAdapter) Finalized() eth.L2BlockRef {
	return a.controller.Finalized()
}

// SetCrossUnsafeHead implements EngineControllerInterface
func (a *EngineControllerAdapter) SetCrossUnsafeHead(ref eth.L2BlockRef) {
	a.controller.SetCrossUnsafeHead(ref)
}

// CrossUnsafeL2Head implements EngineControllerInterface
func (a *EngineControllerAdapter) CrossUnsafeL2Head() eth.L2BlockRef {
	return a.controller.CrossUnsafeL2Head()
}

// SetBackupUnsafeL2Head implements EngineControllerInterface
func (a *EngineControllerAdapter) SetBackupUnsafeL2Head(ref eth.L2BlockRef, pending bool) {
	a.controller.SetBackupUnsafeL2Head(ref, pending)
}