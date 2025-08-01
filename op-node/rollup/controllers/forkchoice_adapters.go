package controllers

import (
	"context"

	"github.com/ethereum-optimism/optimism/op-node/rollup/clsync"
	"github.com/ethereum-optimism/optimism/op-node/rollup/finality"
	"github.com/ethereum-optimism/optimism/op-node/rollup/sequencing"
	"github.com/ethereum-optimism/optimism/op-node/rollup/status"
)

// SequencerForkchoiceAdapter adapts the Sequencer to work with ForkchoiceController
type SequencerForkchoiceAdapter struct {
	sequencer *sequencing.Sequencer
}

func NewSequencerForkchoiceAdapter(sequencer *sequencing.Sequencer) *SequencerForkchoiceAdapter {
	return &SequencerForkchoiceAdapter{sequencer: sequencer}
}

func (s *SequencerForkchoiceAdapter) HandleForkchoiceUpdate(ctx context.Context, update ForkchoiceUpdate) error {
	// Convert our update back to the event format and call the existing handler
	event := update.ConvertToEvent()
	s.sequencer.OnEvent(ctx, event)
	return nil
}

// CLSyncForkchoiceAdapter adapts the CLSync component to work with ForkchoiceController
type CLSyncForkchoiceAdapter struct {
	clsync *clsync.CLSync
}

func NewCLSyncForkchoiceAdapter(clsync *clsync.CLSync) *CLSyncForkchoiceAdapter {
	return &CLSyncForkchoiceAdapter{clsync: clsync}
}

func (c *CLSyncForkchoiceAdapter) HandleForkchoiceUpdate(ctx context.Context, update ForkchoiceUpdate) error {
	// Convert our update back to the event format and call the existing handler
	event := update.ConvertToEvent()
	c.clsync.OnEvent(ctx, event)
	return nil
}

// StatusTrackerForkchoiceAdapter adapts the StatusTracker to work with ForkchoiceController
type StatusTrackerForkchoiceAdapter struct {
	tracker *status.StatusTracker
}

func NewStatusTrackerForkchoiceAdapter(tracker *status.StatusTracker) *StatusTrackerForkchoiceAdapter {
	return &StatusTrackerForkchoiceAdapter{tracker: tracker}
}

func (s *StatusTrackerForkchoiceAdapter) HandleForkchoiceUpdate(ctx context.Context, update ForkchoiceUpdate) error {
	// Convert our update back to the event format and call the existing handler
	event := update.ConvertToEvent()
	s.tracker.OnEvent(ctx, event)
	return nil
}

// FinalizerForkchoiceAdapter adapts the Finalizer to work with ForkchoiceController
type FinalizerForkchoiceAdapter struct {
	finalizer *finality.Finalizer
}

func NewFinalizerForkchoiceAdapter(finalizer *finality.Finalizer) *FinalizerForkchoiceAdapter {
	return &FinalizerForkchoiceAdapter{finalizer: finalizer}
}

func (f *FinalizerForkchoiceAdapter) HandleForkchoiceUpdate(ctx context.Context, update ForkchoiceUpdate) error {
	// Convert our update back to the event format and call the existing handler
	event := update.ConvertToEvent()
	f.finalizer.OnEvent(ctx, event)
	return nil
}

// L1OriginSelectorForkchoiceAdapter adapts the L1OriginSelector to work with ForkchoiceController
type L1OriginSelectorForkchoiceAdapter struct {
	selector *sequencing.L1OriginSelector
}

func NewL1OriginSelectorForkchoiceAdapter(selector *sequencing.L1OriginSelector) *L1OriginSelectorForkchoiceAdapter {
	return &L1OriginSelectorForkchoiceAdapter{selector: selector}
}

func (o *L1OriginSelectorForkchoiceAdapter) HandleForkchoiceUpdate(ctx context.Context, update ForkchoiceUpdate) error {
	// Convert our update back to the event format and call the existing handler
	event := update.ConvertToEvent()
	o.selector.OnEvent(ctx, event)
	return nil
}