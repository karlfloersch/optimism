package controller

import (
	"context"
	"fmt"

	interopengine "github.com/ethereum-optimism/optimism/op-supernode/supernode/activity/interop/engine"
)

type ObservationSource interface {
	ObserveRound(ctx context.Context, acceptedTS *uint64, frontierTS uint64) (interopengine.RoundObservation, error)
}

type FrontierEvidence struct {
	Timestamp uint64
}

type EvidenceResolver interface {
	ResolveFrontier(ctx context.Context, frontier interopengine.FrontierSnapshot) (FrontierEvidence, error)
}

type Verifier interface {
	Verify(ctx context.Context, observation interopengine.RoundObservation, evidence FrontierEvidence) (interopengine.VerificationResult, error)
}

type StateStore interface {
	Load() (interopengine.InteropState, error)
	Commit(state interopengine.InteropState) error
}

type EffectRunner interface {
	Run(ctx context.Context, pending []interopengine.PendingEffect) error
}

type Controller struct {
	activationTimestamp uint64
	engine              *interopengine.Engine
	store               StateStore
	source              ObservationSource
	resolver            EvidenceResolver
	verifier            Verifier
	effectExec          EffectRunner
}

func New(
	activationTimestamp uint64,
	engine *interopengine.Engine,
	store StateStore,
	source ObservationSource,
	resolver EvidenceResolver,
	verifier Verifier,
	effectExec EffectRunner,
) *Controller {
	return &Controller{
		activationTimestamp: activationTimestamp,
		engine:              engine,
		store:               store,
		source:              source,
		resolver:            resolver,
		verifier:            verifier,
		effectExec:          effectExec,
	}
}

func (c *Controller) Step(ctx context.Context) (interopengine.StepResult, error) {
	state, err := c.store.Load()
	if err != nil {
		return interopengine.StepResult{}, fmt.Errorf("load interop state: %w", err)
	}

	if len(state.PendingEffects) > 0 {
		if err := c.effectExec.Run(ctx, state.PendingEffects); err != nil {
			return interopengine.StepResult{}, fmt.Errorf("drain pending effects: %w", err)
		}
		state.PendingEffects = nil
		if err := c.store.Commit(state); err != nil {
			return interopengine.StepResult{}, fmt.Errorf("commit cleared pending effects: %w", err)
		}
	}

	observation, err := c.source.ObserveRound(ctx, acceptedTS(state), c.frontierTS(state))
	if err != nil {
		return interopengine.StepResult{}, fmt.Errorf("observe round: %w", err)
	}

	verification, err := c.verify(ctx, observation)
	if err != nil {
		return interopengine.StepResult{}, err
	}

	step, err := c.engine.Step(state, interopengine.StepInput{
		Observation:  observation,
		Verification: verification,
	})
	if err != nil {
		return interopengine.StepResult{}, err
	}

	newState := interopengine.CopyState(step.NewState)
	newState.PendingEffects = pendingEffects(step.Effects)
	if err := c.store.Commit(newState); err != nil {
		return interopengine.StepResult{}, fmt.Errorf("commit step state: %w", err)
	}

	if len(newState.PendingEffects) == 0 {
		return step, nil
	}
	if err := c.effectExec.Run(ctx, newState.PendingEffects); err != nil {
		return interopengine.StepResult{}, fmt.Errorf("run step effects: %w", err)
	}
	newState.PendingEffects = nil
	if err := c.store.Commit(newState); err != nil {
		return interopengine.StepResult{}, fmt.Errorf("commit cleared step effects: %w", err)
	}
	return step, nil
}

func (c *Controller) verify(ctx context.Context, observation interopengine.RoundObservation) (interopengine.VerificationResult, error) {
	if !observation.Frontier.Present {
		switch observation.Frontier.Reason {
		case interopengine.AvailabilityNotReady:
			return interopengine.VerificationResult{
				Timestamp: observation.FrontierTS,
				Status:    interopengine.VerificationNotReady,
			}, nil
		case interopengine.AvailabilityConflict:
			return interopengine.VerificationResult{
				Timestamp: observation.FrontierTS,
				Status:    interopengine.VerificationConflict,
			}, nil
		default:
			return interopengine.VerificationResult{}, fmt.Errorf("frontier unavailable for unsupported reason %d", observation.Frontier.Reason)
		}
	}

	evidence, err := c.resolver.ResolveFrontier(ctx, observation.Frontier.Value)
	if err != nil {
		return interopengine.VerificationResult{}, fmt.Errorf("resolve frontier evidence: %w", err)
	}
	verification, err := c.verifier.Verify(ctx, observation, evidence)
	if err != nil {
		return interopengine.VerificationResult{}, fmt.Errorf("verify frontier: %w", err)
	}
	return verification, nil
}

func acceptedTS(state interopengine.InteropState) *uint64 {
	if state.Accepted == nil {
		return nil
	}
	ts := state.Accepted.Timestamp
	return &ts
}

func (c *Controller) frontierTS(state interopengine.InteropState) uint64 {
	if state.Accepted == nil {
		return c.activationTimestamp
	}
	return state.Accepted.Timestamp + 1
}

func pendingEffects(effects []interopengine.Effect) []interopengine.PendingEffect {
	if len(effects) == 0 {
		return nil
	}
	out := make([]interopengine.PendingEffect, 0, len(effects))
	seen := make(map[string]struct{}, len(effects))
	for _, effect := range effects {
		id := interopengine.EffectID(effect)
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		out = append(out, interopengine.PendingEffect{
			ID:     id,
			Effect: effect,
		})
	}
	return out
}
