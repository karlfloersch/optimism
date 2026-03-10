package engine

import (
	"fmt"
	"maps"

	"github.com/ethereum-optimism/optimism/op-service/eth"
)

type Engine struct {
	cfg Config
}

func New(cfg Config) (*Engine, error) {
	if err := ValidateConfig(cfg); err != nil {
		return nil, err
	}
	return &Engine{cfg: cfg}, nil
}

func (e *Engine) Step(state InteropState, input StepInput) (StepResult, error) {
	if err := ValidateState(e.cfg, state); err != nil {
		return StepResult{}, err
	}
	if err := ValidateInput(e.cfg, state, input); err != nil {
		return StepResult{}, err
	}

	newState := CopyState(state)

	if state.Accepted != nil {
		switch {
		case !input.Observation.Accepted.Present && input.Observation.Accepted.Reason == AvailabilityConflict:
			return StepResult{NewState: newState, Outcome: OutcomeConflict}, nil
		case !input.Observation.Accepted.Present:
			return StepResult{}, fmt.Errorf("accepted observation missing for accepted timestamp %d", state.Accepted.Timestamp)
		case !EqualAcceptedSnapshots(state.Accepted, &input.Observation.Accepted.Value):
			return e.rewindOneStep(newState)
		}
	}

	if !input.Observation.Frontier.Present {
		switch input.Observation.Frontier.Reason {
		case AvailabilityNotReady:
			return StepResult{NewState: newState, Outcome: OutcomeWait}, nil
		case AvailabilityConflict:
			return StepResult{NewState: newState, Outcome: OutcomeConflict}, nil
		default:
			return StepResult{}, fmt.Errorf("frontier snapshot unavailable for unsupported reason %d", input.Observation.Frontier.Reason)
		}
	}

	frontier := input.Observation.Frontier.Value
	existingDenied, hadDenied := newState.DeniedByTS[frontier.Timestamp]
	prunedFrontierDenied := hadDenied && !EqualFrontierSnapshots(&existingDenied.DeniedFrontier, &frontier)
	if prunedFrontierDenied {
		delete(newState.DeniedByTS, frontier.Timestamp)
	}
	switch input.Verification.Status {
	case VerificationNotReady:
		return StepResult{NewState: newState, Effects: pruneFrontierEffects(frontier.Timestamp, prunedFrontierDenied), Outcome: OutcomeWait}, nil
	case VerificationConflict:
		return StepResult{NewState: newState, Effects: pruneFrontierEffects(frontier.Timestamp, prunedFrontierDenied), Outcome: OutcomeConflict}, nil
	case VerificationInvalid:
		newState.DeniedByTS[frontier.Timestamp] = DeniedDecision{
			Timestamp:      frontier.Timestamp,
			DeniedFrontier: cloneFrontierSnapshot(frontier),
			InvalidHeads:   maps.Clone(input.Verification.InvalidHeads),
		}
		effects := make([]Effect, 0, len(input.Verification.InvalidHeads))
		for chainID, block := range input.Verification.InvalidHeads {
			effects = append(effects, InvalidateChainHead{
				ChainID:   chainID,
				Timestamp: frontier.Timestamp,
				Block:     block,
			})
		}
		effects = append(pruneFrontierEffects(frontier.Timestamp, prunedFrontierDenied), effects...)
		return StepResult{NewState: newState, Effects: effects, Outcome: OutcomeNoOp}, nil
	case VerificationValid:
		accepted := SnapshotFromFrontier(frontier)
		newState.Accepted = &accepted
		if newState.AcceptedHistory == nil {
			newState.AcceptedHistory = make(map[uint64]AcceptedSnapshot)
		}
		newState.AcceptedHistory[accepted.Timestamp] = accepted
		ts := accepted.Timestamp
		newState.LastValidatedTS = &ts
		delete(newState.DeniedByTS, frontier.Timestamp)
		return StepResult{NewState: newState, Effects: pruneFrontierEffects(frontier.Timestamp, prunedFrontierDenied), Outcome: OutcomeAdvance}, nil
	default:
		return StepResult{}, fmt.Errorf("unsupported verification status %d", input.Verification.Status)
	}
}

func (e *Engine) rewindOneStep(state InteropState) (StepResult, error) {
	if state.Accepted == nil {
		return StepResult{}, ErrRewindRequiresHistory
	}
	currentTS := state.Accepted.Timestamp
	delete(state.AcceptedHistory, currentTS)
	if currentTS == e.cfg.ActivationTimestamp {
		prunedChains := affectedChainsForDiscardedSuffix(state.DeniedByTS, nil)
		state.Accepted = nil
		state.LastValidatedTS = nil
		state.DeniedByTS = map[uint64]DeniedDecision{}
		effects := []Effect{ClearDeniedDecisions{}}
		resetTS := uint64(0)
		if e.cfg.ActivationTimestamp > 0 {
			resetTS = e.cfg.ActivationTimestamp - 1
		}
		for chainID := range prunedChains {
			effects = append(effects, ResetChainToAccepted{
				ChainID:   chainID,
				Timestamp: resetTS,
				L2Head:    eth.BlockID{},
			})
		}
		return StepResult{NewState: state, Effects: effects, Outcome: OutcomeRewind}, nil
	}

	prevTS := currentTS - 1
	prev, ok := state.AcceptedHistory[prevTS]
	if !ok {
		return StepResult{}, ErrRewindRequiresHistory
	}
	prunedChains := affectedChainsForDiscardedSuffix(state.DeniedByTS, &prevTS)
	state.Accepted = &prev
	rewoundTS := prevTS
	state.LastValidatedTS = &rewoundTS
	state.DeniedByTS = pruneDeniedAfter(state.DeniedByTS, &rewoundTS)
	effects := []Effect{PruneDeniedDecisions{AfterTimestamp: rewoundTS}}
	for chainID := range prunedChains {
		head, ok := prev.L2Heads[chainID]
		if !ok {
			continue
		}
		effects = append(effects, ResetChainToAccepted{
			ChainID:   chainID,
			Timestamp: rewoundTS,
			L2Head:    head,
		})
	}
	return StepResult{NewState: state, Effects: effects, Outcome: OutcomeRewind}, nil
}

func affectedChainsForDiscardedSuffix(deniedByTS map[uint64]DeniedDecision, keepTS *uint64) map[eth.ChainID]struct{} {
	affected := make(map[eth.ChainID]struct{})
	for ts, decision := range deniedByTS {
		if keepTS != nil && ts <= *keepTS {
			continue
		}
		for chainID := range decision.InvalidHeads {
			affected[chainID] = struct{}{}
		}
	}
	return affected
}

func pruneFrontierEffects(timestamp uint64, pruned bool) []Effect {
	if !pruned {
		return nil
	}
	return []Effect{PruneFrontierDeniedDecisions{Timestamp: timestamp}}
}
