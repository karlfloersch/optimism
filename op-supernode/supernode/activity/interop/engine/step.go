package engine

import (
	"fmt"
	"maps"
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
	newState.DeniedByTS[frontier.Timestamp] = pruneStaleDeniedFrontiers(newState.DeniedByTS[frontier.Timestamp], frontier)
	if len(newState.DeniedByTS[frontier.Timestamp]) == 0 {
		delete(newState.DeniedByTS, frontier.Timestamp)
	}
	switch input.Verification.Status {
	case VerificationNotReady:
		return StepResult{NewState: newState, Outcome: OutcomeWait}, nil
	case VerificationConflict:
		return StepResult{NewState: newState, Outcome: OutcomeConflict}, nil
	case VerificationInvalid:
		newState.DeniedByTS[frontier.Timestamp] = []DeniedDecision{{
			Timestamp:      frontier.Timestamp,
			DeniedFrontier: cloneFrontierSnapshot(frontier),
			InvalidHeads:   maps.Clone(input.Verification.InvalidHeads),
		}}
		return StepResult{NewState: newState, Outcome: OutcomeNoOp}, nil
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
		return StepResult{NewState: newState, Outcome: OutcomeAdvance}, nil
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
		state.Accepted = nil
		state.LastValidatedTS = nil
		state.DeniedByTS = map[uint64][]DeniedDecision{}
		return StepResult{NewState: state, Outcome: OutcomeRewind}, nil
	}

	prevTS := currentTS - 1
	prev, ok := state.AcceptedHistory[prevTS]
	if !ok {
		return StepResult{}, ErrRewindRequiresHistory
	}
	state.Accepted = &prev
	rewoundTS := prevTS
	state.LastValidatedTS = &rewoundTS
	state.DeniedByTS = pruneDeniedAfter(state.DeniedByTS, &rewoundTS)
	return StepResult{NewState: state, Outcome: OutcomeRewind}, nil
}
