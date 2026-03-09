package engine

import (
	"errors"
	"fmt"
	"maps"
	"slices"

	"github.com/ethereum-optimism/optimism/op-service/eth"
)

var (
	ErrPendingEffectsNotDrained = errors.New("pending effects must be drained before step")
	ErrRewindRequiresHistory    = errors.New("rewind requires accepted history support")
)

func ValidateConfig(cfg Config) error {
	return nil
}

func ValidateState(cfg Config, state InteropState) error {
	if err := ValidateConfig(cfg); err != nil {
		return err
	}
	if len(state.PendingEffects) > 0 {
		return ErrPendingEffectsNotDrained
	}
	if state.Accepted == nil {
		if len(state.AcceptedHistory) != 0 {
			return errors.New("accepted history must be empty when accepted snapshot is nil")
		}
		if state.LastValidatedTS != nil {
			return errors.New("last validated timestamp must be nil when accepted snapshot is nil")
		}
	} else {
		if len(state.AcceptedHistory) == 0 {
			return errors.New("accepted history must include the accepted snapshot")
		}
		if state.LastValidatedTS == nil {
			return errors.New("last validated timestamp must be set when accepted snapshot is present")
		}
		if *state.LastValidatedTS != state.Accepted.Timestamp {
			return fmt.Errorf("last validated timestamp %d does not match accepted timestamp %d", *state.LastValidatedTS, state.Accepted.Timestamp)
		}
		if state.Accepted.L1Inclusion != maxL1Head(state.Accepted.L1Heads) {
			return errors.New("accepted L1 inclusion must equal max accepted L1 head")
		}
		historyAccepted, ok := state.AcceptedHistory[state.Accepted.Timestamp]
		if !ok {
			return fmt.Errorf("accepted history missing snapshot at timestamp %d", state.Accepted.Timestamp)
		}
		if !EqualAcceptedSnapshots(state.Accepted, &historyAccepted) {
			return fmt.Errorf("accepted history snapshot at timestamp %d does not match accepted snapshot", state.Accepted.Timestamp)
		}
		for ts, snapshot := range state.AcceptedHistory {
			if ts > state.Accepted.Timestamp {
				return fmt.Errorf("accepted history contains future snapshot at timestamp %d beyond accepted %d", ts, state.Accepted.Timestamp)
			}
			if snapshot.Timestamp != ts {
				return fmt.Errorf("accepted history key %d does not match snapshot timestamp %d", ts, snapshot.Timestamp)
			}
			if snapshot.L1Inclusion != maxL1Head(snapshot.L1Heads) {
				return fmt.Errorf("accepted history snapshot at timestamp %d has invalid L1 inclusion", ts)
			}
		}
		for ts := cfg.ActivationTimestamp; ts <= state.Accepted.Timestamp; ts++ {
			if _, ok := state.AcceptedHistory[ts]; !ok {
				return fmt.Errorf("accepted history missing timestamp %d", ts)
			}
		}
	}
	for _, ts := range sortedDeniedTimestamps(state.DeniedByTS) {
		for idx, decision := range state.DeniedByTS[ts] {
			if decision.Timestamp != ts {
				return fmt.Errorf("denied decision %d stored under timestamp %d has timestamp %d", idx, ts, decision.Timestamp)
			}
			if decision.DeniedFrontier.Timestamp != ts {
				return fmt.Errorf("denied frontier %d stored under timestamp %d has frontier timestamp %d", idx, ts, decision.DeniedFrontier.Timestamp)
			}
			if len(decision.InvalidHeads) == 0 {
				return fmt.Errorf("denied decision %d at timestamp %d must include invalid heads", idx, ts)
			}
			if decision.DeniedFrontier.L1Inclusion != maxL1Head(decision.DeniedFrontier.L1Heads) {
				return fmt.Errorf("denied decision %d at timestamp %d has invalid L1 inclusion", idx, ts)
			}
		}
	}
	return nil
}

func ValidateInput(cfg Config, state InteropState, input StepInput) error {
	if err := ValidateVerification(input.Verification); err != nil {
		return err
	}
	expectedFrontierTS := cfg.ActivationTimestamp
	if state.Accepted != nil {
		expectedFrontierTS = state.Accepted.Timestamp + 1
	}
	if input.Observation.FrontierTS != expectedFrontierTS {
		return fmt.Errorf("frontier timestamp %d does not match expected %d", input.Observation.FrontierTS, expectedFrontierTS)
	}
	if input.Verification.Timestamp != input.Observation.FrontierTS {
		return fmt.Errorf("verification timestamp %d does not match frontier timestamp %d", input.Verification.Timestamp, input.Observation.FrontierTS)
	}
	if state.Accepted == nil {
		if input.Observation.Accepted.Present {
			return errors.New("accepted observation must be absent before activation")
		}
		if input.Observation.Accepted.Reason != AvailabilityPreActivation {
			return fmt.Errorf("accepted observation reason must be pre-activation before activation, got %d", input.Observation.Accepted.Reason)
		}
	} else {
		if input.Observation.AcceptedTS != state.Accepted.Timestamp {
			return fmt.Errorf("accepted observation timestamp %d does not match state accepted timestamp %d", input.Observation.AcceptedTS, state.Accepted.Timestamp)
		}
		if input.Observation.Accepted.Reason == AvailabilityPreActivation {
			return errors.New("accepted observation cannot be pre-activation when accepted snapshot exists")
		}
	}
	return nil
}

func ValidateVerification(v VerificationResult) error {
	switch v.Status {
	case VerificationInvalid:
		if len(v.InvalidHeads) == 0 {
			return errors.New("verification invalid result must include invalid heads")
		}
	default:
		if len(v.InvalidHeads) != 0 {
			return errors.New("verification invalid heads must be empty unless status is invalid")
		}
	}
	return nil
}

func maxL1Head(heads map[eth.ChainID]eth.BlockID) eth.BlockID {
	var maxHead eth.BlockID
	first := true
	for _, head := range heads {
		if first || head.Number > maxHead.Number {
			maxHead = head
			first = false
		}
	}
	return maxHead
}

func identicalDeniedFrontierExists(decisions []DeniedDecision, frontier FrontierSnapshot) bool {
	for _, decision := range decisions {
		if EqualFrontierSnapshots(&decision.DeniedFrontier, &frontier) {
			return true
		}
	}
	return false
}

func pruneStaleDeniedFrontiers(decisions []DeniedDecision, frontier FrontierSnapshot) []DeniedDecision {
	filtered := make([]DeniedDecision, 0, len(decisions))
	for _, decision := range decisions {
		if EqualFrontierSnapshots(&decision.DeniedFrontier, &frontier) {
			filtered = append(filtered, decision)
		}
	}
	return filtered
}

func pruneDeniedAfter(deniedByTS map[uint64][]DeniedDecision, keepTS *uint64) map[uint64][]DeniedDecision {
	if len(deniedByTS) == 0 {
		return deniedByTS
	}
	if keepTS == nil {
		return map[uint64][]DeniedDecision{}
	}
	filtered := make(map[uint64][]DeniedDecision, len(deniedByTS))
	for ts, decisions := range deniedByTS {
		if ts <= *keepTS {
			filtered[ts] = decisions
		}
	}
	return filtered
}

func dedupPendingEffects(effects []Effect) []PendingEffect {
	if len(effects) == 0 {
		return nil
	}
	byID := make(map[string]PendingEffect, len(effects))
	for _, effect := range effects {
		id := EffectID(effect)
		byID[id] = PendingEffect{ID: id, Effect: effect}
	}
	ids := slices.Collect(maps.Keys(byID))
	slices.Sort(ids)
	out := make([]PendingEffect, 0, len(ids))
	for _, id := range ids {
		out = append(out, byID[id])
	}
	return out
}
