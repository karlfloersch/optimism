package store

import (
	"fmt"

	interopengine "github.com/ethereum-optimism/optimism/op-supernode/supernode/activity/interop/engine"
)

type encodedState struct {
	Accepted        *interopengine.AcceptedSnapshot           `json:"accepted"`
	AcceptedHistory map[uint64]interopengine.AcceptedSnapshot `json:"acceptedHistory"`
	DeniedByTS      map[uint64][]interopengine.DeniedDecision `json:"deniedByTS"`
	LastValidatedTS *uint64                                   `json:"lastValidatedTS"`
	PendingEffects  []encodedPendingEffect                    `json:"pendingEffects"`
}

type encodedPendingEffect struct {
	ID            string                                      `json:"id"`
	Type          string                                      `json:"type"`
	Rewind        *interopengine.RewindAcceptedState          `json:"rewind,omitempty"`
	ResetChain    *interopengine.ResetChainToAccepted         `json:"resetChain,omitempty"`
	PruneDenied   *interopengine.PruneDeniedDecisions         `json:"pruneDenied,omitempty"`
	PruneFrontier *interopengine.PruneFrontierDeniedDecisions `json:"pruneFrontier,omitempty"`
	ClearDenied   *interopengine.ClearDeniedDecisions         `json:"clearDenied,omitempty"`
	Invalidate    *interopengine.InvalidateChainHead          `json:"invalidate,omitempty"`
}

func encodeState(state interopengine.InteropState) (encodedState, error) {
	out := encodedState{
		Accepted:        state.Accepted,
		AcceptedHistory: state.AcceptedHistory,
		DeniedByTS:      state.DeniedByTS,
		LastValidatedTS: state.LastValidatedTS,
		PendingEffects:  make([]encodedPendingEffect, 0, len(state.PendingEffects)),
	}
	for _, pending := range state.PendingEffects {
		encoded, err := encodePendingEffect(pending)
		if err != nil {
			return encodedState{}, err
		}
		out.PendingEffects = append(out.PendingEffects, encoded)
	}
	return out, nil
}

func encodePendingEffect(pending interopengine.PendingEffect) (encodedPendingEffect, error) {
	out := encodedPendingEffect{ID: pending.ID}
	switch effect := pending.Effect.(type) {
	case interopengine.RewindAcceptedState:
		out.Type = "rewind-accepted"
		e := effect
		out.Rewind = &e
	case interopengine.ResetChainToAccepted:
		out.Type = "reset-chain"
		e := effect
		out.ResetChain = &e
	case interopengine.PruneDeniedDecisions:
		out.Type = "prune-denied"
		e := effect
		out.PruneDenied = &e
	case interopengine.PruneFrontierDeniedDecisions:
		out.Type = "prune-frontier"
		e := effect
		out.PruneFrontier = &e
	case interopengine.ClearDeniedDecisions:
		out.Type = "clear-denied"
		e := effect
		out.ClearDenied = &e
	case interopengine.InvalidateChainHead:
		out.Type = "invalidate-chain-head"
		e := effect
		out.Invalidate = &e
	default:
		return encodedPendingEffect{}, fmt.Errorf("unsupported pending effect type %T", pending.Effect)
	}
	return out, nil
}

func (e encodedState) decode() (interopengine.InteropState, error) {
	out := interopengine.InteropState{
		Accepted:        e.Accepted,
		AcceptedHistory: e.AcceptedHistory,
		DeniedByTS:      e.DeniedByTS,
		LastValidatedTS: e.LastValidatedTS,
		PendingEffects:  make([]interopengine.PendingEffect, 0, len(e.PendingEffects)),
	}
	if out.AcceptedHistory == nil {
		out.AcceptedHistory = map[uint64]interopengine.AcceptedSnapshot{}
	}
	if out.DeniedByTS == nil {
		out.DeniedByTS = map[uint64][]interopengine.DeniedDecision{}
	}
	for _, pending := range e.PendingEffects {
		decoded, err := pending.decode()
		if err != nil {
			return interopengine.InteropState{}, err
		}
		out.PendingEffects = append(out.PendingEffects, decoded)
	}
	return out, nil
}

func (e encodedPendingEffect) decode() (interopengine.PendingEffect, error) {
	switch e.Type {
	case "rewind-accepted":
		if e.Rewind == nil {
			return interopengine.PendingEffect{}, fmt.Errorf("pending effect %s missing rewind payload", e.ID)
		}
		return interopengine.PendingEffect{ID: e.ID, Effect: *e.Rewind}, nil
	case "reset-chain":
		if e.ResetChain == nil {
			return interopengine.PendingEffect{}, fmt.Errorf("pending effect %s missing reset-chain payload", e.ID)
		}
		return interopengine.PendingEffect{ID: e.ID, Effect: *e.ResetChain}, nil
	case "prune-denied":
		if e.PruneDenied == nil {
			return interopengine.PendingEffect{}, fmt.Errorf("pending effect %s missing prune-denied payload", e.ID)
		}
		return interopengine.PendingEffect{ID: e.ID, Effect: *e.PruneDenied}, nil
	case "prune-frontier":
		if e.PruneFrontier == nil {
			return interopengine.PendingEffect{}, fmt.Errorf("pending effect %s missing prune-frontier payload", e.ID)
		}
		return interopengine.PendingEffect{ID: e.ID, Effect: *e.PruneFrontier}, nil
	case "clear-denied":
		if e.ClearDenied == nil {
			return interopengine.PendingEffect{}, fmt.Errorf("pending effect %s missing clear-denied payload", e.ID)
		}
		return interopengine.PendingEffect{ID: e.ID, Effect: *e.ClearDenied}, nil
	case "invalidate-chain-head":
		if e.Invalidate == nil {
			return interopengine.PendingEffect{}, fmt.Errorf("pending effect %s missing invalidate payload", e.ID)
		}
		return interopengine.PendingEffect{ID: e.ID, Effect: *e.Invalidate}, nil
	default:
		return interopengine.PendingEffect{}, fmt.Errorf("unsupported pending effect type %q", e.Type)
	}
}
