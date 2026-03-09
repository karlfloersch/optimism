package engine

import (
	"fmt"
	"maps"
	"slices"

	"github.com/ethereum-optimism/optimism/op-service/eth"
)

type Config struct {
	ActivationTimestamp uint64
}

type AvailabilityReason uint8

const (
	AvailabilityPresent AvailabilityReason = iota
	AvailabilityPreActivation
	AvailabilityNotReady
	AvailabilityConflict
)

type SnapshotAvailability[T any] struct {
	Present bool
	Reason  AvailabilityReason
	Value   T
}

type AcceptedSnapshot struct {
	Timestamp   uint64
	L1Inclusion eth.BlockID
	L1Heads     map[eth.ChainID]eth.BlockID
	L2Heads     map[eth.ChainID]eth.BlockID
}

type FrontierSnapshot struct {
	Timestamp   uint64
	L1Inclusion eth.BlockID
	L1Heads     map[eth.ChainID]eth.BlockID
	L2Heads     map[eth.ChainID]eth.BlockID
}

type DeniedDecision struct {
	Timestamp      uint64
	DeniedFrontier FrontierSnapshot
	InvalidHeads   map[eth.ChainID]eth.BlockID
}

type InteropState struct {
	Accepted        *AcceptedSnapshot
	AcceptedHistory map[uint64]AcceptedSnapshot
	DeniedByTS      map[uint64][]DeniedDecision
	LastValidatedTS *uint64
	PendingEffects  []PendingEffect
}

type RoundObservation struct {
	AcceptedTS uint64
	Accepted   SnapshotAvailability[AcceptedSnapshot]
	FrontierTS uint64
	Frontier   SnapshotAvailability[FrontierSnapshot]
}

type VerificationStatus uint8

const (
	VerificationValid VerificationStatus = iota
	VerificationInvalid
	VerificationNotReady
	VerificationConflict
)

type VerificationResult struct {
	Timestamp    uint64
	Status       VerificationStatus
	InvalidHeads map[eth.ChainID]eth.BlockID
}

type Outcome uint8

const (
	OutcomeNoOp Outcome = iota
	OutcomeWait
	OutcomeAdvance
	OutcomeRewind
	OutcomeConflict
)

type Effect interface {
	effectID() string
}

type RewindAcceptedState struct {
	ToTimestamp uint64
}

func (e RewindAcceptedState) effectID() string {
	return fmt.Sprintf("rewind-accepted:%d", e.ToTimestamp)
}

type ResetChainToAccepted struct {
	ChainID   eth.ChainID
	Timestamp uint64
	L2Head    eth.BlockID
}

func (e ResetChainToAccepted) effectID() string {
	return fmt.Sprintf("reset-chain:%s:%d:%s", e.ChainID.String(), e.Timestamp, e.L2Head.Hash.Hex())
}

type PruneDeniedDecisions struct {
	AfterTimestamp uint64
}

func (e PruneDeniedDecisions) effectID() string {
	return fmt.Sprintf("prune-denied-after:%d", e.AfterTimestamp)
}

type PruneFrontierDeniedDecisions struct {
	Timestamp uint64
}

func (e PruneFrontierDeniedDecisions) effectID() string {
	return fmt.Sprintf("prune-frontier-denied:%d", e.Timestamp)
}

type PendingEffect struct {
	ID     string
	Effect Effect
}

type StepInput struct {
	Observation  RoundObservation
	Verification VerificationResult
}

type StepResult struct {
	NewState InteropState
	Effects  []Effect
	Outcome  Outcome
}

func SnapshotFromFrontier(s FrontierSnapshot) AcceptedSnapshot {
	return AcceptedSnapshot{
		Timestamp:   s.Timestamp,
		L1Inclusion: s.L1Inclusion,
		L1Heads:     maps.Clone(s.L1Heads),
		L2Heads:     maps.Clone(s.L2Heads),
	}
}

func EqualAcceptedSnapshots(a, b *AcceptedSnapshot) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return a.Timestamp == b.Timestamp &&
		a.L1Inclusion == b.L1Inclusion &&
		maps.Equal(a.L1Heads, b.L1Heads) &&
		maps.Equal(a.L2Heads, b.L2Heads)
}

func EqualFrontierSnapshots(a, b *FrontierSnapshot) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return a.Timestamp == b.Timestamp &&
		a.L1Inclusion == b.L1Inclusion &&
		maps.Equal(a.L1Heads, b.L1Heads) &&
		maps.Equal(a.L2Heads, b.L2Heads)
}

func EffectID(effect Effect) string {
	return effect.effectID()
}

func CopyState(state InteropState) InteropState {
	out := InteropState{
		AcceptedHistory: make(map[uint64]AcceptedSnapshot, len(state.AcceptedHistory)),
		DeniedByTS:      make(map[uint64][]DeniedDecision, len(state.DeniedByTS)),
		PendingEffects:  make([]PendingEffect, 0, len(state.PendingEffects)),
	}
	if state.Accepted != nil {
		snapshot := *state.Accepted
		snapshot.L1Heads = maps.Clone(state.Accepted.L1Heads)
		snapshot.L2Heads = maps.Clone(state.Accepted.L2Heads)
		out.Accepted = &snapshot
	}
	if state.LastValidatedTS != nil {
		ts := *state.LastValidatedTS
		out.LastValidatedTS = &ts
	}
	for ts, snapshot := range state.AcceptedHistory {
		out.AcceptedHistory[ts] = AcceptedSnapshot{
			Timestamp:   snapshot.Timestamp,
			L1Inclusion: snapshot.L1Inclusion,
			L1Heads:     maps.Clone(snapshot.L1Heads),
			L2Heads:     maps.Clone(snapshot.L2Heads),
		}
	}
	for ts, decisions := range state.DeniedByTS {
		cloned := make([]DeniedDecision, 0, len(decisions))
		for _, decision := range decisions {
			cloned = append(cloned, DeniedDecision{
				Timestamp:      decision.Timestamp,
				DeniedFrontier: cloneFrontierSnapshot(decision.DeniedFrontier),
				InvalidHeads:   maps.Clone(decision.InvalidHeads),
			})
		}
		out.DeniedByTS[ts] = cloned
	}
	for _, pending := range state.PendingEffects {
		out.PendingEffects = append(out.PendingEffects, pending)
	}
	return out
}

func cloneFrontierSnapshot(snapshot FrontierSnapshot) FrontierSnapshot {
	return FrontierSnapshot{
		Timestamp:   snapshot.Timestamp,
		L1Inclusion: snapshot.L1Inclusion,
		L1Heads:     maps.Clone(snapshot.L1Heads),
		L2Heads:     maps.Clone(snapshot.L2Heads),
	}
}

func sortedDeniedTimestamps(deniedByTS map[uint64][]DeniedDecision) []uint64 {
	timestamps := slices.Collect(maps.Keys(deniedByTS))
	slices.Sort(timestamps)
	return timestamps
}

func sortedAcceptedTimestamps(history map[uint64]AcceptedSnapshot) []uint64 {
	timestamps := slices.Collect(maps.Keys(history))
	slices.Sort(timestamps)
	return timestamps
}
