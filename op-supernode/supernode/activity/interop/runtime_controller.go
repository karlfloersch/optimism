package interop

import (
	"context"
	"fmt"
	"maps"

	"github.com/ethereum-optimism/optimism/op-service/eth"
	interopcontroller "github.com/ethereum-optimism/optimism/op-supernode/supernode/activity/interop/controller"
	interopengine "github.com/ethereum-optimism/optimism/op-supernode/supernode/activity/interop/engine"
)

type runtimeObservationSource struct {
	interop *Interop
}

func (s *runtimeObservationSource) ObserveRound(_ context.Context, acceptedTS *uint64, frontierTS uint64) (interopengine.RoundObservation, error) {
	if pauseTS := s.interop.pauseAtTimestamp.Load(); pauseTS != 0 && frontierTS >= pauseTS {
		return interopengine.RoundObservation{
			AcceptedTS: acceptedTimestampValue(acceptedTS),
			Accepted:   mustObserveAccepted(s.interop, acceptedTS),
			FrontierTS: frontierTS,
			Frontier: interopengine.SnapshotAvailability[interopengine.FrontierSnapshot]{
				Present: false,
				Reason:  interopengine.AvailabilityNotReady,
			},
		}, nil
	}

	accepted, err := s.interop.observeAcceptedSnapshot(acceptedTS)
	if err != nil {
		return interopengine.RoundObservation{}, err
	}
	frontier, err := s.interop.observeFrontierSnapshot(frontierTS)
	if err != nil {
		return interopengine.RoundObservation{}, err
	}
	return interopengine.RoundObservation{
		AcceptedTS: acceptedTimestampValue(acceptedTS),
		Accepted:   accepted,
		FrontierTS: frontierTS,
		Frontier:   frontier,
	}, nil
}

func mustObserveAccepted(i *Interop, acceptedTS *uint64) interopengine.SnapshotAvailability[interopengine.AcceptedSnapshot] {
	accepted, err := i.observeAcceptedSnapshot(acceptedTS)
	if err != nil {
		i.log.Error("failed to observe accepted snapshot while paused", "acceptedTS", acceptedTS, "err", err)
		return interopengine.SnapshotAvailability[interopengine.AcceptedSnapshot]{
			Present: false,
			Reason:  interopengine.AvailabilityConflict,
		}
	}
	return accepted
}

func acceptedTimestampValue(ts *uint64) uint64 {
	if ts == nil {
		return 0
	}
	return *ts
}

type runtimeEvidenceResolver struct {
	interop *Interop
}

func (r *runtimeEvidenceResolver) ResolveFrontier(ctx context.Context, frontier interopengine.FrontierSnapshot) (interopcontroller.FrontierEvidence, error) {
	return resolveRuntimeFrontierEvidence(ctx, r.interop, frontier)
}

type runtimeVerifier struct {
	interop *Interop
}

func (v *runtimeVerifier) Verify(_ context.Context, observation interopengine.RoundObservation, evidence interopcontroller.FrontierEvidence) (interopengine.VerificationResult, error) {
	frontier := observation.Frontier.Value
	if err := v.interop.loadLogsFromEvidence(frontier.Timestamp, frontier.L2Heads, evidence.Blocks); err != nil {
		if err == ErrPreviousTimestampNotSealed {
			return interopengine.VerificationResult{
				Timestamp: frontier.Timestamp,
				Status:    interopengine.VerificationNotReady,
			}, nil
		}
		return interopengine.VerificationResult{}, fmt.Errorf("load logs for frontier %d: %w", frontier.Timestamp, err)
	}

	result, err := v.interop.verifyFn(frontier.Timestamp, frontier.L2Heads)
	if err != nil {
		return interopengine.VerificationResult{}, err
	}
	cycleResult, err := v.interop.cycleVerifyFn(frontier.Timestamp, frontier.L2Heads)
	if err != nil {
		return interopengine.VerificationResult{}, fmt.Errorf("cycle verification failed: %w", err)
	}
	if len(cycleResult.InvalidHeads) > 0 {
		if result.InvalidHeads == nil {
			result.InvalidHeads = make(map[eth.ChainID]eth.BlockID)
		}
		for chainID, invalidBlock := range cycleResult.InvalidHeads {
			result.InvalidHeads[chainID] = invalidBlock
		}
	}

	if !runtimeResultMatchesFrontier(result, frontier) {
		return interopengine.VerificationResult{
			Timestamp: frontier.Timestamp,
			Status:    interopengine.VerificationConflict,
		}, nil
	}

	verification := interopengine.VerificationResult{
		Timestamp: frontier.Timestamp,
	}
	if len(result.InvalidHeads) > 0 {
		verification.Status = interopengine.VerificationInvalid
		verification.InvalidHeads = maps.Clone(result.InvalidHeads)
		return verification, nil
	}
	verification.Status = interopengine.VerificationValid
	return verification, nil
}

func resolveRuntimeFrontierEvidence(ctx context.Context, i *Interop, frontier interopengine.FrontierSnapshot) (interopcontroller.FrontierEvidence, error) {
	evidence := interopcontroller.FrontierEvidence{
		Timestamp: frontier.Timestamp,
		Blocks:    make(map[eth.ChainID]interopcontroller.BlockEvidence, len(frontier.L2Heads)),
	}
	for chainID, blockID := range frontier.L2Heads {
		chain, ok := i.chains[chainID]
		if !ok {
			return interopcontroller.FrontierEvidence{}, fmt.Errorf("missing chain %s for frontier evidence", chainID)
		}
		blockInfo, receipts, err := chain.FetchReceipts(ctx, blockID)
		if err != nil {
			return interopcontroller.FrontierEvidence{}, fmt.Errorf("fetch receipts for chain %s block %s: %w", chainID, blockID, err)
		}
		evidence.Blocks[chainID] = interopcontroller.BlockEvidence{
			BlockInfo: blockInfo,
			Receipts:  receipts,
		}
	}
	return evidence, nil
}

func runtimeResultMatchesFrontier(result Result, frontier interopengine.FrontierSnapshot) bool {
	if result.Timestamp != frontier.Timestamp {
		return false
	}
	if result.L1Inclusion != frontier.L1Inclusion {
		return false
	}
	return maps.Equal(result.L2Heads, frontier.L2Heads)
}

type runtimeEffectRunner struct {
	interop *Interop
}

func (r *runtimeEffectRunner) Run(ctx context.Context, pending []interopengine.PendingEffect) error {
	for _, item := range pending {
		switch effect := item.Effect.(type) {
		case interopengine.InvalidateChainHead:
			if err := r.interop.invalidateBlock(effect.ChainID, effect.Block, effect.Timestamp); err != nil {
				return err
			}
		case interopengine.PruneDeniedDecisions:
			for chainID, chain := range r.interop.chains {
				removed, err := chain.PruneDeniedAfterTimestamp(effect.AfterTimestamp)
				if err != nil {
					return fmt.Errorf("prune denied after timestamp on chain %s: %w", chainID, err)
				}
				if len(removed) > 0 {
					r.interop.log.Info("pruned denied decisions after accepted rewind", "chainID", chainID, "afterTimestamp", effect.AfterTimestamp, "removedHeights", len(removed))
				}
			}
		case interopengine.PruneFrontierDeniedDecisions:
			for chainID, chain := range r.interop.chains {
				removed, err := chain.PruneDeniedAtTimestamp(effect.Timestamp)
				if err != nil {
					return fmt.Errorf("prune denied at timestamp on chain %s: %w", chainID, err)
				}
				if len(removed) > 0 {
					r.interop.log.Info("pruned denied decisions at frontier timestamp", "chainID", chainID, "timestamp", effect.Timestamp, "removedHeights", len(removed))
				}
			}
		case interopengine.ClearDeniedDecisions:
			for chainID, chain := range r.interop.chains {
				removed, err := chain.ClearDenied()
				if err != nil {
					return fmt.Errorf("clear denied decisions on chain %s: %w", chainID, err)
				}
				if len(removed) > 0 {
					r.interop.log.Info("cleared denied decisions", "chainID", chainID, "removedHeights", len(removed))
				}
			}
		case interopengine.ResetChainToAccepted:
			db, ok := r.interop.logsDBs[effect.ChainID]
			if !ok {
				return fmt.Errorf("logsDB not found for chain %s", effect.ChainID)
			}
			if err := r.interop.rewindLogsDBToHead(effect.ChainID, db, effect.L2Head); err != nil {
				return err
			}
			chain, ok := r.interop.chains[effect.ChainID]
			if !ok {
				return fmt.Errorf("chain %s not found for reset", effect.ChainID)
			}
			if err := chain.RewindEngine(ctx, effect.Timestamp, eth.BlockRef{}); err != nil {
				return fmt.Errorf("rewind chain %s to accepted timestamp %d: %w", effect.ChainID, effect.Timestamp, err)
			}
		case interopengine.RewindAcceptedState:
			// The accepted state is already updated atomically in the state store.
			continue
		default:
			return fmt.Errorf("unsupported interop effect %T", effect)
		}
	}
	return nil
}
