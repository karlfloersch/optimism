package supernode

// This file collects Supernode methods that forward to the interop activity
// and are intended for integration tests and debugging tooling only. They
// must not be called by production code paths. Keeping them in one file
// makes the test-only surface easy to audit alongside
// interop/interop_test_access.go.

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/ethereum-optimism/optimism/op-service/eth"
	"github.com/ethereum-optimism/optimism/op-supernode/supernode/activity"
	"github.com/ethereum-optimism/optimism/op-supernode/supernode/activity/interop"
	suptypes "github.com/ethereum-optimism/optimism/op-supervisor/supervisor/types"
)

var errNoInteropActivity = errors.New("supernode: no interop activity")

// findInteropActivity returns the single interop activity, if present.
func (s *Supernode) findInteropActivity() *interop.Interop {
	for _, a := range s.activities {
		if ia, ok := a.(*interop.Interop); ok {
			return ia
		}
	}
	return nil
}

// ---------------------------------------------------------------------------
// Pause / Resume
// ---------------------------------------------------------------------------

// PauseInteropActivity pauses the interop activity at the given timestamp.
// When the interop activity attempts to process this timestamp, it returns
// early without processing.
func (s *Supernode) PauseInteropActivity(ts uint64) {
	ia := s.findInteropActivity()
	if ia == nil {
		s.log.Warn("PauseInterop called but no interop activity found")
		return
	}
	ia.PauseAt(ts)
}

// ResumeInteropActivity clears any pause on the interop activity, allowing
// normal processing to continue.
func (s *Supernode) ResumeInteropActivity() {
	ia := s.findInteropActivity()
	if ia == nil {
		s.log.Warn("ResumeInterop called but no interop activity found")
		return
	}
	ia.Resume()
}

// ---------------------------------------------------------------------------
// Backfill observability & injection
// ---------------------------------------------------------------------------

// InteropBackfillAttempts returns the number of log-backfill attempts the
// interop activity has made since its most recent Start. Returns 0 if there
// is no interop activity.
func (s *Supernode) InteropBackfillAttempts() int32 {
	ia := s.findInteropActivity()
	if ia == nil {
		return 0
	}
	return ia.BackfillAttempts()
}

// InteropBackfillCompleted reports whether the interop activity has finished
// its log backfill phase. Returns false if there is no interop activity.
func (s *Supernode) InteropBackfillCompleted() bool {
	ia := s.findInteropActivity()
	if ia == nil {
		return false
	}
	return ia.BackfillCompleted()
}

// ---------------------------------------------------------------------------
// Activation-timestamp inspection
// ---------------------------------------------------------------------------

// InteropActivationTimestamp returns the immutable protocol activation
// timestamp of the interop activity. Returns 0 if there is no interop activity.
func (s *Supernode) InteropActivationTimestamp() uint64 {
	ia := s.findInteropActivity()
	if ia == nil {
		return 0
	}
	return ia.ActivationTimestamp()
}

// InteropRuntimeActivationTimestamp returns the (possibly-advanced-by-backfill)
// runtime activation timestamp of the interop activity. Returns 0 if there is
// no interop activity.
func (s *Supernode) InteropRuntimeActivationTimestamp() uint64 {
	ia := s.findInteropActivity()
	if ia == nil {
		return 0
	}
	return ia.RuntimeActivationTimestamp()
}

// ---------------------------------------------------------------------------
// LogsDB inspection
// ---------------------------------------------------------------------------

// InteropFirstSealedBlock returns the earliest block sealed in the interop
// logs DB for the given chain. Returns an error if there is no interop
// activity, the chain is unknown, or the DB is empty.
func (s *Supernode) InteropFirstSealedBlock(chainID eth.ChainID) (suptypes.BlockSeal, error) {
	ia := s.findInteropActivity()
	if ia == nil {
		return suptypes.BlockSeal{}, errNoInteropActivity
	}
	return ia.FirstSealedBlock(chainID)
}

// InteropLatestSealedBlock returns the most recent block sealed in the interop
// logs DB for the given chain.
func (s *Supernode) InteropLatestSealedBlock(chainID eth.ChainID) (suptypes.BlockSeal, bool, error) {
	ia := s.findInteropActivity()
	if ia == nil {
		return suptypes.BlockSeal{}, false, errNoInteropActivity
	}
	return ia.LatestSealedBlock(chainID)
}

// ---------------------------------------------------------------------------
// Interop-activity hot restart
// ---------------------------------------------------------------------------

// verifierReplacer is the subset of simpleChainContainer we depend on in
// RestartInteropActivity to swap a verifier registration without touching
// the public ChainContainer interface.
type verifierReplacer interface {
	ReplaceVerifier(old, new activity.VerificationActivity) bool
}

// RestartInteropActivity stops the running interop activity (if any),
// optionally wipes its on-disk logs DB files, constructs a fresh instance
// from the originally-configured parameters, re-registers it with each chain
// container as a verifier, and starts it under the supernode's existing
// lifecycle context. The HTTP server, chain containers, and all other
// activities keep running. This is the core primitive for tests that want
// to exercise log backfill against a running, ready cluster without the
// cost and flakiness of restarting the entire supernode.
//
// preInjectBackfillFailures, if positive, is applied to the replacement
// activity atomically before its background goroutine is launched, so the
// very first runLogBackfill call on the new activity will observe the
// injection. Any other test-only mutations on the old activity are
// discarded when it is Stopped.
func (s *Supernode) RestartInteropActivity(wipeLogsDBs bool, preInjectBackfillFailures int32) error {
	if s.lifecycleCtx == nil {
		return fmt.Errorf("supernode: RestartInteropActivity called before Start")
	}
	if s.interopActivationTs == nil {
		return fmt.Errorf("supernode: RestartInteropActivity called but interop was never configured")
	}

	old := s.findInteropActivity()
	if old == nil {
		return errNoInteropActivity
	}

	// Stop the old activity: cancels its ctx, waits its loop to exit on its
	// own, then closes verifiedDB and all logs DBs. Safe to ignore errors as
	// Stop only surfaces close errors and we're about to wipe/reopen.
	_ = old.Stop(context.Background())

	if wipeLogsDBs {
		if s.cfg == nil || s.cfg.DataDir == "" {
			return fmt.Errorf("supernode: cannot wipe logs DBs without a configured DataDir")
		}
		for chainID := range s.chains {
			chainDir := filepath.Join(s.cfg.DataDir, fmt.Sprintf("chain-%s", chainID))
			if err := os.RemoveAll(chainDir); err != nil {
				return fmt.Errorf("supernode: wipe chain dir %s: %w", chainDir, err)
			}
			s.log.Info("wiped interop chain data dir", "chain", chainID, "path", chainDir)
		}
	}

	newIA := interop.New(
		s.log.New("activity", "interop"),
		*s.interopActivationTs,
		s.interopMsgExpiryWindow,
		s.chains,
		s.cfg.DataDir,
		s.l1Client,
		s.cfg.InteropLogBackfillDepth,
	)
	if newIA == nil {
		return fmt.Errorf("supernode: failed to construct replacement interop activity")
	}

	if preInjectBackfillFailures > 0 {
		newIA.InjectBackfillFailures(preInjectBackfillFailures)
	}

	// Replace in s.activities so Reset-callback fan-out and test-only accessors
	// find the new instance.
	replaced := false
	for i, a := range s.activities {
		if a == old {
			s.activities[i] = newIA
			replaced = true
			break
		}
	}
	if !replaced {
		return fmt.Errorf("supernode: old interop activity not found in activities slice")
	}

	// Swap verifier registration on every chain container.
	for chainID, chain := range s.chains {
		r, ok := chain.(verifierReplacer)
		if !ok {
			return fmt.Errorf("supernode: chain container for %s does not support ReplaceVerifier", chainID)
		}
		if !r.ReplaceVerifier(old, newIA) {
			return fmt.Errorf("supernode: old interop activity not registered as verifier on chain %s", chainID)
		}
	}

	// Launch the replacement activity on the existing lifecycle context so
	// it shares the supernode's shutdown path. Wait-group participation mirrors
	// how activities are launched in Start().
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		err := newIA.Start(s.lifecycleCtx)
		switch err {
		case nil:
			s.log.Error("activity quit unexpectedly", "name", newIA.Name())
		case context.Canceled:
			s.log.Info("activity closing due to cancelled context", "name", newIA.Name())
		case context.DeadlineExceeded:
			s.log.Warn("activity quit due to deadline exceeded", "name", newIA.Name())
		default:
			s.log.Error("error running restarted interop activity", "name", newIA.Name(), "error", err)
		}
	}()

	s.log.Info("interop activity restarted", "wipedLogsDBs", wipeLogsDBs)
	return nil
}
