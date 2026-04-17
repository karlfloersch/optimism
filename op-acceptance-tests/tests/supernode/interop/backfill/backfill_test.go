package backfill

import (
	"testing"
	"time"

	"github.com/ethereum-optimism/optimism/op-devstack/devtest"
	"github.com/ethereum-optimism/optimism/op-devstack/presets"
)

// backfillDepth is the configured look-back window for log backfill. Any value
// larger than a few block times is sufficient to exercise the full backfill
// path against the local safe tip.
const backfillDepth = 60 * time.Second

// newSupernodeInteropWithBackfill builds a two-L2 interop system with interop
// active at genesis, time-travel enabled, and the supernode configured to run
// log backfill on every Start.
func newSupernodeInteropWithBackfill(t devtest.T) *presets.TwoL2SupernodeInterop {
	return presets.NewTwoL2SupernodeInterop(t, 0,
		presets.WithTimeTravelEnabled(),
		presets.WithInteropLogBackfillDepth(backfillDepth),
	)
}

// awaitRecentlyVerified picks a timestamp slightly in the past of the current
// local safe tip of both chains and waits for the supernode to verify it.
// Returns the timestamp that was verified, which callers can reuse as an
// anchor for later assertions across restarts.
func awaitRecentlyVerified(t devtest.T, sys *presets.TwoL2SupernodeInterop) uint64 {
	blockTime := sys.L2A.Escape().RollupConfig().BlockTime

	var target uint64
	t.Require().Eventually(func() bool {
		statusA := sys.L2ACL.SyncStatus()
		statusB := sys.L2BCL.SyncStatus()
		if statusA.LocalSafeL2.Number < 2 || statusB.LocalSafeL2.Number < 2 {
			return false
		}
		tsA := statusA.LocalSafeL2.Time
		tsB := statusB.LocalSafeL2.Time
		minTs := tsA
		if tsB < minTs {
			minTs = tsB
		}
		// Step one block back to give both chains time to have sealed a
		// block at exactly `target`.
		target = minTs - blockTime
		return target > sys.GenesisTime
	}, 60*time.Second, time.Second, "both chains should make local-safe progress")

	sys.Supernode.AwaitValidatedTimestamp(target)
	return target
}

// TestSupernodeLogBackfill_HappyPath exercises the end-to-end happy path of log
// backfill: the supernode is stopped, its logs DBs are wiped to simulate a
// cold restart with preserved upstream state, and then it is restarted with
// backfill enabled. Backfill must complete and verification must resume at
// timestamps that the supernode had previously verified.
func TestSupernodeLogBackfill_HappyPath(gt *testing.T) {
	t := devtest.ParallelT(gt)
	sys := newSupernodeInteropWithBackfill(t)

	// Phase 1: let the system run until backfill on the initial Start has
	// completed and the supernode has verified a recent timestamp.
	sys.Supernode.AwaitBackfillCompleted()
	t.Require().GreaterOrEqual(sys.Supernode.BackfillAttempts(), int32(1),
		"initial backfill should run at least once")
	firstAnchor := awaitRecentlyVerified(t, sys)
	t.Logger().Info("initial backfill completed and first anchor verified",
		"anchor", firstAnchor,
		"attempts", sys.Supernode.BackfillAttempts(),
	)

	// Phase 2: cold-restart the supernode with its logs DBs wiped. This
	// forces the interop activity to rebuild its initiating-message logs via
	// backfill before the main loop can resume verification.
	sys.Supernode.Stop()
	sys.Supernode.WipeLogsDBs()
	sys.Supernode.Start()

	// Phase 3: backfill must complete and the supernode must catch up to a
	// recent timestamp. We re-anchor on a fresh recent timestamp so we
	// tolerate any delay the restart introduces.
	sys.Supernode.AwaitBackfillCompleted()
	t.Require().GreaterOrEqual(sys.Supernode.BackfillAttempts(), int32(1),
		"post-restart backfill should run at least once")

	secondAnchor := awaitRecentlyVerified(t, sys)
	t.Require().GreaterOrEqualf(secondAnchor, firstAnchor,
		"post-restart verification should not regress (first=%d, second=%d)",
		firstAnchor, secondAnchor)

	t.Logger().Info("post-restart backfill completed",
		"first_anchor", firstAnchor,
		"second_anchor", secondAnchor,
		"attempts", sys.Supernode.BackfillAttempts(),
	)
}

// TestSupernodeLogBackfill_RetriesUntilNodesReady exercises the retry loop
// that wraps runLogBackfill. A test-only failure injector queues a fixed
// number of synthetic errors before the supernode Starts; the retry loop
// must consume those failures, back off, and eventually succeed without
// manual intervention. This models the production scenario where the
// virtual nodes are slow to come up and backfill initially fails.
func TestSupernodeLogBackfill_RetriesUntilNodesReady(gt *testing.T) {
	t := devtest.ParallelT(gt)
	sys := newSupernodeInteropWithBackfill(t)

	// Phase 1: let the initial Start settle and produce some history that
	// backfill can re-ingest after the wipe.
	sys.Supernode.AwaitBackfillCompleted()
	firstAnchor := awaitRecentlyVerified(t, sys)

	// Phase 2: cold-restart with a fixed number of synthetic backfill
	// failures queued before Start. The retry loop must consume them all
	// and then converge on its own.
	const injectedFailures = int32(2)
	sys.Supernode.Stop()
	sys.Supernode.WipeLogsDBs()
	sys.Supernode.InjectBackfillFailures(injectedFailures)
	sys.Supernode.Start()

	// Phase 3: observe that the retry loop engaged (attempts exceeded the
	// injected count) and that backfill eventually completed anyway.
	sys.Supernode.AwaitBackfillAttempts(injectedFailures + 1)
	sys.Supernode.AwaitBackfillCompleted()
	t.Require().GreaterOrEqualf(sys.Supernode.BackfillAttempts(), injectedFailures+1,
		"retry loop should have run at least %d attempts, got %d",
		injectedFailures+1, sys.Supernode.BackfillAttempts())

	// Phase 4: verification resumes at a fresh recent timestamp.
	secondAnchor := awaitRecentlyVerified(t, sys)
	t.Require().GreaterOrEqualf(secondAnchor, firstAnchor,
		"post-retry verification should not regress (first=%d, second=%d)",
		firstAnchor, secondAnchor)

	t.Logger().Info("retry-until-ready test passed",
		"injected_failures", injectedFailures,
		"attempts", sys.Supernode.BackfillAttempts(),
		"first_anchor", firstAnchor,
		"second_anchor", secondAnchor,
	)
}
