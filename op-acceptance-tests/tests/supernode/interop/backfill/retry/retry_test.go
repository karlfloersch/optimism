// Package retry contains the acceptance test for the log-backfill retry
// loop. It lives in its own package so it runs in its own test binary,
// isolated from the happy-path test.
package retry

import (
	"testing"

	"github.com/ethereum-optimism/optimism/op-acceptance-tests/tests/supernode/interop/backfill/backfillutil"
	"github.com/ethereum-optimism/optimism/op-devstack/devtest"
)

// TestSupernodeLogBackfill_RetriesUntilSuccess exercises the retry loop
// wrapping runLogBackfill.
//
//  1. Bring up a two-L2 supernode with interop enabled at genesis.
//  2. Let both chains accumulate enough history for backfill to have real
//     work to do.
//  3. Hot-restart the interop activity with its logs DBs wiped AND a fixed
//     number of synthetic backfill failures queued. The retry loop must
//     consume every injected failure and then converge on its own.
//  4. Assert that BackfillAttempts advanced past the injection count and
//     that the final DB coverage is the same as the happy path.
func TestSupernodeLogBackfill_RetriesUntilSuccess(gt *testing.T) {
	t := devtest.SerialT(gt)
	sys := backfillutil.NewSystem(t)

	sys.Supernode.AwaitBackfillCompleted()
	backfillutil.AwaitHistoryAtLeast(t, sys, backfillutil.MinHistoryBeforeRestart)

	// Atomically: stop the old activity, wipe logs DBs, queue N synthetic
	// failures on the replacement, then launch it. This guarantees the
	// first runLogBackfill call observes the injection (no scheduling race
	// between InjectBackfillFailures and the activity's goroutine starting).
	const injectedFailures = int32(2)
	sys.Supernode.RestartInterop(true, injectedFailures)

	sys.Supernode.AwaitBackfillAttempts(injectedFailures + 1)
	sys.Supernode.AwaitBackfillCompleted()

	t.Require().GreaterOrEqualf(sys.Supernode.BackfillAttempts(), injectedFailures+1,
		"retry loop should have run at least %d attempts, got %d",
		injectedFailures+1, sys.Supernode.BackfillAttempts())

	backfillutil.AssertBackfillCovered(t, sys)
}
