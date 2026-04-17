package stack

import "github.com/ethereum-optimism/optimism/op-service/apis"

type Supernode interface {
	Common
	QueryAPI() apis.SupernodeQueryAPI
}

// InteropTestControl provides integration test control methods for the interop activity.
// This interface is for integration test control only.
type InteropTestControl interface {
	// PauseInteropActivity pauses the interop activity at the given timestamp.
	// When the interop activity attempts to process this timestamp, it returns early.
	// This function is for integration test control only.
	PauseInteropActivity(ts uint64)

	// ResumeInteropActivity clears any pause on the interop activity, allowing normal processing.
	// This function is for integration test control only.
	ResumeInteropActivity()

	// Stop stops the underlying supernode process. Used to orchestrate restart scenarios
	// (e.g. wiping state between Stop and Start to force log backfill on the next boot).
	Stop()
	// Start starts the underlying supernode process after a prior Stop.
	Start()
	// WipeLogsDBs deletes on-disk logs DB files so the next Start must reconstruct
	// them via backfill from the virtual nodes.
	WipeLogsDBs() error

	// InteropBackfillAttempts returns the number of times the interop activity
	// has invoked runLogBackfill since its most recent Start.
	InteropBackfillAttempts() int32
	// InteropBackfillCompleted reports whether the interop activity has finished
	// its log backfill phase for the current Start.
	InteropBackfillCompleted() bool
	// InjectInteropBackfillFailures queues n synthetic backfill failures so the
	// retry loop backs off that many times before backfill can succeed. The
	// injection must be configured before the supernode Starts to take effect.
	InjectInteropBackfillFailures(n int32)
}
