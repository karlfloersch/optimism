package stack

import (
	"github.com/ethereum-optimism/optimism/op-service/apis"
	"github.com/ethereum-optimism/optimism/op-service/eth"
	suptypes "github.com/ethereum-optimism/optimism/op-supervisor/supervisor/types"
)

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

	// RestartInteropActivity stops the running interop activity, optionally
	// wipes its on-disk logs DBs, and launches a fresh instance against the
	// still-running supernode (HTTP server, chain containers, and all other
	// activities remain up). preInjectBackfillFailures, if positive, is
	// applied to the fresh activity before its goroutine starts, so the very
	// first runLogBackfill attempt sees the injection. Used to exercise log
	// backfill against a ready cluster without restarting the whole supernode
	// process.
	RestartInteropActivity(wipeLogsDBs bool, preInjectBackfillFailures int32) error

	// InteropBackfillAttempts returns the number of times the interop activity
	// has invoked runLogBackfill since its most recent (re)start.
	InteropBackfillAttempts() int32
	// InteropBackfillCompleted reports whether the interop activity has finished
	// its log backfill phase for the current (re)start.
	InteropBackfillCompleted() bool

	// InteropActivationTimestamp returns the immutable protocol activation
	// timestamp of the interop activity.
	InteropActivationTimestamp() uint64
	// InteropRuntimeActivationTimestamp returns the (possibly-advanced-by-backfill)
	// runtime activation timestamp of the interop activity.
	InteropRuntimeActivationTimestamp() uint64
	// InteropFirstSealedBlock returns the earliest block sealed in the interop
	// logs DB for the given chain. Fails if the chain is unknown or the DB is
	// empty.
	InteropFirstSealedBlock(chainID eth.ChainID) (suptypes.BlockSeal, error)
	// InteropLatestSealedBlock returns the most recent block sealed in the
	// interop logs DB for the given chain. Second return value is false if
	// the DB is empty.
	InteropLatestSealedBlock(chainID eth.ChainID) (suptypes.BlockSeal, bool, error)
}
