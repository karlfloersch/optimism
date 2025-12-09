package sysgo

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/log"
	"github.com/stretchr/testify/require"

	"github.com/ethereum-optimism/optimism/op-devstack/devtest"
	"github.com/ethereum-optimism/optimism/op-devstack/shim"
	"github.com/ethereum-optimism/optimism/op-devstack/stack"
	"github.com/ethereum-optimism/optimism/op-devstack/stack/match"
	"github.com/ethereum-optimism/optimism/op-service/testlog"
	suptypes "github.com/ethereum-optimism/optimism/op-supervisor/supervisor/types"
)

// TestInteropFilter tests the interop filter sysgo integration.
// This test requires the full sysgo infrastructure to be working.
func TestInteropFilter(gt *testing.T) {
	var ids DefaultMinimalSystemWithInteropFilterIDs
	opt := DefaultMinimalSystemWithInteropFilter(&ids)

	logger := testlog.Logger(gt, log.LevelInfo)

	onFail, onSkipNow := exiters(gt)
	p := devtest.NewP(context.Background(), logger, onFail, onSkipNow)
	gt.Cleanup(p.Close)

	orch := NewOrchestrator(p, stack.Combine[*Orchestrator]())
	stack.ApplyOptionLifecycle(opt, orch)

	gt.Run("test interop filter startup", func(gt *testing.T) {
		gt.Parallel()

		t := devtest.SerialT(gt)
		system := shim.NewSystem(t)
		orch.Hydrate(system)

		// Verify interop filter is available
		filter := system.InteropFilter(match.FirstInteropFilter)
		require.NotNil(t, filter, "interop filter should be available")

		// Verify failsafe is initially disabled
		failsafe, err := filter.AdminAPI().GetFailsafeEnabled(t.Ctx())
		require.NoError(t, err)
		require.False(t, failsafe, "failsafe should be disabled initially")
	})

	gt.Run("test interop filter ready state", func(gt *testing.T) {
		gt.Parallel()

		t := devtest.SerialT(gt)
		system := shim.NewSystem(t)
		orch.Hydrate(system)

		filter := system.InteropFilter(match.FirstInteropFilter)
		require.NotNil(t, filter)

		// Wait for filter to become ready (backfill complete)
		// The filter has a 1 minute backfill duration in tests
		ctx, cancel := context.WithTimeout(t.Ctx(), 2*time.Minute)
		defer cancel()

		for {
			select {
			case <-ctx.Done():
				t.Errorf("timed out waiting for interop filter to become ready")
				t.FailNow()
			case <-time.After(time.Second):
				// Try a checkAccessList call - if it doesn't return ErrUninitialized, we're ready
				err := filter.QueryAPI().CheckAccessList(ctx, nil, suptypes.LocalUnsafe, suptypes.ExecutingDescriptor{})
				if err == nil || !errors.Is(err, suptypes.ErrUninitialized) {
					return // Filter is ready
				}
			}
		}
	})
}

// TestInteropFilterMatchers tests that interop filter matchers work correctly.
func TestInteropFilterMatchers(gt *testing.T) {
	// This is a simpler test that just verifies the matcher infrastructure
	// without requiring the full deployer
	require.NotNil(gt, match.FirstInteropFilter, "FirstInteropFilter matcher should exist")
}
