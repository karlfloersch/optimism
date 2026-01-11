package filter

import (
	"context"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/log"
	"github.com/stretchr/testify/require"

	"github.com/ethereum-optimism/optimism/op-interop-filter/metrics"
	"github.com/ethereum-optimism/optimism/op-service/eth"
	"github.com/ethereum-optimism/optimism/op-supervisor/supervisor/types"
)

func TestBackend_FailsafeEnabled(t *testing.T) {
	backend := newTestBackendWithMockChain(t)
	chainID := eth.ChainIDFromUInt64(1)

	// Failsafe should be disabled by default
	require.False(t, backend.FailsafeEnabled())

	// Manual failsafe override
	backend.SetFailsafeEnabled(true)
	require.True(t, backend.FailsafeEnabled())

	// Disable manual failsafe
	backend.SetFailsafeEnabled(false)
	require.False(t, backend.FailsafeEnabled())

	// Chain error also enables failsafe
	backend.chains[chainID].SetError(ErrorReorg, "test error")
	require.True(t, backend.FailsafeEnabled())

	// SetFailsafeEnabled(false) does NOT clear chain errors
	backend.SetFailsafeEnabled(false)
	require.True(t, backend.FailsafeEnabled()) // still true due to chain error

	// Clear chain error directly
	backend.chains[chainID].ClearError()
	require.False(t, backend.FailsafeEnabled())
}

func TestBackend_CheckAccessList_FailsafeEnabled(t *testing.T) {
	backend := newTestBackendWithMockChain(t)
	chainID := eth.ChainIDFromUInt64(1)

	// Enable failsafe by setting a chain error
	backend.chains[chainID].SetError(ErrorReorg, "test reorg")

	// CheckAccessList should return ErrFailsafeEnabled
	err := backend.CheckAccessList(
		context.Background(),
		[]common.Hash{},
		types.LocalUnsafe,
		types.ExecutingDescriptor{},
	)
	require.ErrorIs(t, err, types.ErrFailsafeEnabled)
}

func TestBackend_CheckAccessList_NotReady(t *testing.T) {
	backend := newTestBackend(t)

	// Backend with no chains is not ready
	err := backend.CheckAccessList(
		context.Background(),
		[]common.Hash{},
		types.LocalUnsafe,
		types.ExecutingDescriptor{},
	)
	require.ErrorIs(t, err, types.ErrUninitialized)
}

func TestBackend_Ready_NoChains(t *testing.T) {
	backend := newTestBackend(t)

	// Backend with no chains should not be ready
	require.False(t, backend.Ready())
}

func TestBackend_Ready_WithChains(t *testing.T) {
	backend := newTestBackendWithMockChain(t)

	// Backend with mock ready chain should be ready
	require.True(t, backend.Ready())
}

func TestBackend_ChainError_EnablesFailsafe(t *testing.T) {
	backend := newTestBackendWithMockChain(t)
	chainID := eth.ChainIDFromUInt64(1)

	// Failsafe should be disabled initially (no chain errors)
	require.False(t, backend.FailsafeEnabled())

	// Set an error on the chain ingester
	backend.chains[chainID].SetError(ErrorReorg, "test reorg")

	// Failsafe should now be enabled (derived from chain error state)
	require.True(t, backend.FailsafeEnabled())

	// Clear the error
	backend.chains[chainID].ClearError()

	// Failsafe should be disabled again
	require.False(t, backend.FailsafeEnabled())
}

func TestBackend_CheckAccessList_UnsupportedSafetyLevel(t *testing.T) {
	backend := newTestBackendWithMockChain(t)

	// Finalized is not supported (we don't track derivation)
	err := backend.CheckAccessList(
		context.Background(),
		[]common.Hash{},
		types.Finalized,
		types.ExecutingDescriptor{},
	)
	require.Error(t, err)
	require.Contains(t, err.Error(), "unsupported safety level")
}

// Test helpers

func newTestBackend(t *testing.T) *Backend {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	cfg := &Config{
		MessageExpiryWindow: uint64(DefaultMessageExpiryWindow.Seconds()),
		ValidationInterval:  500 * time.Millisecond,
		PollInterval:        2 * time.Second,
	}
	chains := make(map[eth.ChainID]ChainIngester)

	// Create simple cross-validator
	validator := NewSimpleCrossValidator(chains, cfg.MessageExpiryWindow)

	return NewBackend(ctx, BackendParams{
		Logger:                  log.New(),
		Metrics:                 metrics.NoopMetrics,
		Config:                  cfg,
		Chains:                  chains,
		ChainLifecycle:          nil,
		CrossValidator:          validator,
		CrossValidatorLifecycle: &noopLifecycle{},
	})
}

func newTestBackendWithMockChain(t *testing.T) *Backend {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	cfg := &Config{
		MessageExpiryWindow: uint64(DefaultMessageExpiryWindow.Seconds()),
		ValidationInterval:  500 * time.Millisecond,
		PollInterval:        2 * time.Second,
	}

	// Add a memory chain ingester that reports as ready
	chainID := eth.ChainIDFromUInt64(1)
	ingester := NewMemoryChainIngester()
	chains := map[eth.ChainID]ChainIngester{
		chainID: ingester,
	}

	// Create simple cross-validator
	validator := NewSimpleCrossValidator(chains, cfg.MessageExpiryWindow)

	return NewBackend(ctx, BackendParams{
		Logger:                  log.New(),
		Metrics:                 metrics.NoopMetrics,
		Config:                  cfg,
		Chains:                  chains,
		ChainLifecycle:          nil,
		CrossValidator:          validator,
		CrossValidatorLifecycle: &noopLifecycle{},
	})
}

// noopLifecycle implements Startable and Stoppable for testing
type noopLifecycle struct{}

func (n *noopLifecycle) Start() error { return nil }
func (n *noopLifecycle) Stop() error  { return nil }

func TestSimpleCrossValidator_TimestampOrdering(t *testing.T) {
	chainID := eth.ChainIDFromUInt64(1)
	ingester := NewMemoryChainIngester()
	chains := map[eth.ChainID]ChainIngester{chainID: ingester}
	validator := NewSimpleCrossValidator(chains, 100)

	tests := []struct {
		name                      string
		initTimestamp             uint64
		execMsgInclusionTimestamp uint64
		wantErr                   bool
		errContains               string
	}{
		{
			name:                      "init before exec - valid",
			initTimestamp:             100,
			execMsgInclusionTimestamp: 200,
			wantErr:                   false, // will fail on Contains, but timestamp check passes
		},
		{
			name:                      "init equals exec - invalid",
			initTimestamp:             100,
			execMsgInclusionTimestamp: 100,
			wantErr:                   true,
			errContains:               "not before inclusion timestamp",
		},
		{
			name:                      "init after exec - invalid",
			initTimestamp:             200,
			execMsgInclusionTimestamp: 100,
			wantErr:                   true,
			errContains:               "not before inclusion timestamp",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			access := types.Access{
				ChainID:   chainID,
				Timestamp: tc.initTimestamp,
			}
			execDescriptor := types.ExecutingDescriptor{
				ChainID:   chainID,
				Timestamp: tc.execMsgInclusionTimestamp,
			}

			err := validator.ValidateAccessEntry(access, types.LocalUnsafe, execDescriptor)

			if tc.wantErr {
				require.Error(t, err)
				require.ErrorIs(t, err, types.ErrConflict)
				require.Contains(t, err.Error(), tc.errContains)
			} else {
				// If timestamp check passes, it will fail on Contains (no logs in memory)
				if err != nil {
					require.NotContains(t, err.Error(), "not before inclusion timestamp")
				}
			}
		})
	}
}

func TestSimpleCrossValidator_Expiry(t *testing.T) {
	chainID := eth.ChainIDFromUInt64(1)
	ingester := NewMemoryChainIngester()
	chains := map[eth.ChainID]ChainIngester{chainID: ingester}
	validator := NewSimpleCrossValidator(chains, 100) // 100 seconds expiry

	tests := []struct {
		name                      string
		initTimestamp             uint64
		execMsgInclusionTimestamp uint64
		wantErr                   bool
		errContains               string
	}{
		{
			name:                      "within expiry window",
			initTimestamp:             100,
			execMsgInclusionTimestamp: 150, // expires at 200, exec at 150 - OK
			wantErr:                   false,
		},
		{
			name:                      "at expiry boundary",
			initTimestamp:             100,
			execMsgInclusionTimestamp: 200, // expires at 200, exec at 200 - OK (>=)
			wantErr:                   false,
		},
		{
			name:                      "just past expiry",
			initTimestamp:             100,
			execMsgInclusionTimestamp: 201, // expires at 200, exec at 201 - expired
			wantErr:                   true,
			errContains:               "expired",
		},
		{
			name:                      "well past expiry",
			initTimestamp:             100,
			execMsgInclusionTimestamp: 500, // expires at 200, exec at 500 - expired
			wantErr:                   true,
			errContains:               "expired",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			access := types.Access{
				ChainID:   chainID,
				Timestamp: tc.initTimestamp,
			}
			execDescriptor := types.ExecutingDescriptor{
				ChainID:   chainID,
				Timestamp: tc.execMsgInclusionTimestamp,
			}

			err := validator.ValidateAccessEntry(access, types.LocalUnsafe, execDescriptor)

			if tc.wantErr {
				require.Error(t, err)
				require.ErrorIs(t, err, types.ErrConflict)
				require.Contains(t, err.Error(), tc.errContains)
			} else {
				// If expiry check passes, it will fail on Contains (no logs)
				if err != nil {
					require.NotContains(t, err.Error(), "expired")
				}
			}
		})
	}
}

func TestSimpleCrossValidator_UnknownChain(t *testing.T) {
	chainID := eth.ChainIDFromUInt64(1)
	unknownChainID := eth.ChainIDFromUInt64(999)
	ingester := NewMemoryChainIngester()
	chains := map[eth.ChainID]ChainIngester{chainID: ingester}
	validator := NewSimpleCrossValidator(chains, 100)

	access := types.Access{
		ChainID:   unknownChainID,
		Timestamp: 100,
	}
	execDescriptor := types.ExecutingDescriptor{
		ChainID:   chainID,
		Timestamp: 200,
	}

	err := validator.ValidateAccessEntry(access, types.LocalUnsafe, execDescriptor)
	require.Error(t, err)
	require.ErrorIs(t, err, types.ErrUnknownChain)
}

func TestSimpleCrossValidator_TimeoutExpiry(t *testing.T) {
	chainID := eth.ChainIDFromUInt64(1)
	ingester := NewMemoryChainIngester()
	chains := map[eth.ChainID]ChainIngester{chainID: ingester}
	validator := NewSimpleCrossValidator(chains, 100) // 100 seconds expiry

	tests := []struct {
		name                      string
		initTimestamp             uint64
		execMsgInclusionTimestamp uint64
		timeout                   uint64
		wantErr                   bool
		errContains               string
	}{
		{
			name:                      "no timeout - valid",
			initTimestamp:             100,
			execMsgInclusionTimestamp: 150,
			timeout:                   0, // no timeout
			wantErr:                   false,
		},
		{
			name:                      "timeout within expiry",
			initTimestamp:             100,
			execMsgInclusionTimestamp: 120,
			timeout:                   30, // exec + timeout = 150, expires at 200 - OK
			wantErr:                   false,
		},
		{
			name:                      "timeout at expiry boundary",
			initTimestamp:             100,
			execMsgInclusionTimestamp: 150,
			timeout:                   50, // exec + timeout = 200, expires at 200 - OK (>=)
			wantErr:                   false,
		},
		{
			name:                      "timeout past expiry",
			initTimestamp:             100,
			execMsgInclusionTimestamp: 150,
			timeout:                   51, // exec + timeout = 201, expires at 200 - fail
			wantErr:                   true,
			errContains:               "expire before timeout",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			access := types.Access{
				ChainID:   chainID,
				Timestamp: tc.initTimestamp,
			}
			execDescriptor := types.ExecutingDescriptor{
				ChainID:   chainID,
				Timestamp: tc.execMsgInclusionTimestamp,
				Timeout:   tc.timeout,
			}

			err := validator.ValidateAccessEntry(access, types.LocalUnsafe, execDescriptor)

			if tc.wantErr {
				require.Error(t, err)
				require.ErrorIs(t, err, types.ErrConflict)
				require.Contains(t, err.Error(), tc.errContains)
			} else {
				// If timeout check passes, it may fail later on other checks
				if err != nil {
					require.NotContains(t, err.Error(), "expire before timeout")
				}
			}
		})
	}
}

func TestSimpleCrossValidator_CrossUnsafeTimestamp(t *testing.T) {
	chainID := eth.ChainIDFromUInt64(1)
	ingester := NewMemoryChainIngester()
	chains := map[eth.ChainID]ChainIngester{chainID: ingester}
	validator := NewSimpleCrossValidator(chains, 1000)

	// Set cross-validated timestamp to 500
	validator.SetCrossValidatedTimestamp(500)

	tests := []struct {
		name          string
		initTimestamp uint64
		safetyLevel   types.SafetyLevel
		wantErr       bool
		errContains   string
	}{
		{
			name:          "LocalUnsafe ignores cross-unsafe timestamp",
			initTimestamp: 600, // past cross-validated, but LocalUnsafe doesn't check
			safetyLevel:   types.LocalUnsafe,
			wantErr:       false,
		},
		{
			name:          "CrossUnsafe at boundary",
			initTimestamp: 500, // equals cross-validated timestamp - OK
			safetyLevel:   types.CrossUnsafe,
			wantErr:       false,
		},
		{
			name:          "CrossUnsafe before boundary",
			initTimestamp: 400, // before cross-validated timestamp - OK
			safetyLevel:   types.CrossUnsafe,
			wantErr:       false,
		},
		{
			name:          "CrossUnsafe past boundary",
			initTimestamp: 501, // past cross-validated timestamp - fail
			safetyLevel:   types.CrossUnsafe,
			wantErr:       true,
			errContains:   "not yet cross-unsafe validated",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			access := types.Access{
				ChainID:   chainID,
				Timestamp: tc.initTimestamp,
			}
			execDescriptor := types.ExecutingDescriptor{
				ChainID:   chainID,
				Timestamp: tc.initTimestamp + 100, // always valid exec timestamp
			}

			err := validator.ValidateAccessEntry(access, tc.safetyLevel, execDescriptor)

			if tc.wantErr {
				require.Error(t, err)
				require.ErrorIs(t, err, types.ErrOutOfScope)
				require.Contains(t, err.Error(), tc.errContains)
			} else {
				// If cross-unsafe check passes, it may fail later on other checks
				if err != nil {
					require.NotContains(t, err.Error(), "not yet cross-unsafe validated")
				}
			}
		})
	}
}
