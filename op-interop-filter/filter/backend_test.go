package filter

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/log"
	"github.com/stretchr/testify/require"

	"github.com/ethereum-optimism/optimism/op-interop-filter/metrics"
	"github.com/ethereum-optimism/optimism/op-service/eth"
	"github.com/ethereum-optimism/optimism/op-supervisor/supervisor/backend/db/logs"
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
	backend.chains[chainID].setError(ErrorReorg, "test error")
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
	backend.chains[chainID].setError(ErrorReorg, "test reorg")

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
	backend.chains[chainID].setError(ErrorReorg, "test reorg")

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
	chains := make(map[eth.ChainID]*ChainIngester)

	b := &Backend{
		log:     log.New(),
		metrics: metrics.NoopMetrics,
		cfg:     cfg,
		chains:  chains,
		ctx:     ctx,
		cancel:  cancel,
	}

	// Create cross-message validator (with empty chains initially)
	b.crossMessageValidator = NewCrossMessageValidator(ctx, log.New(), metrics.NoopMetrics, cfg, chains)

	return b
}

func newTestBackendWithMockChain(t *testing.T) *Backend {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	cfg := &Config{
		MessageExpiryWindow: uint64(DefaultMessageExpiryWindow.Seconds()),
		ValidationInterval:  500 * time.Millisecond,
		PollInterval:        2 * time.Second,
	}

	// Add a mock chain ingester that reports as ready
	chainID := eth.ChainIDFromUInt64(1)
	mockIngester := &mockChainIngester{ready: true}
	chains := map[eth.ChainID]*ChainIngester{
		chainID: mockIngester.asChainIngester(),
	}

	b := &Backend{
		log:     log.New(),
		metrics: metrics.NoopMetrics,
		cfg:     cfg,
		chains:  chains,
		ctx:     ctx,
		cancel:  cancel,
	}

	// Create cross-message validator with the mock chain
	b.crossMessageValidator = NewCrossMessageValidator(ctx, log.New(), metrics.NoopMetrics, cfg, chains)

	return b
}

// mockChainIngester provides a minimal mock for testing
type mockChainIngester struct {
	ready           bool
	logsDB          *logs.DB
	latestTimestamp uint64
}

// asChainIngester converts the mock into a real ChainIngester with only the ready field set
// This is a workaround since we can't easily mock the ChainIngester struct
func (m *mockChainIngester) asChainIngester() *ChainIngester {
	c := &ChainIngester{
		log:     log.New(),           // Required for setError() logging
		metrics: metrics.NoopMetrics, // Required for setError() metrics
	}
	if m.ready {
		c.ready.Store(true)
	}
	if m.logsDB != nil {
		c.logsDB = m.logsDB
	}
	if m.latestTimestamp > 0 {
		c.testLatestTimestamp.Store(m.latestTimestamp)
	}
	return c
}

func TestValidateExecutingMessage_TimestampOrdering(t *testing.T) {
	backend := newTestBackendWithMockChain(t)
	chainID := eth.ChainIDFromUInt64(1)

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
			execMsg := &types.ExecutingMessage{
				ChainID:   chainID,
				Timestamp: tc.initTimestamp,
			}

			err := backend.crossMessageValidator.ValidateExecutingMessage(execMsg, tc.execMsgInclusionTimestamp)

			if tc.wantErr {
				require.Error(t, err)
				require.ErrorIs(t, err, types.ErrConflict)
				require.Contains(t, err.Error(), tc.errContains)
			} else {
				// If timestamp check passes, it will fail on Contains (no logsDB)
				// That's expected - we're only testing timestamp logic here
				if err != nil {
					require.NotContains(t, err.Error(), "not before inclusion timestamp")
				}
			}
		})
	}
}

func TestValidateExecutingMessage_Expiry(t *testing.T) {
	backend := newTestBackendWithMockChain(t)
	backend.crossMessageValidator.cfg.MessageExpiryWindow = 100 // 100 seconds for easy math
	chainID := eth.ChainIDFromUInt64(1)

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
			execMsg := &types.ExecutingMessage{
				ChainID:   chainID,
				Timestamp: tc.initTimestamp,
			}

			err := backend.crossMessageValidator.ValidateExecutingMessage(execMsg, tc.execMsgInclusionTimestamp)

			if tc.wantErr {
				require.Error(t, err)
				require.ErrorIs(t, err, types.ErrConflict)
				require.Contains(t, err.Error(), tc.errContains)
			} else {
				// If expiry check passes, it will fail on Contains (no logsDB)
				if err != nil {
					require.NotContains(t, err.Error(), "expired")
				}
			}
		})
	}
}

func TestValidateExecutingMessage_UnknownChain(t *testing.T) {
	backend := newTestBackendWithMockChain(t) // has chain 1
	unknownChainID := eth.ChainIDFromUInt64(999)

	execMsg := &types.ExecutingMessage{
		ChainID:   unknownChainID,
		Timestamp: 100,
	}

	err := backend.crossMessageValidator.ValidateExecutingMessage(execMsg, 200)
	require.Error(t, err)
	require.ErrorIs(t, err, types.ErrUnknownChain)
}

func TestValidateAccessEntry_TimeoutExpiry(t *testing.T) {
	backend := newTestBackendWithMockChain(t)
	backend.crossMessageValidator.cfg.MessageExpiryWindow = 100
	chainID := eth.ChainIDFromUInt64(1)

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

			err := backend.crossMessageValidator.ValidateAccessEntry(access,types.LocalUnsafe, execDescriptor)

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

func TestValidateAccessEntry_CrossUnsafeTimestamp(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	cfg := &Config{
		MessageExpiryWindow: 1000,
		ValidationInterval:  500 * time.Millisecond,
		PollInterval:        2 * time.Second,
	}
	chainID := eth.ChainIDFromUInt64(1)

	// Create a mock chain with latestTimestamp of 500
	mockIngester := &mockChainIngester{ready: true, latestTimestamp: 500}
	chains := map[eth.ChainID]*ChainIngester{
		chainID: mockIngester.asChainIngester(),
	}

	backend := &Backend{
		log:     log.New(),
		metrics: metrics.NoopMetrics,
		cfg:     cfg,
		chains:  chains,
		ctx:     ctx,
		cancel:  cancel,
	}

	// Create cross-message validator with the mock chain AND set a cross-validated timestamp
	backend.crossMessageValidator = NewCrossMessageValidator(ctx, log.New(), metrics.NoopMetrics, cfg, chains)
	// Manually set the cross-validated timestamp to match latestTimestamp for testing
	tsPtr, _ := backend.crossMessageValidator.crossValidatedTs.Load(chainID)
	tsPtr.(*atomic.Uint64).Store(500)
	backend.crossMessageValidator.globalCrossValidatedTs.Store(500)

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

			err := backend.crossMessageValidator.ValidateAccessEntry(access,tc.safetyLevel, execDescriptor)

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
