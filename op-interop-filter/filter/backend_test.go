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
	"github.com/ethereum-optimism/optimism/op-supervisor/supervisor/backend/db/logs"
	"github.com/ethereum-optimism/optimism/op-supervisor/supervisor/types"
)

func TestBackend_FailsafeEnabled(t *testing.T) {
	backend := newTestBackend(t)

	// Failsafe should be disabled by default
	require.False(t, backend.FailsafeEnabled())

	// Enable failsafe
	backend.SetFailsafeEnabled(true)
	require.True(t, backend.FailsafeEnabled())

	// Disable failsafe
	backend.SetFailsafeEnabled(false)
	require.False(t, backend.FailsafeEnabled())
}

func TestBackend_CheckAccessList_FailsafeEnabled(t *testing.T) {
	backend := newTestBackendWithMockChain(t)

	// Enable failsafe
	backend.SetFailsafeEnabled(true)

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

func TestBackend_OnReorg_EnablesFailsafe(t *testing.T) {
	backend := newTestBackend(t)

	// Failsafe should be disabled initially
	require.False(t, backend.FailsafeEnabled())

	// Simulate a reorg callback
	backend.onReorg(eth.ChainIDFromUInt64(1))

	// Failsafe should now be enabled
	require.True(t, backend.FailsafeEnabled())
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

	return &Backend{
		log:     log.New(),
		metrics: metrics.NoopMetrics,
		cfg: &Config{
			MessageExpiryWindow: uint64(DefaultMessageExpiryWindow.Seconds()),
			ValidationInterval:  500 * time.Millisecond,
			PollInterval:        2 * time.Second,
		},
		chains: make(map[eth.ChainID]*ChainIngester),
		ctx:    ctx,
		cancel: cancel,
	}
}

func newTestBackendWithMockChain(t *testing.T) *Backend {
	backend := newTestBackend(t)

	// Add a mock chain ingester that reports as ready
	chainID := eth.ChainIDFromUInt64(1)
	mockIngester := &mockChainIngester{ready: true}
	backend.chains[chainID] = mockIngester.asChainIngester()

	return backend
}

// mockChainIngester provides a minimal mock for testing
type mockChainIngester struct {
	ready  bool
	logsDB *logs.DB
}

// asChainIngester converts the mock into a real ChainIngester with only the ready field set
// This is a workaround since we can't easily mock the ChainIngester struct
func (m *mockChainIngester) asChainIngester() *ChainIngester {
	c := &ChainIngester{}
	if m.ready {
		c.ready.Store(true)
	}
	if m.logsDB != nil {
		c.logsDB = m.logsDB
	}
	return c
}

func TestValidateExecutingMessage_TimestampOrdering(t *testing.T) {
	backend := newTestBackendWithMockChain(t)
	chainID := eth.ChainIDFromUInt64(1)

	tests := []struct {
		name          string
		initTimestamp uint64
		execTimestamp uint64
		wantErr       bool
		errContains   string
	}{
		{
			name:          "init before exec - valid",
			initTimestamp: 100,
			execTimestamp: 200,
			wantErr:       false, // will fail on Contains, but timestamp check passes
		},
		{
			name:          "init equals exec - invalid",
			initTimestamp: 100,
			execTimestamp: 100,
			wantErr:       true,
			errContains:   "not before execution timestamp",
		},
		{
			name:          "init after exec - invalid",
			initTimestamp: 200,
			execTimestamp: 100,
			wantErr:       true,
			errContains:   "not before execution timestamp",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			execMsg := &types.ExecutingMessage{
				ChainID:   chainID,
				Timestamp: tc.initTimestamp,
			}

			err := backend.validateExecutingMessage(execMsg, tc.execTimestamp)

			if tc.wantErr {
				require.Error(t, err)
				require.ErrorIs(t, err, types.ErrConflict)
				require.Contains(t, err.Error(), tc.errContains)
			} else {
				// If timestamp check passes, it will fail on Contains (no logsDB)
				// That's expected - we're only testing timestamp logic here
				if err != nil {
					require.NotContains(t, err.Error(), "not before execution timestamp")
				}
			}
		})
	}
}

func TestValidateExecutingMessage_Expiry(t *testing.T) {
	backend := newTestBackendWithMockChain(t)
	backend.cfg.MessageExpiryWindow = 100 // 100 seconds for easy math
	chainID := eth.ChainIDFromUInt64(1)

	tests := []struct {
		name          string
		initTimestamp uint64
		execTimestamp uint64
		wantErr       bool
		errContains   string
	}{
		{
			name:          "within expiry window",
			initTimestamp: 100,
			execTimestamp: 150, // expires at 200, exec at 150 - OK
			wantErr:       false,
		},
		{
			name:          "at expiry boundary",
			initTimestamp: 100,
			execTimestamp: 200, // expires at 200, exec at 200 - OK (>=)
			wantErr:       false,
		},
		{
			name:          "just past expiry",
			initTimestamp: 100,
			execTimestamp: 201, // expires at 200, exec at 201 - expired
			wantErr:       true,
			errContains:   "expired",
		},
		{
			name:          "well past expiry",
			initTimestamp: 100,
			execTimestamp: 500, // expires at 200, exec at 500 - expired
			wantErr:       true,
			errContains:   "expired",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			execMsg := &types.ExecutingMessage{
				ChainID:   chainID,
				Timestamp: tc.initTimestamp,
			}

			err := backend.validateExecutingMessage(execMsg, tc.execTimestamp)

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

	err := backend.validateExecutingMessage(execMsg, 200)
	require.Error(t, err)
	require.ErrorIs(t, err, types.ErrUnknownChain)
}

func TestCheckAccessListEntry_TimeoutExpiry(t *testing.T) {
	backend := newTestBackendWithMockChain(t)
	backend.cfg.MessageExpiryWindow = 100
	chainID := eth.ChainIDFromUInt64(1)

	tests := []struct {
		name          string
		initTimestamp uint64
		execTimestamp uint64
		timeout       uint64
		wantErr       bool
		errContains   string
	}{
		{
			name:          "no timeout - valid",
			initTimestamp: 100,
			execTimestamp: 150,
			timeout:       0, // no timeout
			wantErr:       false,
		},
		{
			name:          "timeout within expiry",
			initTimestamp: 100,
			execTimestamp: 120,
			timeout:       30, // exec + timeout = 150, expires at 200 - OK
			wantErr:       false,
		},
		{
			name:          "timeout at expiry boundary",
			initTimestamp: 100,
			execTimestamp: 150,
			timeout:       50, // exec + timeout = 200, expires at 200 - OK (>=)
			wantErr:       false,
		},
		{
			name:          "timeout past expiry",
			initTimestamp: 100,
			execTimestamp: 150,
			timeout:       51, // exec + timeout = 201, expires at 200 - fail
			wantErr:       true,
			errContains:   "expire before timeout",
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
				Timestamp: tc.execTimestamp,
				Timeout:   tc.timeout,
			}

			err := backend.checkAccessListEntry(context.Background(), access, types.LocalUnsafe, execDescriptor)

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

func TestCheckAccessListEntry_CrossUnsafeTimestamp(t *testing.T) {
	backend := newTestBackendWithMockChain(t)
	backend.cfg.MessageExpiryWindow = 1000
	chainID := eth.ChainIDFromUInt64(1)

	// Set cross-unsafe timestamp to 500
	backend.crossUnsafeTimestamp.Store(500)

	tests := []struct {
		name          string
		initTimestamp uint64
		safetyLevel   types.SafetyLevel
		wantErr       bool
		errContains   string
	}{
		{
			name:          "LocalUnsafe ignores cross-unsafe timestamp",
			initTimestamp: 600, // past cross-unsafe, but LocalUnsafe doesn't check
			safetyLevel:   types.LocalUnsafe,
			wantErr:       false,
		},
		{
			name:          "CrossUnsafe at boundary",
			initTimestamp: 500, // equals cross-unsafe timestamp - OK
			safetyLevel:   types.CrossUnsafe,
			wantErr:       false,
		},
		{
			name:          "CrossUnsafe before boundary",
			initTimestamp: 400, // before cross-unsafe timestamp - OK
			safetyLevel:   types.CrossUnsafe,
			wantErr:       false,
		},
		{
			name:          "CrossUnsafe past boundary",
			initTimestamp: 501, // past cross-unsafe timestamp - fail
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

			err := backend.checkAccessListEntry(context.Background(), access, tc.safetyLevel, execDescriptor)

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
