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
	validator := NewMockCrossValidator(chains, cfg.MessageExpiryWindow)

	return NewBackend(ctx, BackendParams{
		Logger:         log.New(),
		Metrics:        metrics.NoopMetrics,
		Chains:         chains,
		CrossValidator: validator,
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

	// Add a mock chain ingester that reports as ready
	chainID := eth.ChainIDFromUInt64(1)
	ingester := NewMockChainIngester()
	chains := map[eth.ChainID]ChainIngester{
		chainID: ingester,
	}

	// Create simple cross-validator
	validator := NewMockCrossValidator(chains, cfg.MessageExpiryWindow)

	return NewBackend(ctx, BackendParams{
		Logger:         log.New(),
		Metrics:        metrics.NoopMetrics,
		Chains:         chains,
		CrossValidator: validator,
	})
}

func TestMockCrossValidator_TimestampOrdering(t *testing.T) {
	chainID := eth.ChainIDFromUInt64(1)
	ingester := NewMockChainIngester()
	chains := map[eth.ChainID]ChainIngester{chainID: ingester}
	validator := NewMockCrossValidator(chains, 100)

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

func TestMockCrossValidator_Expiry(t *testing.T) {
	chainID := eth.ChainIDFromUInt64(1)
	ingester := NewMockChainIngester()
	chains := map[eth.ChainID]ChainIngester{chainID: ingester}
	validator := NewMockCrossValidator(chains, 100) // 100 seconds expiry

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

func TestMockCrossValidator_UnknownChain(t *testing.T) {
	chainID := eth.ChainIDFromUInt64(1)
	unknownChainID := eth.ChainIDFromUInt64(999)
	ingester := NewMockChainIngester()
	chains := map[eth.ChainID]ChainIngester{chainID: ingester}
	validator := NewMockCrossValidator(chains, 100)

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

func TestMockCrossValidator_TimeoutExpiry(t *testing.T) {
	chainID := eth.ChainIDFromUInt64(1)
	ingester := NewMockChainIngester()
	chains := map[eth.ChainID]ChainIngester{chainID: ingester}
	validator := NewMockCrossValidator(chains, 100) // 100 seconds expiry

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

// ValidatorFactory creates a CrossValidator with a specific cross-validated timestamp.
type ValidatorFactory func(chains map[eth.ChainID]ChainIngester, expiryWindow uint64, crossValidatedTs uint64) CrossValidator

// validatorFactories contains factories for all CrossValidator implementations.
var validatorFactories = map[string]ValidatorFactory{
	"MockCrossValidator": func(chains map[eth.ChainID]ChainIngester, expiry uint64, crossTs uint64) CrossValidator {
		v := NewMockCrossValidator(chains, expiry)
		v.SetCrossValidatedTimestamp(crossTs)
		return v
	},
	"LockstepCrossValidator": func(chains map[eth.ChainID]ChainIngester, expiry uint64, crossTs uint64) CrossValidator {
		return NewLockstepCrossValidator(
			context.Background(),
			log.New(),
			metrics.NoopMetrics,
			expiry,
			time.Hour, // won't tick in test
			chains,
			crossTs, // startTimestamp = cross-validated timestamp
		)
	},
}

// TestBackend_CheckAccessList_Portable tests CheckAccessList behavior through the public API.
// These tests run against all CrossValidator implementations to ensure consistent behavior.
func TestBackend_CheckAccessList_Portable(t *testing.T) {
	for validatorName, createValidator := range validatorFactories {
		t.Run(validatorName, func(t *testing.T) {
			runPortableValidationTests(t, createValidator)
		})
	}
}

func runPortableValidationTests(t *testing.T, createValidator ValidatorFactory) {
	chainID := eth.ChainIDFromUInt64(1)
	const expiryWindow = uint64(1000)

	// Valid checksum must start with PrefixChecksum (0x03)
	validChecksum := func(suffix byte) types.MessageChecksum {
		cs := types.MessageChecksum{}
		cs[0] = 3 // PrefixChecksum
		cs[1] = suffix
		return cs
	}

	// Helper to create a backend with given components
	createBackend := func(ingester ChainIngester, validator CrossValidator) *Backend {
		chains := map[eth.ChainID]ChainIngester{chainID: ingester}
		return NewBackend(context.Background(), BackendParams{
			Logger:         log.New(),
			Metrics:        metrics.NoopMetrics,
			Chains:         chains,
			CrossValidator: validator,
		})
	}

	// Helper to build access list entries from an Access
	buildAccessList := func(access types.Access) []common.Hash {
		return types.EncodeAccessList([]types.Access{access})
	}

	t.Run("rejects_when_failsafe_enabled", func(t *testing.T) {
		ingester := NewMockChainIngester()
		chains := map[eth.ChainID]ChainIngester{chainID: ingester}
		validator := createValidator(chains, expiryWindow, 500)
		backend := createBackend(ingester, validator)

		backend.SetFailsafeEnabled(true)

		access := types.Access{ChainID: chainID, Timestamp: 100, BlockNumber: 10, LogIndex: 0, Checksum: validChecksum(1)}
		execDesc := types.ExecutingDescriptor{ChainID: chainID, Timestamp: 200}

		err := backend.CheckAccessList(context.Background(), buildAccessList(access), types.LocalUnsafe, execDesc)
		require.ErrorIs(t, err, types.ErrFailsafeEnabled)
	})

	t.Run("rejects_when_not_ready", func(t *testing.T) {
		ingester := NewMockChainIngester()
		ingester.SetReady(false)
		chains := map[eth.ChainID]ChainIngester{chainID: ingester}
		validator := createValidator(chains, expiryWindow, 500)
		backend := createBackend(ingester, validator)

		access := types.Access{ChainID: chainID, Timestamp: 100, BlockNumber: 10, LogIndex: 0, Checksum: validChecksum(1)}
		execDesc := types.ExecutingDescriptor{ChainID: chainID, Timestamp: 200}

		err := backend.CheckAccessList(context.Background(), buildAccessList(access), types.LocalUnsafe, execDesc)
		require.ErrorIs(t, err, types.ErrUninitialized)
	})

	t.Run("rejects_unknown_executing_chain", func(t *testing.T) {
		ingester := NewMockChainIngester()
		chains := map[eth.ChainID]ChainIngester{chainID: ingester}
		validator := createValidator(chains, expiryWindow, 500)
		backend := createBackend(ingester, validator)

		unknownChain := eth.ChainIDFromUInt64(999)
		access := types.Access{ChainID: chainID, Timestamp: 100, BlockNumber: 10, LogIndex: 0, Checksum: validChecksum(1)}
		execDesc := types.ExecutingDescriptor{ChainID: unknownChain, Timestamp: 200}

		err := backend.CheckAccessList(context.Background(), buildAccessList(access), types.LocalUnsafe, execDesc)
		require.ErrorIs(t, err, types.ErrUnknownChain)
	})

	t.Run("rejects_unsupported_safety_level", func(t *testing.T) {
		ingester := NewMockChainIngester()
		chains := map[eth.ChainID]ChainIngester{chainID: ingester}
		validator := createValidator(chains, expiryWindow, 500)
		backend := createBackend(ingester, validator)

		access := types.Access{ChainID: chainID, Timestamp: 100, BlockNumber: 10, LogIndex: 0, Checksum: validChecksum(1)}
		execDesc := types.ExecutingDescriptor{ChainID: chainID, Timestamp: 200}

		err := backend.CheckAccessList(context.Background(), buildAccessList(access), types.Finalized, execDesc)
		require.Error(t, err)
		require.Contains(t, err.Error(), "unsupported safety level")
	})

	t.Run("accepts_valid_local_unsafe_message", func(t *testing.T) {
		ingester := NewMockChainIngester()
		chains := map[eth.ChainID]ChainIngester{chainID: ingester}
		validator := createValidator(chains, expiryWindow, 500)
		backend := createBackend(ingester, validator)

		// Add the init message to the ingester
		checksum := validChecksum(1)
		ingester.AddLog(100, 10, 0, checksum, types.BlockSeal{Number: 10})

		access := types.Access{ChainID: chainID, Timestamp: 100, BlockNumber: 10, LogIndex: 0, Checksum: checksum}
		execDesc := types.ExecutingDescriptor{ChainID: chainID, Timestamp: 200}

		err := backend.CheckAccessList(context.Background(), buildAccessList(access), types.LocalUnsafe, execDesc)
		require.NoError(t, err)
	})

	t.Run("rejects_missing_init_message", func(t *testing.T) {
		ingester := NewMockChainIngester()
		chains := map[eth.ChainID]ChainIngester{chainID: ingester}
		validator := createValidator(chains, expiryWindow, 500)
		backend := createBackend(ingester, validator)

		// Don't add any init message
		access := types.Access{ChainID: chainID, Timestamp: 100, BlockNumber: 10, LogIndex: 0, Checksum: validChecksum(1)}
		execDesc := types.ExecutingDescriptor{ChainID: chainID, Timestamp: 200}

		err := backend.CheckAccessList(context.Background(), buildAccessList(access), types.LocalUnsafe, execDesc)
		require.ErrorIs(t, err, types.ErrConflict)
	})

	t.Run("rejects_expired_message", func(t *testing.T) {
		ingester := NewMockChainIngester()
		chains := map[eth.ChainID]ChainIngester{chainID: ingester}
		validator := createValidator(chains, expiryWindow, 500)
		backend := createBackend(ingester, validator)

		checksum := validChecksum(1)
		ingester.AddLog(100, 10, 0, checksum, types.BlockSeal{Number: 10})

		// Init at 100, expiry window 1000, exec at 2000 -> expired
		access := types.Access{ChainID: chainID, Timestamp: 100, BlockNumber: 10, LogIndex: 0, Checksum: checksum}
		execDesc := types.ExecutingDescriptor{ChainID: chainID, Timestamp: 2000}

		err := backend.CheckAccessList(context.Background(), buildAccessList(access), types.LocalUnsafe, execDesc)
		require.ErrorIs(t, err, types.ErrConflict)
		require.Contains(t, err.Error(), "expired")
	})

	t.Run("rejects_init_not_before_exec", func(t *testing.T) {
		ingester := NewMockChainIngester()
		chains := map[eth.ChainID]ChainIngester{chainID: ingester}
		validator := createValidator(chains, expiryWindow, 500)
		backend := createBackend(ingester, validator)

		checksum := validChecksum(1)
		ingester.AddLog(200, 10, 0, checksum, types.BlockSeal{Number: 10})

		// Init at 200, exec at 100 -> init not before exec
		access := types.Access{ChainID: chainID, Timestamp: 200, BlockNumber: 10, LogIndex: 0, Checksum: checksum}
		execDesc := types.ExecutingDescriptor{ChainID: chainID, Timestamp: 100}

		err := backend.CheckAccessList(context.Background(), buildAccessList(access), types.LocalUnsafe, execDesc)
		require.ErrorIs(t, err, types.ErrConflict)
	})

	t.Run("cross_unsafe_accepts_validated_timestamp", func(t *testing.T) {
		ingester := NewMockChainIngester()
		chains := map[eth.ChainID]ChainIngester{chainID: ingester}
		validator := createValidator(chains, expiryWindow, 500) // cross-validated up to 500
		backend := createBackend(ingester, validator)

		checksum := validChecksum(1)
		ingester.AddLog(100, 10, 0, checksum, types.BlockSeal{Number: 10})

		// Init at 100, cross-validated up to 500
		access := types.Access{ChainID: chainID, Timestamp: 100, BlockNumber: 10, LogIndex: 0, Checksum: checksum}
		execDesc := types.ExecutingDescriptor{ChainID: chainID, Timestamp: 200}

		err := backend.CheckAccessList(context.Background(), buildAccessList(access), types.CrossUnsafe, execDesc)
		require.NoError(t, err)
	})

	t.Run("cross_unsafe_rejects_unvalidated_timestamp", func(t *testing.T) {
		ingester := NewMockChainIngester()
		chains := map[eth.ChainID]ChainIngester{chainID: ingester}
		validator := createValidator(chains, expiryWindow, 50) // cross-validated only up to 50
		backend := createBackend(ingester, validator)

		checksum := validChecksum(1)
		ingester.AddLog(100, 10, 0, checksum, types.BlockSeal{Number: 10})

		// Init at 100, but cross-validated only up to 50
		access := types.Access{ChainID: chainID, Timestamp: 100, BlockNumber: 10, LogIndex: 0, Checksum: checksum}
		execDesc := types.ExecutingDescriptor{ChainID: chainID, Timestamp: 200}

		err := backend.CheckAccessList(context.Background(), buildAccessList(access), types.CrossUnsafe, execDesc)
		require.ErrorIs(t, err, types.ErrOutOfScope)
	})
}

func TestMockCrossValidator_CrossUnsafeTimestamp(t *testing.T) {
	chainID := eth.ChainIDFromUInt64(1)
	ingester := NewMockChainIngester()
	chains := map[eth.ChainID]ChainIngester{chainID: ingester}
	validator := NewMockCrossValidator(chains, 1000)

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
