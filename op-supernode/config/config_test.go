package config

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestCLIConfig_Check_interopLogBackfill(t *testing.T) {
	ptr := func(u uint64) *uint64 { return &u }
	tests := []struct {
		name    string
		cfg     *CLIConfig
		wantErr string
	}{
		{
			name: "ok with activation and depth",
			cfg:  &CLIConfig{L1NodeAddr: "http://x", InteropActivationTimestamp: ptr(1), InteropLogBackfillDepth: time.Hour},
		},
		{
			// No CLI activation here is fine at the Check() layer; the
			// rollup-derived path is a valid activation source, and the
			// pairing is re-checked in supernode.New after resolution.
			name: "depth without CLI activation is allowed at Check; resolved later",
			cfg:  &CLIConfig{L1NodeAddr: "http://x", InteropLogBackfillDepth: time.Hour},
		},
		{
			name:    "negative depth",
			cfg:     &CLIConfig{L1NodeAddr: "http://x", InteropActivationTimestamp: ptr(1), InteropLogBackfillDepth: -time.Second},
			wantErr: "interop.log-backfill-depth must be >= 0",
		},
		{
			// Zero is the documented "disables backfill" value. Must stay
			// accepted by Check() — a regression here would make every
			// operator who doesn't explicitly configure depth fail startup.
			name: "zero depth disables backfill and passes Check",
			cfg:  &CLIConfig{L1NodeAddr: "http://x"},
		},
		{
			// Sub-second positive durations truncate to zero inside
			// LogBackfillLowerBound (see log_backfill.go), so accepting
			// them would silently no-op backfill. Check() should reject
			// at config time so operators see the problem immediately.
			name:    "sub-second positive depth rejected",
			cfg:     &CLIConfig{L1NodeAddr: "http://x", InteropActivationTimestamp: ptr(1), InteropLogBackfillDepth: 500 * time.Millisecond},
			wantErr: "must be >= 1s when non-zero",
		},
		{
			// Exactly 1s is the smallest non-zero depth that survives the
			// seconds-floor conversion. Pin it so future refactors don't
			// accidentally tighten the bound past what operators can set.
			name: "one-second depth is the minimum accepted positive value",
			cfg:  &CLIConfig{L1NodeAddr: "http://x", InteropActivationTimestamp: ptr(1), InteropLogBackfillDepth: time.Second},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.cfg.Check()
			if tt.wantErr == "" {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, tt.wantErr)
			}
		})
	}
}
