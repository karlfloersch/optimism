package config

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestCLIConfig_Check_logBackfillRequiresActivation(t *testing.T) {
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
			name:    "backfill without activation",
			cfg:     &CLIConfig{L1NodeAddr: "http://x", InteropLogBackfillDepth: time.Hour},
			wantErr: "interop.log-backfill-depth requires interop.activation-timestamp",
		},
		{
			name:    "negative depth",
			cfg:     &CLIConfig{L1NodeAddr: "http://x", InteropActivationTimestamp: ptr(1), InteropLogBackfillDepth: -time.Second},
			wantErr: "interop.log-backfill-depth must be >= 0",
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
