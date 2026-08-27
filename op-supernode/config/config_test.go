package config

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/urfave/cli/v2"

	"github.com/ethereum-optimism/optimism/op-supernode/flags"
)

func TestNewConfigReadsSharedBeaconSlotDurationOverride(t *testing.T) {
	var got *CLIConfig
	app := cli.NewApp()
	app.Flags = flags.Flags
	app.Action = func(ctx *cli.Context) error {
		got = NewConfig(ctx)
		return nil
	}
	require.NoError(t, app.Run([]string{
		"op-supernode",
		"--l1", "http://anvil:8545",
		"--l1.beacon.slot-duration-override", "2",
	}))
	require.NotNil(t, got)
	require.Equal(t, uint64(2), got.L1BeaconSlotDurationOverride)
}

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
