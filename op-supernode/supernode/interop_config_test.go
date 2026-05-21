package supernode

import (
	"testing"
	"time"

	opnodecfg "github.com/ethereum-optimism/optimism/op-node/config"
	"github.com/ethereum-optimism/optimism/op-node/rollup"
	nodesync "github.com/ethereum-optimism/optimism/op-node/rollup/sync"
	"github.com/ethereum-optimism/optimism/op-service/eth"
	"github.com/stretchr/testify/require"
)

func TestEffectiveInteropLogBackfillDepth(t *testing.T) {
	t.Parallel()

	depth := time.Hour
	tests := []struct {
		name         string
		depth        time.Duration
		vnCfgs       map[eth.ChainID]*opnodecfg.Config
		wantDepth    time.Duration
		wantDisabled bool
	}{
		{
			name:         "disabled depth stays disabled",
			depth:        0,
			wantDepth:    0,
			wantDisabled: false,
		},
		{
			name:  "keeps backfill when all chains use EL sync",
			depth: depth,
			vnCfgs: map[eth.ChainID]*opnodecfg.Config{
				eth.ChainIDFromUInt64(10):   {Sync: nodesync.Config{SyncMode: nodesync.ELSync}},
				eth.ChainIDFromUInt64(8453): {Sync: nodesync.Config{SyncMode: nodesync.ELSync}},
			},
			wantDepth:    depth,
			wantDisabled: false,
		},
		{
			name:  "disables backfill when any chain uses CL sync",
			depth: depth,
			vnCfgs: map[eth.ChainID]*opnodecfg.Config{
				eth.ChainIDFromUInt64(10):   {Sync: nodesync.Config{SyncMode: nodesync.ELSync}},
				eth.ChainIDFromUInt64(8453): {Sync: nodesync.Config{SyncMode: nodesync.CLSync}},
			},
			wantDepth:    0,
			wantDisabled: true,
		},
		{
			name:  "default sync mode is CL sync",
			depth: depth,
			vnCfgs: map[eth.ChainID]*opnodecfg.Config{
				eth.ChainIDFromUInt64(10): {},
			},
			wantDepth:    0,
			wantDisabled: true,
		},
		{
			name:  "nil configs are ignored",
			depth: depth,
			vnCfgs: map[eth.ChainID]*opnodecfg.Config{
				eth.ChainIDFromUInt64(10): nil,
			},
			wantDepth:    depth,
			wantDisabled: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			gotDepth, gotDisabled := effectiveInteropLogBackfillDepth(tt.depth, tt.vnCfgs)
			require.Equal(t, tt.wantDepth, gotDepth)
			require.Equal(t, tt.wantDisabled, gotDisabled)
		})
	}
}

func TestResolveInteropActivationTimestamp(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		override *uint64
		vnCfgs   map[eth.ChainID]*opnodecfg.Config
		want     *uint64
		wantErr  string
	}{
		{
			name:     "override wins over rollup configs",
			override: uint64Ptr(42),
			vnCfgs: map[eth.ChainID]*opnodecfg.Config{
				eth.ChainIDFromUInt64(10):   {Rollup: rollup.Config{InteropTime: uint64Ptr(100)}},
				eth.ChainIDFromUInt64(8453): {Rollup: rollup.Config{InteropTime: uint64Ptr(200)}},
			},
			want: uint64Ptr(42),
		},
		{
			name: "derive from consistent rollup configs",
			vnCfgs: map[eth.ChainID]*opnodecfg.Config{
				eth.ChainIDFromUInt64(10):   {Rollup: rollup.Config{InteropTime: uint64Ptr(1234)}},
				eth.ChainIDFromUInt64(8453): {Rollup: rollup.Config{InteropTime: uint64Ptr(1234)}},
			},
			want: uint64Ptr(1234),
		},
		{
			name: "leave interop disabled when no rollup config enables it",
			vnCfgs: map[eth.ChainID]*opnodecfg.Config{
				eth.ChainIDFromUInt64(10):   {Rollup: rollup.Config{}},
				eth.ChainIDFromUInt64(8453): {Rollup: rollup.Config{}},
			},
		},
		{
			name: "error on mixed nil and configured rollup timestamps",
			vnCfgs: map[eth.ChainID]*opnodecfg.Config{
				eth.ChainIDFromUInt64(10):   {Rollup: rollup.Config{}},
				eth.ChainIDFromUInt64(8453): {Rollup: rollup.Config{InteropTime: uint64Ptr(1234)}},
			},
			wantErr: "has no interop activation timestamp",
		},
		{
			name: "error on mismatched rollup timestamps",
			vnCfgs: map[eth.ChainID]*opnodecfg.Config{
				eth.ChainIDFromUInt64(10):   {Rollup: rollup.Config{InteropTime: uint64Ptr(100)}},
				eth.ChainIDFromUInt64(8453): {Rollup: rollup.Config{InteropTime: uint64Ptr(200)}},
			},
			wantErr: "mismatched interop activation timestamps",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := resolveInteropActivationTimestamp(tt.override, tt.vnCfgs)
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				return
			}

			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

func uint64Ptr(v uint64) *uint64 {
	return &v
}
