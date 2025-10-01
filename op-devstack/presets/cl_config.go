package presets

import (
	"github.com/ethereum-optimism/optimism/op-devstack/devtest"
	"github.com/ethereum-optimism/optimism/op-devstack/stack"
	"github.com/ethereum-optimism/optimism/op-devstack/sysgo"
	"github.com/ethereum-optimism/optimism/op-node/rollup/sync"
)

func WithExecutionLayerSyncOnVerifiers() stack.CommonOption {
	return stack.MakeCommon(
		sysgo.WithGlobalL2CLOption(sysgo.L2CLOptionFn(
			func(_ devtest.P, id stack.L2CLNodeID, cfg *sysgo.L2CLConfig) {
				cfg.VerifierSyncMode = sync.ELSync
			})))
}

func WithConsensusLayerSync() stack.CommonOption {
	return stack.MakeCommon(
		sysgo.WithGlobalL2CLOption(sysgo.L2CLOptionFn(
			func(_ devtest.P, id stack.L2CLNodeID, cfg *sysgo.L2CLConfig) {
				cfg.SequencerSyncMode = sync.CLSync
				cfg.VerifierSyncMode = sync.CLSync
			})))
}

func WithSafeDBEnabled() stack.CommonOption {
	return stack.MakeCommon(
		sysgo.WithGlobalL2CLOption(sysgo.L2CLOptionFn(
			func(p devtest.P, id stack.L2CLNodeID, cfg *sysgo.L2CLConfig) {
				cfg.SafeDBPath = p.TempDir()
			})))
}

// WithLiteMode configures verifier nodes to run in lite mode, sourcing safe/finalized heads from a remote RPC.
// The remoteRPC parameter should be the RPC endpoint URL to source blocks from (typically the sequencer's RPC).
// This function should be used with a specific node ID to configure only that node.
func WithLiteMode(remoteRPC string) sysgo.L2CLOption {
	return sysgo.L2CLOptionFn(func(p devtest.P, id stack.L2CLNodeID, cfg *sysgo.L2CLConfig) {
		// Only enable lite mode on verifiers, not sequencers
		if !cfg.IsSequencer {
			cfg.LiteModeEnabled = true
			cfg.LiteModeRemoteRPC = remoteRPC
		}
	})
}
