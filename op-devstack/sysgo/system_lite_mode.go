package sysgo

import (
	"github.com/ethereum-optimism/optimism/op-devstack/devtest"
	"github.com/ethereum-optimism/optimism/op-devstack/stack"
)

// LiteModeSystem creates a single-chain multi-node system where one verifier runs in lite mode,
// sourcing safe/finalized heads from the sequencer's RPC endpoint.
func LiteModeSystem(dest *DefaultSingleChainMultiNodeSystemIDs) stack.Option[*Orchestrator] {
	ids := NewDefaultSingleChainMultiNodeSystemIDs(DefaultL1ID, DefaultL2AID)

	opt := stack.Combine[*Orchestrator]()

	// Start with the base minimal system (creates sequencer node)
	opt.Add(DefaultMinimalSystem(&dest.DefaultMinimalSystemIDs))

	// Add verifier EL node
	opt.Add(WithL2ELNode(ids.L2ELB))

	// Add verifier CL node with lite mode configuration
	// We use AfterDeploy to configure lite mode after the sequencer is available
	opt.Add(stack.AfterDeploy(func(orch *Orchestrator) {
		// Get the sequencer's EL RPC endpoint (not CL)
		// Lite mode needs to fetch blocks via eth_getBlockByNumber which is only available on the EL
		sequencerEL, ok := orch.l2ELs.Get(ids.L2EL)
		orch.P().Require().True(ok, "sequencer EL node required for lite mode")

		sequencerRPC := sequencerEL.UserRPC()
		orch.P().Logger().Info("Configuring lite mode verifier", "sequencer_rpc", sequencerRPC)

		// Create the verifier with lite mode enabled using the sequencer's EL RPC
		stack.ApplyOptionLifecycle(WithL2CLNode(ids.L2CLB, ids.L1CL, ids.L1EL, ids.L2ELB, WithLiteModeOption(sequencerRPC)), orch)
	}))

	// P2P connect L2CL nodes (for unsafe block sync via P2P)
	opt.Add(WithL2CLP2PConnection(ids.L2CL, ids.L2CLB))
	opt.Add(WithL2ELP2PConnection(ids.L2EL, ids.L2ELB))

	opt.Add(stack.Finally(func(orch *Orchestrator) {
		*dest = ids
	}))
	return opt
}

// WithLiteModeOption creates an L2CLOption that enables lite mode with the given remote RPC
func WithLiteModeOption(remoteRPC string) L2CLOption {
	return L2CLOptionFn(func(p devtest.P, id stack.L2CLNodeID, cfg *L2CLConfig) {
		// Only enable lite mode on verifiers
		if !cfg.IsSequencer {
			cfg.LiteModeEnabled = true
			cfg.LiteModeRemoteRPC = remoteRPC
			p.Logger().Info("Lite mode configured for node", "node_id", id, "remote_rpc", remoteRPC)
		}
	})
}
