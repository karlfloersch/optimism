package sysgo

import (
	"github.com/ethereum-optimism/optimism/op-chain-ops/devkeys"
	"github.com/ethereum-optimism/optimism/op-devstack/devtest"
	"github.com/ethereum-optimism/optimism/op-devstack/stack"
)

// TipModeSystem creates a single-chain multi-node system where one verifier runs in tip mode,
// sourcing safe/finalized heads from the sequencer's RPC endpoint.
// Note: This system does NOT include a challenger because tip mode nodes do not maintain
// a safe head database, which the challenger requires for dispute game verification.
func TipModeSystem(dest *DefaultSingleChainMultiNodeSystemIDs) stack.Option[*Orchestrator] {
	ids := NewDefaultSingleChainMultiNodeSystemIDs(DefaultL1ID, DefaultL2AID)

	opt := stack.Combine[*Orchestrator]()

	// Build a minimal system without the challenger (since tip mode doesn't support safe head DB)
	opt.Add(stack.BeforeDeploy(func(o *Orchestrator) {
		o.P().Logger().Info("Setting up tip mode system (no challenger)")
	}))

	opt.Add(WithMnemonicKeys(devkeys.TestMnemonic))

	opt.Add(WithDeployer(),
		WithDeployerOptions(
			WithLocalContractSources(),
			WithCommons(ids.L1.ChainID()),
			WithPrefundedL2(ids.L1.ChainID(), ids.L2.ChainID()),
		),
	)

	opt.Add(WithL1Nodes(ids.L1EL, ids.L1CL))

	opt.Add(WithL2ELNode(ids.L2EL))
	opt.Add(WithL2CLNode(ids.L2CL, ids.L1CL, ids.L1EL, ids.L2EL, L2CLSequencer()))

	opt.Add(WithBatcher(ids.L2Batcher, ids.L1EL, ids.L2CL, ids.L2EL))
	opt.Add(WithProposer(ids.L2Proposer, ids.L1EL, &ids.L2CL, nil))

	opt.Add(WithFaucets([]stack.L1ELNodeID{ids.L1EL}, []stack.L2ELNodeID{ids.L2EL}))

	opt.Add(WithTestSequencer(ids.TestSequencer, ids.L1CL, ids.L2CL, ids.L1EL, ids.L2EL))

	// NOTE: Challenger is intentionally omitted because it queries the safe head database,
	// which is disabled in tip mode. This was causing 1350+ error logs per test run.

	// Add verifier EL node
	opt.Add(WithL2ELNode(ids.L2ELB))

	// Add verifier CL node with tip mode configuration
	// We use AfterDeploy to configure tip mode after the sequencer is available
	opt.Add(stack.AfterDeploy(func(orch *Orchestrator) {
		// Get the sequencer's EL RPC endpoint (not CL)
		// Tip mode needs to fetch blocks via eth_getBlockByNumber which is only available on the EL
		sequencerEL, ok := orch.l2ELs.Get(ids.L2EL)
		orch.P().Require().True(ok, "sequencer EL node required for tip mode")

		sequencerRPC := sequencerEL.UserRPC()
		orch.P().Logger().Info("Configuring tip mode verifier", "sequencer_rpc", sequencerRPC)

		// Create the verifier with tip mode enabled using the sequencer's EL RPC
		stack.ApplyOptionLifecycle(WithL2CLNode(ids.L2CLB, ids.L1CL, ids.L1EL, ids.L2ELB, WithTipModeOption(sequencerRPC)), orch)
	}))

	// P2P connect L2CL nodes (for unsafe block sync via P2P)
	opt.Add(WithL2CLP2PConnection(ids.L2CL, ids.L2CLB))
	opt.Add(WithL2ELP2PConnection(ids.L2EL, ids.L2ELB))

	opt.Add(stack.Finally(func(orch *Orchestrator) {
		*dest = ids
	}))
	return opt
}

// WithTipModeOption creates an L2CLOption that enables tip mode with the given remote RPC
func WithTipModeOption(remoteRPC string) L2CLOption {
	return L2CLOptionFn(func(p devtest.P, id stack.L2CLNodeID, cfg *L2CLConfig) {
		// Only enable tip mode on verifiers
		if !cfg.IsSequencer {
			cfg.TipModeEnabled = true
			cfg.TipModeRemoteRPC = remoteRPC
			p.Logger().Info("Tip mode configured for node", "node_id", id, "remote_rpc", remoteRPC)
		}
	})
}
