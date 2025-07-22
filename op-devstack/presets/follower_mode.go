package presets

import (
	"time"

	"github.com/ethereum-optimism/optimism/op-devstack/devtest"
	"github.com/ethereum-optimism/optimism/op-devstack/dsl"
	"github.com/ethereum-optimism/optimism/op-devstack/shim"
	"github.com/ethereum-optimism/optimism/op-devstack/stack"
	"github.com/ethereum-optimism/optimism/op-devstack/sysgo"
	"github.com/ethereum-optimism/optimism/op-node/config"
)

// FollowerMode represents a safe head gossip test system with:
// - Single L2 chain
// - Prover node (sequencer): derives safe heads normally, gossips them via P2P
// - Follower node (verifier): disables derivation, receives safe heads via P2P gossip
type FollowerMode struct {
	SingleChainMultiNode

	// ProverCL is the sequencer node running in prover mode (gossips safe heads)
	ProverCL *dsl.L2CLNode
	// FollowerCL is the verifier node running in follower mode (receives safe heads)
	FollowerCL *dsl.L2CLNode

	// system is needed for time travel functionality
	system stack.ExtensibleSystem
}

// WithFollowerMode specifies a system for safe head gossip testing
// Sets up a single L2 chain with two nodes: one prover, one follower
func WithFollowerMode() stack.CommonOption {
	return stack.Combine(
		// Use single chain multi-node system as base
		stack.MakeCommon(sysgo.DefaultSingleChainMultiNodeSystem(&sysgo.DefaultSingleChainMultiNodeSystemIDs{})),
		// Enable time travel for test time advancement
		WithTimeTravel(),
		// Configure sequencer as prover mode
		stack.MakeCommon(sysgo.WithL2CLOption(func(p devtest.P, id stack.L2CLNodeID, cfg *config.Config) {
			// The sequencer should be in prover mode
			if isSequencer(p, id) {
				p.Logger().Info("Configuring sequencer node in prover mode", "id", id)
				cfg.Driver.Mode = "prover"
			}
		})),
		// Configure verifier as follower mode
		stack.MakeCommon(sysgo.WithL2CLOption(func(p devtest.P, id stack.L2CLNodeID, cfg *config.Config) {
			// The verifier should be in follower mode
			if !isSequencer(p, id) {
				p.Logger().Info("Configuring verifier node in follower mode", "id", id)
				cfg.Driver.Mode = "follower"
			}
		})),
	)
}

// isSequencer determines if a given L2CL node ID represents a sequencer
// This is a simple way to distinguish sequencer from verifier in the single-chain setup
func isSequencer(p devtest.P, id stack.L2CLNodeID) bool {
	// In SingleChainMultiNode, the sequencer is the "active" sequencer node
	// The verifier is the additional non-sequencer node
	idStr := id.String()

	// Based on the devstack node naming patterns:
	// - Sequencers typically have "sequencer" in the name or are the primary CL node
	// - Verifiers are secondary nodes
	isSeq := (idStr == "L2CLNode-sequencer-901" ||
		idStr == "sequencer-901" ||
		idStr == "L2CLNode-main-901" ||
		idStr == "main-901")

	p.Logger().Debug("Checking if node is sequencer", "id", idStr, "isSequencer", isSeq)
	return isSeq
}

func NewFollowerMode(t devtest.T) *FollowerMode {
	system := shim.NewSystem(t)
	orch := Orchestrator()
	t.Logger().Info("Setting up follower mode system with single chain and two nodes")
	orch.Hydrate(system)

	// Build on SingleChainMultiNode foundation
	singleChain := NewSingleChainMultiNode(t)

	out := &FollowerMode{
		SingleChainMultiNode: *singleChain,
		// ProverCL = Sequencer (L2CL from minimal)
		ProverCL: singleChain.L2CL,
		// FollowerCL = Verifier (L2CLB from multi-node)
		FollowerCL: singleChain.L2CLB,
		system:     system,
	}

	t.Logger().Info("Follower mode system initialized",
		"prover_id", out.ProverCL.Escape().ID(),
		"follower_id", out.FollowerCL.Escape().ID())

	return out
}

func (f *FollowerMode) AdvanceTime(amount time.Duration) {
	ttSys, ok := f.system.(stack.TimeTravelSystem)
	f.T.Require().True(ok, "attempting to advance time on incompatible system")
	ttSys.AdvanceTime(amount)
}
