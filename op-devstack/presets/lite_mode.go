package presets

import (
	"github.com/ethereum-optimism/optimism/op-devstack/devtest"
	"github.com/ethereum-optimism/optimism/op-devstack/dsl"
	"github.com/ethereum-optimism/optimism/op-devstack/shim"
	"github.com/ethereum-optimism/optimism/op-devstack/stack"
	"github.com/ethereum-optimism/optimism/op-devstack/stack/match"
	"github.com/ethereum-optimism/optimism/op-devstack/sysgo"
	"github.com/ethereum-optimism/optimism/op-supervisor/supervisor/types"
)

// LiteMode contains a single-chain multi-node setup where one verifier runs in lite mode
type LiteMode struct {
	Minimal

	// L2ELB is the verifier EL node
	L2ELB *dsl.L2ELNode
	// L2CLB is the verifier CL node running in lite mode
	L2CLB *dsl.L2CLNode
}

func WithLiteModeSystem() stack.CommonOption {
	return stack.MakeCommon(sysgo.LiteModeSystem(&sysgo.DefaultSingleChainMultiNodeSystemIDs{}))
}

func NewLiteMode(t devtest.T) *LiteMode {
	system := shim.NewSystem(t)
	orch := Orchestrator()
	orch.Hydrate(system)
	minimal := minimalFromSystem(t, system, orch)
	l2 := system.L2Network(match.Assume(t, match.L2ChainA))
	verifierCL := l2.L2CLNode(match.Assume(t,
		match.And(
			match.Not(match.WithSequencerActive(t.Ctx())),
			match.Not[stack.L2CLNodeID, stack.L2CLNode](minimal.L2CL.ID()),
		)))
	verifierEL := l2.L2ELNode(match.Assume(t,
		match.And(
			match.EngineFor(verifierCL),
			match.Not[stack.L2ELNodeID, stack.L2ELNode](minimal.L2EL.ID()))))
	preset := &LiteMode{
		Minimal: *minimal,
		L2ELB:   dsl.NewL2ELNode(verifierEL, orch.ControlPlane()),
		L2CLB:   dsl.NewL2CLNode(verifierCL, orch.ControlPlane()),
	}
	// Ensure the lite mode follower node is in sync with the sequencer before starting tests
	dsl.CheckAll(t,
		preset.L2CLB.MatchedFn(preset.L2CL, types.LocalSafe, 30),
		preset.L2CLB.MatchedFn(preset.L2CL, types.Finalized, 30),
	)
	return preset
}
