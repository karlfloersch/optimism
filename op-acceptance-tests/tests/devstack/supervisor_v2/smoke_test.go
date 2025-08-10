package supervisor_v2

import (
	"testing"

	"github.com/ethereum-optimism/optimism/op-devstack/devtest"
	"github.com/ethereum-optimism/optimism/op-devstack/dsl"
	"github.com/ethereum-optimism/optimism/op-devstack/presets"
	"github.com/ethereum-optimism/optimism/op-devstack/shim"
	"github.com/ethereum-optimism/optimism/op-devstack/stack/match"
)

// TestSupervisorV2Smoke verifies L2 block production under the SupervisorV2 minimal preset.
func TestSupervisorV2Smoke(t *testing.T) {
	tt := devtest.SerialT(t)
	// Hydrate system view
	system := shim.NewSystem(tt)
	orch := presets.Orchestrator()
	orch.Hydrate(system)

	l2 := system.L2Network(match.Assume(tt, match.L2ChainA))
	elStack := l2.L2ELNode(match.Assume(tt, match.FirstL2EL))
	el := dsl.NewL2ELNode(elStack, orch.ControlPlane())

	// Wait for 2 L2 blocks to be produced by the sequencer
	el.WaitForBlock()
	el.WaitForBlock()
}
