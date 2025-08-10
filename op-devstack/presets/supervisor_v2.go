package presets

import (
	"github.com/ethereum/go-ethereum/log"

	"github.com/ethereum-optimism/optimism/op-devstack/devtest"
	"github.com/ethereum-optimism/optimism/op-devstack/shim"
	"github.com/ethereum-optimism/optimism/op-devstack/stack"
	"github.com/ethereum-optimism/optimism/op-devstack/sysgo"
)

// WithSupervisorV2Minimal composes a minimal single-chain preset and adds Supervisor v2 prototype.
func WithSupervisorV2Minimal() stack.CommonOption {
	// Use a minimal system without CL, then let SupervisorV2 embed/manage the op-node.
	return stack.Combine[stack.Orchestrator](
		stack.MakeCommon(sysgo.DefaultMinimalSystemNoCL(&sysgo.DefaultMinimalSystemIDs{})),
		stack.MakeCommon(sysgo.WithSupervisorV2OnFirstChain()),
	)
}

type SupervisorV2Minimal struct {
	Log          log.Logger
	T            devtest.T
	ControlPlane stack.ControlPlane

	// Reuse minimal preset structure fields for convenience
	Minimal
	// A handle to query supervisor status via existing DSL wrapper, if available later.
	// For now, reuse L2CL/L2EL and assert via chain progress or future sv2 client.
}

// NewSupervisorV2Minimal builds a system using minimal preset plus supervisor v2.
func NewSupervisorV2Minimal(t devtest.T) *SupervisorV2Minimal {
	// Construct using standard path to avoid referencing unexported sysgo constructors.
	system := shim.NewSystem(t)
	orch := Orchestrator()
	orch.Hydrate(system)
	m := minimalFromSystem(t, system, orch)
	return &SupervisorV2Minimal{
		Log:          t.Logger(),
		T:            t,
		ControlPlane: orch.ControlPlane(),
		Minimal:      *m,
	}
}
