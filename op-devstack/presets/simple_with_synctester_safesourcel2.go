package presets

import (
	"github.com/ethereum-optimism/optimism/op-devstack/devtest"
	"github.com/ethereum-optimism/optimism/op-devstack/dsl"
	"github.com/ethereum-optimism/optimism/op-devstack/shim"
	"github.com/ethereum-optimism/optimism/op-devstack/stack"
	"github.com/ethereum-optimism/optimism/op-devstack/stack/match"
	"github.com/ethereum-optimism/optimism/op-devstack/sysgo"
)

type SimpleWithSyncTesterLightMode struct {
	Minimal

	SyncTester     *dsl.SyncTester
	SyncTesterL2EL *dsl.L2ELNode
	L2CL2          *dsl.L2CLNode
}

func WithSimpleWithSyncTesterLightMode() stack.CommonOption {
	return stack.MakeCommon(sysgo.DefaultSimpleSystemWithSyncTesterLightMode(&sysgo.DefaultSimpleSystemWithSyncTesterLightModeIDs{}))
}

func NewSimpleWithSyncTesterLightMode(t devtest.T) *SimpleWithSyncTesterLightMode {
	system := shim.NewSystem(t)
	orch := Orchestrator()
	orch.Hydrate(system)
	minimal := minimalFromSystem(t, system, orch)
	l2 := system.L2Network(match.L2ChainA)
	syncTester := l2.SyncTester(match.FirstSyncTester)

	// L2CL2 connected to L2EL initialized by sync tester, with light CL mode enabled
	l2CL2 := l2.L2CLNode(match.SecondL2CL)
	// L2EL initialized by sync tester
	syncTesterL2EL := l2.L2ELNode(match.SecondL2EL)

	return &SimpleWithSyncTesterLightMode{
		Minimal:        *minimal,
		SyncTester:     dsl.NewSyncTester(syncTester),
		SyncTesterL2EL: dsl.NewL2ELNode(syncTesterL2EL, orch.ControlPlane()),
		L2CL2:          dsl.NewL2CLNode(l2CL2, orch.ControlPlane()),
	}
}
