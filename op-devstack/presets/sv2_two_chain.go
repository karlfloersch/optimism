package presets

import (
    "github.com/ethereum/go-ethereum/log"

    "github.com/ethereum-optimism/optimism/op-devstack/devtest"
    "github.com/ethereum-optimism/optimism/op-devstack/shim"
    "github.com/ethereum-optimism/optimism/op-devstack/stack"
    "github.com/ethereum-optimism/optimism/op-devstack/sysgo"
)

// WithSV2TwoChainMinimal composes a minimal two-chain preset and adds Supervisor v2 (multi-chain).
// offset controls interop2 activation (e.g., 6 seconds after genesis).
func WithSV2TwoChainMinimal(offset uint64) stack.CommonOption {
    return stack.Combine[stack.Orchestrator](
        stack.MakeCommon(sysgo.DefaultTwoMinimalSystemNoCL(&sysgo.DefaultTwoMinimalSystemIDs{})),
        stack.MakeCommon(sysgo.WithSupervisorV2OnAllChains()),
        stack.MakeCommon(sysgo.WithInterop2ActivationOffsetForSV2(offset)),
        WithL2NetworkCount(2),
    )
}

// SV2TwoChainMinimal is a convenience wrapper to construct and access a two-chain SV2 minimal system.
type SV2TwoChainMinimal struct {
    Log          log.Logger
    T            devtest.T
    ControlPlane stack.ControlPlane

    // Provide access to the hydrated system via existing DSL types if needed later
    System stack.ExtensibleSystem
}

// NewSV2TwoChainMinimal builds a two-chain system using the minimal two-chain preset plus SV2 multi-chain.
func NewSV2TwoChainMinimal(t devtest.T, offset uint64) *SV2TwoChainMinimal {
    system := shim.NewSystem(t)
    orch := Orchestrator()
    orch.Hydrate(system)
    return &SV2TwoChainMinimal{
        Log:          t.Logger(),
        T:            t,
        ControlPlane: orch.ControlPlane(),
        System:       system,
    }
}


