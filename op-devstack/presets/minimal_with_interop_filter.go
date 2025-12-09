package presets

import (
	"github.com/ethereum-optimism/optimism/op-devstack/devtest"
	"github.com/ethereum-optimism/optimism/op-devstack/dsl"
	"github.com/ethereum-optimism/optimism/op-devstack/shim"
	"github.com/ethereum-optimism/optimism/op-devstack/stack"
	"github.com/ethereum-optimism/optimism/op-devstack/stack/match"
	"github.com/ethereum-optimism/optimism/op-devstack/sysgo"
)

type MinimalWithInteropFilter struct {
	Minimal

	InteropFilter *dsl.InteropFilter
}

func WithMinimalWithInteropFilter() stack.CommonOption {
	return stack.MakeCommon(sysgo.DefaultMinimalSystemWithInteropFilter(&sysgo.DefaultMinimalSystemWithInteropFilterIDs{}))
}

func NewMinimalWithInteropFilter(t devtest.T) *MinimalWithInteropFilter {
	system := shim.NewSystem(t)
	orch := Orchestrator()
	orch.Hydrate(system)
	minimal := minimalFromSystem(t, system, orch)
	interopFilter := system.InteropFilter(match.Assume(t, match.FirstInteropFilter))
	return &MinimalWithInteropFilter{
		Minimal:       *minimal,
		InteropFilter: dsl.NewInteropFilter(interopFilter),
	}
}
