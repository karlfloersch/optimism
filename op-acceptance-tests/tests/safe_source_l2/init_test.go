package safe_source_l2

import (
	"testing"

	"github.com/ethereum-optimism/optimism/op-devstack/compat"
	"github.com/ethereum-optimism/optimism/op-devstack/presets"
)

func TestMain(m *testing.M) {
	presets.DoMain(m,
		presets.WithSingleChainMultiNodeWithLightMode(),
		presets.WithCompatibleTypes(compat.SysGo),
	)
}
