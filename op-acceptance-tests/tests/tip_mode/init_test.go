package tip_mode

import (
	"testing"

	"github.com/ethereum-optimism/optimism/op-devstack/compat"
	"github.com/ethereum-optimism/optimism/op-devstack/presets"
)

func TestMain(m *testing.M) {
	presets.DoMain(m,
		presets.WithTipModeSystem(),
		presets.WithConsensusLayerSync(),
		presets.WithCompatibleTypes(compat.SysGo),
	)
}
