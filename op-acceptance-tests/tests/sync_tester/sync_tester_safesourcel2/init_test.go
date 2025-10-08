package sync_tester_safesourcel2

import (
	"testing"

	"github.com/ethereum-optimism/optimism/op-devstack/compat"
	"github.com/ethereum-optimism/optimism/op-devstack/presets"
)

func TestMain(m *testing.M) {
	presets.DoMain(m,
		presets.WithSimpleWithSyncTesterLightMode(),
		presets.WithCompatibleTypes(compat.SysGo),
	)
}
