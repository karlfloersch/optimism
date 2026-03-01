package reorg_nofilter

import (
	"os"
	"testing"

	"github.com/ethereum-optimism/optimism/op-devstack/presets"
)

func TestMain(m *testing.M) {
	_ = os.Setenv("DEVSTACK_L2CL_KIND", "supernode")
	presets.DoMain(m, presets.WithTwoL2SupernodeInterop(0))
}
