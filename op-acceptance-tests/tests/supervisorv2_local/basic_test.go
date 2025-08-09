package supervisorv2_local

import (
    "testing"

    "github.com/ethereum-optimism/optimism/op-devstack/devtest"
    "github.com/ethereum-optimism/optimism/op-devstack/presets"
)

func TestBootsAndRPC(t *testing.T) {
    tt := devtest.SerialT(t)
    sys := presets.NewMinimal(tt)
    _ = sys // ensure system hydrated; RPCs and components are running
    // No explicit assertions needed; if hydration fails, TestMain will error out.
}