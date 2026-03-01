package reorg_nofilter

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ethereum-optimism/optimism/op-acceptance-tests/tests/supernode/interop/reorg/testutil"
	"github.com/ethereum-optimism/optimism/op-devstack/devtest"
)

// TestInvalidExecMsgReinjectedOnReorg verifies that without mempool filtering,
// an invalid executing message is reinjected after each reorg, causing repeated
// reorg cycles as the supernode keeps rejecting the re-included tx.
func TestInvalidExecMsgReinjectedOnReorg(gt *testing.T) {
	t := devtest.SerialT(gt)
	scenario := testutil.SetupInvalidExecMsgScenario(t, false)
	scenario.WaitForCycles(2)
	require.GreaterOrEqual(t, scenario.CountCycles(), 2,
		"filtering OFF: invalid tx should be reinjected, at least 2 reorgs")
}
