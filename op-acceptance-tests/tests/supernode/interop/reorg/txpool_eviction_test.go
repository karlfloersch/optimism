package reorg

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ethereum-optimism/optimism/op-acceptance-tests/tests/supernode/interop/reorg/testutil"
	"github.com/ethereum-optimism/optimism/op-devstack/devtest"
)

// TestInvalidExecMsgEvictedOnReorg verifies that with mempool filtering enabled,
// an invalid executing message is evicted from the txpool on reorg. The supernode
// rejects the block exactly once, and the replacement block is validated.
func TestInvalidExecMsgEvictedOnReorg(gt *testing.T) {
	t := devtest.SerialT(gt)
	scenario := testutil.SetupInvalidExecMsgScenario(t, true)
	scenario.WaitForStableChain()
	require.Equal(t, 1, scenario.CountCycles(),
		"filtering ON: invalid tx should be evicted, exactly 1 reorg")
}
