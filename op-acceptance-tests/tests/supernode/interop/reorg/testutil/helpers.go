// Package testutil provides shared test helpers for supernode interop reorg tests.
package testutil

import (
	"math/rand"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/ethereum-optimism/optimism/op-devstack/devtest"
	"github.com/ethereum-optimism/optimism/op-devstack/presets"
	"github.com/ethereum-optimism/optimism/op-devstack/sysgo"
	"github.com/ethereum-optimism/optimism/op-service/bigs"
	"github.com/ethereum-optimism/optimism/op-service/eth"
)

// InvalidExecMsgScenario holds the state of a running scenario where an invalid
// executing message has been sent on chain B. Tests use CountCycles to observe
// reorg behavior and the other fields to make assertions.
type InvalidExecMsgScenario struct {
	Sys                   *presets.TwoL2SupernodeInterop
	InvalidBlockNumber    uint64
	InvalidBlockTimestamp uint64
	cycles                int
	highWater             uint64
	t                     devtest.T
}

// CountCycles polls the unsafe head and returns the number of reorg cycles
// observed so far. Each time the head drops below the previous high-water
// mark, that's one cycle.
func (s *InvalidExecMsgScenario) CountCycles() int {
	ctx := s.t.Ctx()
	head, err := s.Sys.L2ELB.Escape().EthClient().BlockRefByLabel(ctx, eth.Unsafe)
	if err != nil {
		return s.cycles
	}
	if head.Number > s.highWater {
		s.highWater = head.Number
	}
	if head.Number < s.highWater {
		s.cycles++
		s.t.Logger().Info("reorg cycle detected",
			"cycle", s.cycles,
			"head_dropped_to", head.Number,
			"high_water_was", s.highWater,
		)
		s.highWater = head.Number
	}
	return s.cycles
}

// SetupInvalidExecMsgScenario creates a TwoL2SupernodeInterop system, configures
// interop mempool filtering on chain B, sends a valid init message on chain A,
// and sends an INVALID executing message on chain B. Returns the scenario state
// for the test to observe reorg behavior.
func SetupInvalidExecMsgScenario(t devtest.T, filteringEnabled bool) *InvalidExecMsgScenario {
	sys := presets.NewTwoL2SupernodeInterop(t, 0)

	// Configure interop mempool filtering on chain B
	orch := presets.Orchestrator().(*sysgo.Orchestrator)
	opGeth, ok := orch.GetL2EL(sys.L2ELB.ID())
	require.True(t, ok, "must find L2 EL node for chain B")
	gethB := opGeth.(*sysgo.OpGeth)

	sys.L2ELB.Stop()
	gethB.SetInteropMempoolFiltering(&filteringEnabled)
	if filteringEnabled {
		mockEndpoint := StartMockInteropFilter(t)
		gethB.SetSupervisorRPC(mockEndpoint)
	}
	sys.L2ELB.Start()
	sys.L2B.WaitForBlock()

	// Create funded EOAs
	alice := sys.FunderA.NewFundedEOA(eth.OneEther)
	bob := sys.FunderB.NewFundedEOA(eth.OneEther)

	// Deploy event logger on chain A
	eventLoggerA := alice.DeployEventLogger()

	// Sync chains
	sys.L2B.CatchUpTo(sys.L2A)
	sys.L2A.CatchUpTo(sys.L2B)

	rng := rand.New(rand.NewSource(12345))

	// Send valid init message on chain A
	initMsg := alice.SendRandomInitMessage(rng, eventLoggerA, 2, 10)
	t.Logger().Info("initiating message sent on chain A",
		"block", initMsg.BlockNumber(),
		"hash", initMsg.BlockHash(),
	)

	// Wait for chain B to catch up
	sys.L2B.WaitForBlock()

	// Send INVALID executing message on chain B
	execMsg := bob.SendInvalidExecMessage(initMsg)
	invalidBlockNumber := bigs.Uint64Strict(execMsg.BlockNumber())
	invalidBlockTimestamp := sys.L2B.TimestampForBlockNum(invalidBlockNumber)
	t.Logger().Info("invalid executing message sent on chain B",
		"block", invalidBlockNumber,
		"hash", execMsg.BlockHash(),
		"filtering", filteringEnabled,
	)

	return &InvalidExecMsgScenario{
		Sys:                   sys,
		InvalidBlockNumber:    invalidBlockNumber,
		InvalidBlockTimestamp: invalidBlockTimestamp,
		t:                     t,
	}
}

// WaitForCycles waits until at least n reorg cycles are observed.
func (s *InvalidExecMsgScenario) WaitForCycles(n int) {
	require.Eventually(s.t, func() bool {
		return s.CountCycles() >= n
	}, 180*time.Second, 500*time.Millisecond,
		"expected at least %d reorg cycles but only observed %d", n, s.cycles)
}

// WaitForStableChain waits for the supernode to validate several blocks past
// the invalid block's timestamp, proving the chain is stable after the reorg.
func (s *InvalidExecMsgScenario) WaitForStableChain() {
	ctx := s.t.Ctx()
	blockTime := s.Sys.L2A.Escape().RollupConfig().BlockTime
	stableTimestamp := s.InvalidBlockTimestamp + 3*blockTime
	require.Eventually(s.t, func() bool {
		s.CountCycles() // keep counting while we wait
		resp, err := s.Sys.Supernode.Escape().QueryAPI().SuperRootAtTimestamp(ctx, stableTimestamp)
		if err != nil {
			return false
		}
		return resp.Data != nil
	}, 180*time.Second, 500*time.Millisecond,
		"supernode should validate blocks past the replacement")
}
