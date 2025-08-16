package supervisor

import (
	"context"
	"testing"
	"time"

	"github.com/ethereum-optimism/optimism/op-service/eth"
)

type proposalChecker struct{ props []Proposal }

func (p *proposalChecker) Evaluate(ctx context.Context, snap Snapshot) ([]Proposal, error) {
	return p.props, nil
}

func TestProposalExecutorAddsDenylistAndCallsRollback(t *testing.T) {
	s := NewSupervisor(testLogger())
	// speed up
	s.runnerInterval = 10 * time.Millisecond
	// enable executing proposals
	t.Setenv("SV2_ENABLE_CHECKERS", "true")
	// deterministic label
	s.SetL1ScopeLabel(eth.Finalized)
	// fake sync status: finalized=10 for both chains
	s.fetchSyncStatus = func(ctx context.Context, rpc string) (*eth.SyncStatus, error) {
		return &eth.SyncStatus{FinalizedL2: eth.L2BlockRef{Number: 10}}, nil
	}
	// register two chains (minimal)
	s.mu.Lock()
	s.chains = map[uint64]*chainHandle{1: {embeddedOpNodeUserRPC: "rpc1"}, 2: {embeddedOpNodeUserRPC: "rpc2"}}
	s.mu.Unlock()
	// capture rollback calls
	var called bool
	var callChain uint64
	var callTo uint64
	s.rollbackFn = func(ctx context.Context, chainID uint64, toBlock uint64) error {
		called = true
		callChain = chainID
		callTo = toBlock
		return nil
	}
	// register checker that proposes one action on chain 1
	s.RegisterChecker(&proposalChecker{props: []Proposal{{ChainID: 1, PayloadID: "0xabc", ToBlock: 9, Reason: "test"}}})
	// start runner
	s.maybeStartFinalizedRunner()
	// wait/poll up to 2s for runner to process
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if s.denylist.Has(1, "0xabc") && called {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	// assert denylist updated
	if !s.denylist.Has(1, "0xabc") {
		t.Fatalf("denylist entry not added")
	}
	// assert rollback called with expected args
	if !called || callChain != 1 || callTo != 9 {
		t.Fatalf("rollback not called as expected: called=%v chain=%d to=%d", called, callChain, callTo)
	}
}
