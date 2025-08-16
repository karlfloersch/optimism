package supervisor

import (
	"context"
	"testing"
	"time"

	"github.com/ethereum-optimism/optimism/op-service/eth"
)

// fake status producer for tests
func fakeFetcher(seq []uint64) func(ctx context.Context, rpc string) (*eth.SyncStatus, error) {
	idx := 0
	return func(ctx context.Context, rpc string) (*eth.SyncStatus, error) {
		n := seq[idx%len(seq)]
		idx++
		return &eth.SyncStatus{FinalizedL2: eth.L2BlockRef{Number: n}}, nil
	}
}

type noopChecker struct {
	calls int
	last  Snapshot
}

func (n *noopChecker) Evaluate(ctx context.Context, snap Snapshot) ([]Proposal, error) {
	n.calls++
	n.last = snap
	return nil, nil
}

func TestFinalizedRunnerComputesMinAndCallsCheckers(t *testing.T) {
	s := NewSupervisor(testLogger())
	// speed up runner
	s.runnerInterval = 10 * time.Millisecond
	// ensure label is finalized for determinism
	s.SetL1ScopeLabel(eth.Finalized)
	// inject fake fetcher returning finalized heights per RPC
	s.fetchSyncStatus = func(ctx context.Context, rpc string) (*eth.SyncStatus, error) {
		var n uint64 = 10
		if rpc == "rpc2" {
			n = 12
		}
		return &eth.SyncStatus{FinalizedL2: eth.L2BlockRef{Number: n}}, nil
	}
	// register two chains directly into supervisor (simulate AddChain effect)
	s.mu.Lock()
	s.chains = map[uint64]*chainHandle{
		1: {embeddedOpNodeUserRPC: "rpc1"},
		2: {embeddedOpNodeUserRPC: "rpc2"},
	}
	s.mu.Unlock()
	// enable proposals so checkers are called in the loop
	t.Setenv("SV2_ENABLE_CHECKERS", "true")
	// register a checker
	chk := &noopChecker{}
	s.RegisterChecker(chk)
	// start runner
	s.maybeStartFinalizedRunner()
	// wait and poll for at least one tick
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		cf := s.getCrossFinalized()
		if cf != 0 && chk.calls > 0 {
			break
		}
		t.Logf("tick: cross_finalized=%d calls=%d", s.getCrossFinalized(), chk.calls)
		time.Sleep(10 * time.Millisecond)
	}
	// verify crossFinalized is min(10,12)=10 and checker saw a snapshot with that value
	if got := s.getCrossFinalized(); got != 10 {
		t.Fatalf("expected crossFinalized 10, got %d", got)
	}
	if chk.calls == 0 || chk.last.CrossFinalized != 10 {
		t.Fatalf("checker not called or wrong snapshot: calls=%d cross=%d", chk.calls, chk.last.CrossFinalized)
	}
}
