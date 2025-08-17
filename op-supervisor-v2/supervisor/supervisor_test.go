package supervisor

import (
	"context"
	"testing"

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

func TestCrossFinalizedFromDBOrFallbackZeroOnEmpty(t *testing.T) {
	s := NewSupervisor(testLogger())
	// Register two chains with no cross DB entries; expect cross-finalized=0
	s.mu.Lock()
	s.chains = map[uint64]*chainHandle{1: {}, 2: {}}
	s.mu.Unlock()
	if got := s.getCrossFinalized(); got != 0 {
		t.Fatalf("expected crossFinalized 0 on empty DBs, got %d", got)
	}
}
