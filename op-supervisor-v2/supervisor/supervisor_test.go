package supervisor

import (
	"testing"
)

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
