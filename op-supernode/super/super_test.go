package super

import (
	"testing"

	"github.com/ethereum-optimism/optimism/op-supernode/super/activities/cross"
	"github.com/ethereum-optimism/optimism/op-supernode/super/chain"
)

func TestCrossFinalizedFromDBOrFallbackZeroOnEmpty(t *testing.T) {
	s := NewSuper(testLogger())
	// Register two chains with no cross DB entries; expect cross-finalized=0
	s.mu.Lock()
	s.chains = cross.ChainDirectory{
		1: chain.NewChainContainer(),
		2: chain.NewChainContainer(),
	}
	s.mu.Unlock()
	if got := s.getCrossFinalized(); got != 0 {
		t.Fatalf("expected crossFinalized 0 on empty DBs, got %d", got)
	}
}
