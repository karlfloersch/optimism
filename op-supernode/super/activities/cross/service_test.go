package cross

import (
	"testing"

	"github.com/ethereum/go-ethereum/log"
)

func TestCrossService_Denylisted(t *testing.T) {
	logger := log.NewLogger(log.DiscardHandler())
	chains := make(ChainDirectory)
	tempDir := t.TempDir()
	s := NewCrossService(logger, chains, tempDir)

	chainID := uint64(901)
	hash := "0xdeadbeef"
	timestamp := uint64(1234567890)

	// Initially should not be denylisted
	if s.Denylisted(chainID, hash) {
		t.Fatalf("unexpected denylisted before adding")
	}

	// Add to denylist
	if err := s.denylist.Add(chainID, timestamp, hash); err != nil {
		t.Fatalf("failed to add to denylist: %v", err)
	}

	// Now should be denylisted
	if !s.Denylisted(chainID, hash) {
		t.Fatalf("expected denylisted after adding")
	}
}
