package supervisor

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/ethereum-optimism/optimism/op-service/eth"
	"github.com/ethereum-optimism/optimism/op-supervisor/supervisor/types"
	"github.com/ethereum/go-ethereum/log"
)

func TestCrossFinalizedFromDBOrFallbackZeroOnEmpty(t *testing.T) {
	s := NewSupervisor(testLogger())
	// Register two chains with no cross DB entries; expect cross-finalized=0
	s.mu.Lock()
	s.chains = map[uint64]*ChainContainer{1: {}, 2: {}}
	s.mu.Unlock()
	if got := s.getCrossFinalized(); got != 0 {
		t.Fatalf("expected crossFinalized 0 on empty DBs, got %d", got)
	}
}

func TestCrossSafeHistoryPersistence(t *testing.T) {
	// Create a temporary directory for testing
	tempDir, err := os.MkdirTemp("", "supervisor-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Create supervisor and set data dir
	logger := log.New()
	s := NewSupervisor(logger)
	s.SetDataDir(tempDir)

	// Verify initial state is empty
	if len(s.crossSafeHistory) != 0 {
		t.Errorf("Expected empty history initially, got %d entries", len(s.crossSafeHistory))
	}

	// Add some test entries
	entry1 := crossSafeMD{
		Timestamp: 1000,
		L1Block: eth.BlockRef{
			Hash:   [32]byte{0x01},
			Number: 100,
		},
		L2Blocks: map[uint64]types.DerivedBlockRefPair{
			1: {
				Source:  eth.BlockRef{Hash: [32]byte{0x01}, Number: 100},
				Derived: eth.BlockRef{Hash: [32]byte{0x02}, Number: 200},
			},
		},
	}

	entry2 := crossSafeMD{
		Timestamp: 2000,
		L1Block: eth.BlockRef{
			Hash:   [32]byte{0x03},
			Number: 101,
		},
		L2Blocks: map[uint64]types.DerivedBlockRefPair{
			1: {
				Source:  eth.BlockRef{Hash: [32]byte{0x03}, Number: 101},
				Derived: eth.BlockRef{Hash: [32]byte{0x04}, Number: 201},
			},
		},
	}

	s.addCrossSafeEntry(entry1)
	s.addCrossSafeEntry(entry2)

	// Verify entries were added
	if len(s.crossSafeHistory) != 2 {
		t.Errorf("Expected 2 entries, got %d", len(s.crossSafeHistory))
	}

	// Verify file was created
	historyFile := filepath.Join(tempDir, "crossSafeHistory.json")
	if _, err := os.Stat(historyFile); os.IsNotExist(err) {
		t.Error("History file was not created")
	}

	// Create a new supervisor with the same data dir to test loading
	s2 := NewSupervisor(logger)
	s2.SetDataDir(tempDir)

	// Verify the history was loaded correctly
	if len(s2.crossSafeHistory) != 2 {
		t.Errorf("Expected 2 entries after loading, got %d", len(s2.crossSafeHistory))
	}

	if s2.crossSafeHistory[0].Timestamp != 1000 {
		t.Errorf("Expected first entry timestamp 1000, got %d", s2.crossSafeHistory[0].Timestamp)
	}

	if s2.crossSafeHistory[1].Timestamp != 2000 {
		t.Errorf("Expected second entry timestamp 2000, got %d", s2.crossSafeHistory[1].Timestamp)
	}

	// Test pruning
	newTimestamp := s2.pruneLatestCrossSafeEntry()
	if newTimestamp != 1000 {
		t.Errorf("Expected pruned timestamp 1000, got %d", newTimestamp)
	}

	if len(s2.crossSafeHistory) != 1 {
		t.Errorf("Expected 1 entry after pruning, got %d", len(s2.crossSafeHistory))
	}

	// Create a third supervisor to verify pruning was persisted
	s3 := NewSupervisor(logger)
	s3.SetDataDir(tempDir)

	if len(s3.crossSafeHistory) != 1 {
		t.Errorf("Expected 1 entry after loading pruned history, got %d", len(s3.crossSafeHistory))
	}

	if s3.crossSafeHistory[0].Timestamp != 1000 {
		t.Errorf("Expected remaining entry timestamp 1000, got %d", s3.crossSafeHistory[0].Timestamp)
	}
}

func TestCrossSafeHistoryPersistenceEmptyFile(t *testing.T) {
	// Create a temporary directory for testing
	tempDir, err := os.MkdirTemp("", "supervisor-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Create an empty history file
	historyFile := filepath.Join(tempDir, "crossSafeHistory.json")
	if err := os.WriteFile(historyFile, []byte(""), 0644); err != nil {
		t.Fatalf("Failed to create empty file: %v", err)
	}

	// Create supervisor and set data dir
	logger := log.New()
	s := NewSupervisor(logger)
	s.SetDataDir(tempDir)

	// Verify it handles empty file correctly
	if len(s.crossSafeHistory) != 0 {
		t.Errorf("Expected empty history with empty file, got %d entries", len(s.crossSafeHistory))
	}
}
