package cross

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/ethereum-optimism/optimism/op-service/eth"
	"github.com/ethereum-optimism/optimism/op-supervisor/supervisor/types"
)

// crossSafeMD contains metadata for a cross-safe timestamp entry
type crossSafeMD struct {
	Timestamp uint64                               `json:"timestamp"`
	L1Block   eth.BlockRef                         `json:"l1_block"`  // Latest L1 block (highest number)
	L2Blocks  map[uint64]types.DerivedBlockRefPair `json:"l2_blocks"` // chainID -> L2 block pair
}

// ============================================================================
// Cross-Safe History Management
// ============================================================================

// getCurrentCrossSafeTimestamp returns the latest cross-safe timestamp, or 0 if none exists
func (s *CrossService) getCurrentCrossSafeTimestamp() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.crossSafeHistory) == 0 {
		return 0
	}
	return s.crossSafeHistory[len(s.crossSafeHistory)-1].Timestamp
}

// getLatestCrossSafe returns the latest cross-safe entry, or nil if none exists
func (s *CrossService) getLatestCrossSafe() *crossSafeMD {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.crossSafeHistory) == 0 {
		return nil
	}
	return &s.crossSafeHistory[len(s.crossSafeHistory)-1]
}

// addCrossSafeEntry adds a new cross-safe entry to the history
func (s *CrossService) addCrossSafeEntry(entry crossSafeMD) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.crossSafeHistory = append(s.crossSafeHistory, entry)
	if err := s.saveCrossSafeHistory(); err != nil {
		s.log.Warn("failed to save cross-safe history", "err", err)
	}
}

// setCrossSafeTimestamp sets the cross-safe history to a single entry with the given timestamp (for initialization)
func (s *CrossService) setCrossSafeTimestamp(timestamp uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.crossSafeHistory = []crossSafeMD{{Timestamp: timestamp}}
	if err := s.saveCrossSafeHistory(); err != nil {
		s.log.Warn("failed to save cross-safe history", "err", err)
	}
}

// pruneLatestCrossSafeEntry removes the latest entry from crossSafeHistory and returns the new latest timestamp
func (s *CrossService) pruneLatestCrossSafeEntry() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.crossSafeHistory) == 0 {
		return 0
	}
	if len(s.crossSafeHistory) == 1 {
		// If only one entry, clear the history
		s.crossSafeHistory = nil
		if err := s.saveCrossSafeHistory(); err != nil {
			s.log.Warn("failed to save cross-safe history", "err", err)
		}
		return 0
	}
	// Remove the last entry
	s.crossSafeHistory = s.crossSafeHistory[:len(s.crossSafeHistory)-1]
	if err := s.saveCrossSafeHistory(); err != nil {
		s.log.Warn("failed to save cross-safe history", "err", err)
	}
	// Return the new latest timestamp
	return s.crossSafeHistory[len(s.crossSafeHistory)-1].Timestamp
}

// loadCrossSafeHistory loads the cross-safe history from the persistent file
func (s *CrossService) loadCrossSafeHistory() error {
	if s.crossSafeHistoryFile == "" {
		return nil // no file configured
	}

	data, err := os.ReadFile(s.crossSafeHistoryFile)
	if err != nil {
		if os.IsNotExist(err) {
			// File doesn't exist yet, start with empty history
			s.crossSafeHistory = nil
			return nil
		}
		return fmt.Errorf("failed to read cross-safe history file: %w", err)
	}

	if len(data) == 0 {
		// Empty file, start with empty history
		s.crossSafeHistory = nil
		return nil
	}

	var history []crossSafeMD
	if err := json.Unmarshal(data, &history); err != nil {
		return fmt.Errorf("failed to unmarshal cross-safe history: %w", err)
	}

	s.crossSafeHistory = history
	s.log.Info("loaded cross-safe history from file", "entries", len(history), "file", s.crossSafeHistoryFile)
	return nil
}

// saveCrossSafeHistory persists the current cross-safe history to file
func (s *CrossService) saveCrossSafeHistory() error {
	if s.crossSafeHistoryFile == "" {
		return nil // no file configured
	}

	// Ensure the directory exists
	dir := filepath.Dir(s.crossSafeHistoryFile)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("failed to create directory for cross-safe history file: %w", err)
	}

	data, err := json.Marshal(s.crossSafeHistory)
	if err != nil {
		return fmt.Errorf("failed to marshal cross-safe history: %w", err)
	}

	if err := os.WriteFile(s.crossSafeHistoryFile, data, 0o644); err != nil {
		return fmt.Errorf("failed to write cross-safe history file: %w", err)
	}

	return nil
}
