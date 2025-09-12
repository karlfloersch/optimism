package supervisor

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/ethereum/go-ethereum/log"
)

// DenylistStore is a minimal persisted denylist keyed by chainID and block timestamp, storing block hashes.
type DenylistStore struct {
	mu     sync.Mutex
	path   string
	data   map[uint64]map[uint64]string
	logger log.Logger
}

func NewDenylistStore(path string, logger log.Logger) *DenylistStore {
	dl := &DenylistStore{
		path:   path,
		data:   make(map[uint64]map[uint64]string),
		logger: logger.New("component", "denylist"),
	}
	if err := dl.load(); err != nil {
		dl.logger.Debug("failed to load denylist from file", "path", path, "error", err)
	} else if path != "" {
		dl.logger.Debug("denylist loaded from file", "path", path, "chain_count", len(dl.data))
	}
	return dl
}

func (d *DenylistStore) load() error {
	if d.path == "" {
		return nil
	}
	b, err := os.ReadFile(d.path)
	if err != nil {
		return nil
	}
	var tmp map[string]map[string]string
	if err := json.Unmarshal(b, &tmp); err != nil {
		return nil
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	for k, timestampToHash := range tmp {
		// parse chainID in base-10
		var cid uint64
		_, _ = fmt.Sscanf(k, "%d", &cid)
		if _, ok := d.data[cid]; !ok {
			d.data[cid] = make(map[uint64]string)
		}
		for tsStr, hash := range timestampToHash {
			var ts uint64
			_, _ = fmt.Sscanf(tsStr, "%d", &ts)
			d.data[cid][ts] = hash
		}
	}
	return nil
}

func (d *DenylistStore) persist() error {
	if d.path == "" {
		return nil
	}
	_ = os.MkdirAll(filepath.Dir(d.path), 0o755)
	out := make(map[string]map[string]string)
	d.mu.Lock()
	for cid, timestampToHash := range d.data {
		key := fmt.Sprintf("%d", cid)
		out[key] = make(map[string]string)
		for ts, hash := range timestampToHash {
			tsKey := fmt.Sprintf("%d", ts)
			out[key][tsKey] = hash
		}
	}
	d.mu.Unlock()
	b, _ := json.MarshalIndent(out, "", "  ")
	// atomic write: write to temp and rename
	tmp := d.path + ".tmp"
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, d.path)
}

func (d *DenylistStore) Add(chainID uint64, timestamp uint64, hash string) error {
	d.logger.Debug("adding block to denylist",
		"chain_id", chainID,
		"timestamp", timestamp,
		"block_hash", hash,
		"security_event", "denylist_add")

	d.mu.Lock()
	if _, ok := d.data[chainID]; !ok {
		d.data[chainID] = make(map[uint64]string)
	}
	d.data[chainID][timestamp] = hash
	totalEntries := len(d.data[chainID])
	d.mu.Unlock()

	if err := d.persist(); err != nil {
		d.logger.Debug("failed to persist denylist after add",
			"chain_id", chainID,
			"error", err)
		return err
	}

	d.logger.Debug("denylist updated successfully",
		"chain_id", chainID,
		"total_entries", totalEntries)
	return nil
}

func (d *DenylistStore) Has(chainID uint64, id string) bool {
	d.mu.Lock()
	defer d.mu.Unlock()

	timestampToHash, exists := d.data[chainID]
	if !exists {
		d.logger.Debug("denylist query - chain not found", "chain_id", chainID, "block_hash", id)
		return false
	}

	// Iterate over all entries for this chainID to find matching hash
	for _, hash := range timestampToHash {
		if hash == id {
			d.logger.Debug("denylist hit detected",
				"chain_id", chainID,
				"block_hash", id,
				"security_event", "denylist_hit")
			return true
		}
	}

	d.logger.Debug("denylist query - block not found", "chain_id", chainID, "block_hash", id)
	return false
}

// PruneAtOrNewerThan removes all entries with timestamps at or newer than the given timestamp
func (d *DenylistStore) PruneAtOrNewerThan(timestamp uint64) error {
	d.logger.Debug("starting denylist pruning", "prune_timestamp", timestamp)

	d.mu.Lock()
	defer d.mu.Unlock()

	prunedCount := 0
	chainsAffected := 0

	for chainID, timestampToHash := range d.data {
		chainPruned := 0
		for ts := range timestampToHash {
			if ts >= timestamp {
				delete(timestampToHash, ts)
				chainPruned++
				prunedCount++
			}
		}

		if chainPruned > 0 {
			chainsAffected++
			d.logger.Debug("pruned entries from chain",
				"chain_id", chainID,
				"pruned_count", chainPruned,
				"remaining_count", len(timestampToHash))
		}

		// Clean up empty chain entries
		if len(timestampToHash) == 0 {
			delete(d.data, chainID)
		}
	}

	if err := d.persist(); err != nil {
		d.logger.Debug("failed to persist denylist after pruning", "error", err)
		return err
	}

	d.logger.Debug("denylist pruning completed",
		"total_pruned", prunedCount,
		"chains_affected", chainsAffected,
		"remaining_chains", len(d.data))
	return nil
}
