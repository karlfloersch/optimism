package super

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// DenylistStore is a minimal persisted denylist keyed by chainID and block timestamp, storing block hashes.
type DenylistStore struct {
	mu   sync.Mutex
	path string
	data map[uint64]map[uint64]string
}

func NewDenylistStore(path string) *DenylistStore {
	dl := &DenylistStore{path: path, data: make(map[uint64]map[uint64]string)}
	_ = dl.load()
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
	d.mu.Lock()
	if _, ok := d.data[chainID]; !ok {
		d.data[chainID] = make(map[uint64]string)
	}
	d.data[chainID][timestamp] = hash
	d.mu.Unlock()
	return d.persist()
}

func (d *DenylistStore) Has(chainID uint64, id string) bool {
	d.mu.Lock()
	defer d.mu.Unlock()

	timestampToHash, exists := d.data[chainID]
	if !exists {
		return false
	}

	// Iterate over all entries for this chainID to find matching hash
	for _, hash := range timestampToHash {
		if hash == id {
			return true
		}
	}
	return false
}

// PruneAtOrNewerThan removes all entries with timestamps at or newer than the given timestamp
func (d *DenylistStore) PruneAtOrNewerThan(timestamp uint64) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	for chainID, timestampToHash := range d.data {
		for ts := range timestampToHash {
			if ts >= timestamp {
				delete(timestampToHash, ts)
			}
		}
		// Clean up empty chain entries
		if len(timestampToHash) == 0 {
			delete(d.data, chainID)
		}
	}

	return d.persist()
}
