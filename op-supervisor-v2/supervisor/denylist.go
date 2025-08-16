package supervisor

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// DenylistStore is a minimal persisted denylist keyed by chainID and an ID (payloadID or block hash).
type DenylistStore struct {
	mu   sync.Mutex
	path string
	data map[uint64]map[string]struct{}
}

func NewDenylistStore(path string) *DenylistStore {
	dl := &DenylistStore{path: path, data: make(map[uint64]map[string]struct{})}
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
	var tmp map[string][]string
	if err := json.Unmarshal(b, &tmp); err != nil {
		return nil
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	for k, vals := range tmp {
		// parse chainID in base-10
		var cid uint64
		_, _ = fmt.Sscanf(k, "%d", &cid)
		if _, ok := d.data[cid]; !ok {
			d.data[cid] = make(map[string]struct{})
		}
		for _, v := range vals {
			d.data[cid][v] = struct{}{}
		}
	}
	return nil
}

func (d *DenylistStore) persist() error {
	if d.path == "" {
		return nil
	}
	_ = os.MkdirAll(filepath.Dir(d.path), 0o755)
	out := make(map[string][]string)
	d.mu.Lock()
	for cid, set := range d.data {
		key := fmt.Sprintf("%d", cid)
		for v := range set {
			out[key] = append(out[key], v)
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

func (d *DenylistStore) Add(chainID uint64, id string) error {
	d.mu.Lock()
	if _, ok := d.data[chainID]; !ok {
		d.data[chainID] = make(map[string]struct{})
	}
	d.data[chainID][id] = struct{}{}
	d.mu.Unlock()
	return d.persist()
}

func (d *DenylistStore) Has(chainID uint64, id string) bool {
	d.mu.Lock()
	_, ok := d.data[chainID][id]
	d.mu.Unlock()
	return ok
}
