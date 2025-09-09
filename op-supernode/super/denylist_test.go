package super

import (
	"os"
	"path/filepath"
	"testing"
)

func TestDenylistStore_AddHas(t *testing.T) {
	dl := NewDenylistStore("")
	chainID := uint64(901)
	timestamp := uint64(1234567890)
	hash := "0xdeadbeef"

	if dl.Has(chainID, hash) {
		t.Fatalf("unexpected present before add")
	}
	if err := dl.Add(chainID, timestamp, hash); err != nil {
		t.Fatalf("add: %v", err)
	}
	if !dl.Has(chainID, hash) {
		t.Fatalf("expected present after add")
	}
}

func TestDenylistStore_PersistReload(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "nested", "denylist.json")
	cid := uint64(902)
	ts1 := uint64(1000000000)
	ts2 := uint64(2000000000)
	hash1 := "0xaaa"
	hash2 := "0xbbb"

	// First instance: add entries and ensure file/dirs created
	dl1 := NewDenylistStore(path)
	if err := dl1.Add(cid, ts1, hash1); err != nil {
		t.Fatalf("add hash1: %v", err)
	}
	if err := dl1.Add(cid, ts2, hash2); err != nil {
		t.Fatalf("add hash2: %v", err)
	}
	if !dl1.Has(cid, hash1) || !dl1.Has(cid, hash2) {
		t.Fatalf("expected entries present after add")
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("denylist file not created: %v", err)
	}

	// Second instance: reload from same path, entries must persist
	dl2 := NewDenylistStore(path)
	if !dl2.Has(cid, hash1) || !dl2.Has(cid, hash2) {
		t.Fatalf("expected entries present after reload")
	}
	if dl2.Has(cid, "0xccc") {
		t.Fatalf("unexpected entry present after reload")
	}
}

func TestDenylistStore_PruneAtOrNewerThan(t *testing.T) {
	dl := NewDenylistStore("")
	chainID := uint64(903)

	// Add entries with different timestamps
	oldTimestamp := uint64(1000000000)
	midTimestamp := uint64(1500000000)
	newTimestamp := uint64(2000000000)
	pruneTimestamp := uint64(1500000000) // At midTimestamp

	oldHash := "0xold"
	midHash := "0xmid"
	newHash := "0xnew"

	// Add all entries
	if err := dl.Add(chainID, oldTimestamp, oldHash); err != nil {
		t.Fatalf("add old entry: %v", err)
	}
	if err := dl.Add(chainID, midTimestamp, midHash); err != nil {
		t.Fatalf("add mid entry: %v", err)
	}
	if err := dl.Add(chainID, newTimestamp, newHash); err != nil {
		t.Fatalf("add new entry: %v", err)
	}

	// Verify all are present
	if !dl.Has(chainID, oldHash) || !dl.Has(chainID, midHash) || !dl.Has(chainID, newHash) {
		t.Fatalf("expected all entries present before pruning")
	}

	// Prune entries at or newer than pruneTimestamp
	if err := dl.PruneAtOrNewerThan(pruneTimestamp); err != nil {
		t.Fatalf("prune: %v", err)
	}

	// Verify old entry remains, mid and new entries are gone
	if !dl.Has(chainID, oldHash) {
		t.Fatalf("expected old entry to remain")
	}
	if dl.Has(chainID, midHash) {
		t.Fatalf("expected mid entry to be pruned (at timestamp)")
	}
	if dl.Has(chainID, newHash) {
		t.Fatalf("expected new entry to be pruned (newer than timestamp)")
	}
}
