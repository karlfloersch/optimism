package supervisor

import (
    "os"
    "path/filepath"
    "testing"
)

func TestDenylistStore_AddHas(t *testing.T) {
    dl := NewDenylistStore("")
    chainID := uint64(901)
    id := "0xdeadbeef"

    if dl.Has(chainID, id) {
        t.Fatalf("unexpected present before add")
    }
    if err := dl.Add(chainID, id); err != nil {
        t.Fatalf("add: %v", err)
    }
    if !dl.Has(chainID, id) {
        t.Fatalf("expected present after add")
    }
}

func TestDenylistStore_PersistReload(t *testing.T) {
    dir := t.TempDir()
    path := filepath.Join(dir, "nested", "denylist.json")
    cid := uint64(902)
    id1 := "0xaaa"
    id2 := "0xbbb"

    // First instance: add entries and ensure file/dirs created
    dl1 := NewDenylistStore(path)
    if err := dl1.Add(cid, id1); err != nil {
        t.Fatalf("add id1: %v", err)
    }
    if err := dl1.Add(cid, id2); err != nil {
        t.Fatalf("add id2: %v", err)
    }
    if !dl1.Has(cid, id1) || !dl1.Has(cid, id2) {
        t.Fatalf("expected entries present after add")
    }
    if _, err := os.Stat(path); err != nil {
        t.Fatalf("denylist file not created: %v", err)
    }

    // Second instance: reload from same path, entries must persist
    dl2 := NewDenylistStore(path)
    if !dl2.Has(cid, id1) || !dl2.Has(cid, id2) {
        t.Fatalf("expected entries present after reload")
    }
    if dl2.Has(cid, "0xccc") {
        t.Fatalf("unexpected entry present after reload")
    }
}

