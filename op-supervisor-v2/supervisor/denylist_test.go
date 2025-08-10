package supervisor

import (
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


