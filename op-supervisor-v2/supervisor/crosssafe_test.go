package supervisor

import (
	"os"
	"path/filepath"
	"testing"
)

// Smoke that openChainDBs creates files under a temp dir
func TestOpenChainDBsCreatesFiles(t *testing.T) {
	s := NewSupervisor(testLogger())
	dir := t.TempDir()
	logs, local, cross, err := s.openChainDBs(testLogger(), 1, dir)
	if err != nil {
		t.Fatalf("open dbs: %v", err)
	}
	defer logs.Close()
	defer local.Close()
	defer cross.Close()
	// Ensure files exist
	for _, p := range []string{
		filepath.Join(dir, "logs-1"),
		filepath.Join(dir, "local-1"),
		filepath.Join(dir, "cross-1"),
	} {
		if _, err := os.Stat(p); err != nil {
			t.Fatalf("expected file exists: %s: %v", p, err)
		}
	}
}
