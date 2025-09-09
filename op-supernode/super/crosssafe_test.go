package super

import (
	"os"
	"path/filepath"
	"testing"
)

// Smoke that openLogsDB creates logs file under a temp dir
func TestOpenLogsDBCreatesFile(t *testing.T) {
	s := NewSuper(testLogger())
	dir := t.TempDir()
	logs, err := s.openLogsDB(testLogger(), 1, dir)
	if err != nil {
		t.Fatalf("open logs db: %v", err)
	}
	defer logs.Close()
	// Ensure logs file exists
	logsPath := filepath.Join(dir, "logs-1")
	if _, err := os.Stat(logsPath); err != nil {
		t.Fatalf("expected file exists: %s: %v", logsPath, err)
	}
}
