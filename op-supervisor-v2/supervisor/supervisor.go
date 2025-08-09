package supervisor

import (
    "context"
    "encoding/json"
    "net/http"
    "os"
    "os/exec"
    "sync"
    "time"

    "github.com/ethereum/go-ethereum/log"
)

type Supervisor struct {
    log      log.Logger
    mu       sync.Mutex
    cmd      *exec.Cmd
    started  time.Time
}

func NewSupervisor(l log.Logger) *Supervisor {
    return &Supervisor{log: l}
}

func (s *Supervisor) StartOpNode(binary string, args ...string) error {
    s.mu.Lock()
    defer s.mu.Unlock()
    if s.cmd != nil {
        return nil
    }
    cmd := exec.Command(binary, args...)
    if err := cmd.Start(); err != nil {
        return err
    }
    s.cmd = cmd
    s.started = time.Now()
    go func() {
        _ = cmd.Wait()
        s.mu.Lock()
        s.cmd = nil
        s.mu.Unlock()
    }()
    return nil
}

func (s *Supervisor) Stop() {
    s.mu.Lock()
    defer s.mu.Unlock()
    if s.cmd != nil && s.cmd.Process != nil {
        _ = s.cmd.Process.Kill()
        s.cmd = nil
    }
}

func (s *Supervisor) HTTPHandler() http.Handler {
    mux := http.NewServeMux()
    mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
        w.WriteHeader(http.StatusOK)
        _, _ = w.Write([]byte("ok"))
    })
    mux.HandleFunc("/status", func(w http.ResponseWriter, r *http.Request) {
        s.mu.Lock()
        running := s.cmd != nil
        started := s.started
        s.mu.Unlock()
        _ = json.NewEncoder(w).Encode(map[string]any{
            "op_node_running": running,
            "started_at":      started,
        })
    })
    // placeholder denylist check endpoint (will be implemented later)
    mux.HandleFunc("/denylist/v1/check", func(w http.ResponseWriter, r *http.Request) {
        _ = json.NewEncoder(w).Encode(map[string]any{"denylisted": false})
    })
    return mux
}

// helper for future graceful shutdowns of subprocess
func terminateProcess(ctx context.Context, cmd *exec.Cmd) error {
    if cmd == nil || cmd.Process == nil {
        return nil
    }
    _ = cmd.Process.Signal(os.Interrupt)
    done := make(chan struct{})
    go func() {
        _ = cmd.Wait()
        close(done)
    }()
    select {
    case <-ctx.Done():
        return ctx.Err()
    case <-done:
        return nil
    }
}


