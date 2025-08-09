package supervisor

import (
	"context"
	"encoding/json"
	"net/http"
	"os"
	"os/exec"
	"sync"
	"time"

	"github.com/ethereum-optimism/optimism/op-node/rollup"
	"github.com/ethereum-optimism/optimism/op-service/apis"
	opclient "github.com/ethereum-optimism/optimism/op-service/client"
	"github.com/ethereum-optimism/optimism/op-service/sources"
	"github.com/ethereum/go-ethereum/log"
)

type Supervisor struct {
	log     log.Logger
	mu      sync.Mutex
	cmd     *exec.Cmd
	started time.Time

	// polling
	cancelPoll context.CancelFunc
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
	if s.cancelPoll != nil {
		s.cancelPoll()
		s.cancelPoll = nil
	}
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

// StartPolling starts a background loop that polls op-node sync status and fetches the corresponding L2 block and receipts.
func (s *Supervisor) StartPolling(opNodeRPC, l2RPC string, interval time.Duration, confirmDepth uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.cancelPoll != nil {
		return nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	s.cancelPoll = cancel

	// rollup client (op-node)
	opNodeCli, err := opclient.NewRPC(ctx, s.log, opNodeRPC)
	if err != nil {
		cancel()
		return err
	}
	roll := sources.NewRollupClient(opNodeCli)

	// l2 client
	l2Cli, err := opclient.NewRPC(ctx, s.log, l2RPC)
	if err != nil {
		cancel()
		return err
	}
	// get rollup config from op-node to configure l2 client caches sanely
	rcfg, err := roll.RollupConfig(ctx)
	if err != nil {
		cancel()
		return err
	}
	l2, err := sources.NewL2Client(l2Cli, s.log, nil, sources.L2ClientDefaultConfig(rcfg, true))
	if err != nil {
		cancel()
		return err
	}

	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				st, err := roll.SyncStatus(ctx)
				if err != nil || st == nil {
					s.log.Warn("poll: sync status error", "err", err)
					continue
				}
				localSafe := st.LocalSafeL2
				// Only act on sufficiently confirmed L1 scope; for now just log head info.
				s.log.Info("poll: heads", "unsafe", st.UnsafeL2, "local_safe", localSafe, "safe", st.SafeL2, "finalized", st.FinalizedL2)
				if localSafe.Number == 0 {
					continue
				}
				// Fetch payload + receipts of local-safe head for indexing later
				if _, _, err := l2.FetchReceiptsByNumber(ctx, localSafe.Number); err != nil {
					s.log.Debug("poll: fetch receipts error", "num", localSafe.Number, "err", err)
				}
				_ = rcfg
				_ = confirmDepth
			}
		}
	}()

	return nil
}

// StartPollingWithClients is like StartPolling but reuses existing RPC clients and rollup config.
func (s *Supervisor) StartPollingWithClients(opNodeCli opclient.RPC, l2Cli opclient.RPC, rcfg *rollup.Config, interval time.Duration, confirmDepth uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.cancelPoll != nil {
		return nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	s.cancelPoll = cancel

	roll := sources.NewRollupClient(opNodeCli)
	l2, err := sources.NewL2Client(l2Cli, s.log, nil, sources.L2ClientDefaultConfig(rcfg, true))
	if err != nil {
		cancel()
		return err
	}

	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				st, err := roll.SyncStatus(ctx)
				if err != nil || st == nil {
					s.log.Warn("poll: sync status error", "err", err)
					continue
				}
				localSafe := st.LocalSafeL2
				s.log.Info("poll: heads", "unsafe", st.UnsafeL2, "local_safe", localSafe, "safe", st.SafeL2, "finalized", st.FinalizedL2)
				if localSafe.Number == 0 {
					continue
				}
				if _, _, err := l2.FetchReceiptsByNumber(ctx, localSafe.Number); err != nil {
					s.log.Debug("poll: fetch receipts", "num", localSafe.Number, "err", err)
				}
				_ = confirmDepth
			}
		}
	}()
	return nil
}

// StartPollingWithRollupClient starts polling using a provided RollupClient and L2 RPC client.
func (s *Supervisor) StartPollingWithRollupClient(roll apis.RollupClient, l2Cli opclient.RPC, rcfg *rollup.Config, interval time.Duration, confirmDepth uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.cancelPoll != nil {
		return nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	s.cancelPoll = cancel

	l2, err := sources.NewL2Client(l2Cli, s.log, nil, sources.L2ClientDefaultConfig(rcfg, true))
	if err != nil {
		cancel()
		return err
	}

	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				st, err := roll.SyncStatus(ctx)
				if err != nil || st == nil {
					s.log.Warn("poll: sync status error", "err", err)
					continue
				}
				localSafe := st.LocalSafeL2
				s.log.Info("poll: heads", "unsafe", st.UnsafeL2, "local_safe", localSafe, "safe", st.SafeL2, "finalized", st.FinalizedL2)
				if localSafe.Number == 0 {
					continue
				}
				if _, _, err := l2.FetchReceiptsByNumber(ctx, localSafe.Number); err != nil {
					s.log.Debug("poll: fetch receipts", "num", localSafe.Number, "err", err)
				}
				_ = confirmDepth
			}
		}
	}()
	return nil
}
