package supervisor

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httputil"
	"net/url"
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

	// managed-node mode
	managedOpNodeUserRPC string
	stopManagedOpNode    func(ctx context.Context) error

	// restart context
	managedCfg *managedConfig

	// if true, HTTP handler exposes an /opnode/ reverse proxy to the embedded op-node user RPC
	enableOpNodeProxy bool

	// denylist
	denylist *DenylistStore
}

type managedConfig struct {
	l1RPC        string
	beaconAddr   string
	l2AuthRPC    string
	l2UserRPC    string
	jwtSecret    [32]byte
	rcfg         *rollup.Config
	interval     time.Duration
	confirmDepth uint64
}

func NewSupervisor(l log.Logger) *Supervisor {
	s := &Supervisor{log: l, denylist: NewDenylistStore("")}
	return s
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
	if s.stopManagedOpNode != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_ = s.stopManagedOpNode(ctx)
		cancel()
		s.stopManagedOpNode = nil
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
		opNodeUser := s.managedOpNodeUserRPC
		s.mu.Unlock()
		_ = json.NewEncoder(w).Encode(map[string]any{
			"op_node_running":  running,
			"started_at":       started,
			"op_node_user_rpc": opNodeUser,
		})
	})
	// dev-only admin rollback endpoint: POST /admin/rollback { back_n_blocks?: uint64 }
	mux.HandleFunc("/admin/rollback", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var req struct {
			BackN uint64 `json:"back_n_blocks"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		if req.BackN == 0 {
			req.BackN = 1
		}
		s.mu.Lock()
		cfg := s.managedCfg
		s.mu.Unlock()
		if cfg == nil {
			http.Error(w, "managed mode not running", http.StatusServiceUnavailable)
			return
		}
		if err := s.performRollback(r.Context(), cfg, req.BackN); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})
	// placeholder denylist check endpoint (will be implemented later)
	mux.HandleFunc("/denylist/v1/check", func(w http.ResponseWriter, r *http.Request) {
		// GET /denylist/v1/check?chainId=&id=
		q := r.URL.Query()
		chainIDStr := q.Get("chainId")
		id := q.Get("id")
		var cid uint64
		if chainIDStr != "" {
			_, _ = fmt.Sscanf(chainIDStr, "%d", &cid)
		}
		deny := s.denylist != nil && id != "" && s.denylist.Has(cid, id)
		_ = json.NewEncoder(w).Encode(map[string]any{"denylisted": deny})
	})
	// Entries are managed internally by supervisor policies/tests; no POST endpoint

	if s.enableOpNodeProxy {
		// Expose embedded op-node user RPC via reverse proxy (HTTP) under /opnode/
		mux.HandleFunc("/opnode/", func(w http.ResponseWriter, r *http.Request) {
			s.mu.Lock()
			target := s.managedOpNodeUserRPC
			s.mu.Unlock()
			if target == "" {
				http.Error(w, "op-node RPC not available", http.StatusServiceUnavailable)
				return
			}
			u, err := url.Parse(target)
			if err != nil {
				http.Error(w, "bad op-node RPC URL", http.StatusInternalServerError)
				return
			}
			rp := httputil.NewSingleHostReverseProxy(u)
			// Trim prefix to forward to root
			r.URL.Path = "/"
			rp.ServeHTTP(w, r)
		})
	}
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
		// Wait for op-node RPC to respond before polling
		for i := 0; i < 20; i++ {
			ctxPing, cancelPing := context.WithTimeout(ctx, 500*time.Millisecond)
			_, err := roll.SyncStatus(ctxPing)
			cancelPing()
			if err == nil {
				break
			}
			time.Sleep(250 * time.Millisecond)
		}
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

// StartManaged spawns an op-node internally and starts polling it.
func (s *Supervisor) StartManaged(l1RPC string, beaconAddr string, l2AuthRPC string, l2UserRPC string, jwtSecret [32]byte, rcfg *rollup.Config, interval time.Duration, confirmDepth uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.cancelPoll != nil {
		return nil
	}
	// Start embedded op-node
	userRPC, stopFn, err := s.StartManagedOpNode(l1RPC, beaconAddr, l2AuthRPC, jwtSecret, rcfg)
	if err != nil {
		return err
	}
	s.managedOpNodeUserRPC = userRPC
	s.stopManagedOpNode = stopFn
	s.managedCfg = &managedConfig{l1RPC: l1RPC, beaconAddr: beaconAddr, l2AuthRPC: l2AuthRPC, l2UserRPC: l2UserRPC, jwtSecret: jwtSecret, rcfg: rcfg, interval: interval, confirmDepth: confirmDepth}

	ctx, cancel := context.WithCancel(context.Background())
	s.cancelPoll = cancel

	// Dial clients
	opNodeCli, err := opclient.NewRPC(ctx, s.log, userRPC)
	if err != nil {
		cancel()
		return err
	}
	// Use user RPC (no JWT) for eth_getLogs/receipts
	l2Cli, err := opclient.NewRPC(ctx, s.log, l2UserRPC)
	if err != nil {
		cancel()
		return err
	}
	l2, err := sources.NewL2Client(l2Cli, s.log, nil, sources.L2ClientDefaultConfig(rcfg, true))
	if err != nil {
		cancel()
		return err
	}
	roll := sources.NewRollupClient(opNodeCli)

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
			}
		}
	}()
	return nil
}

// performRollback stops the managed op-node, rolls back the EL by backN blocks using debug_setHead,
// then restarts the op-node and polling.
func (s *Supervisor) performRollback(ctx context.Context, cfg *managedConfig, backN uint64) error {
	// Stop polling and op-node
	s.mu.Lock()
	if s.cancelPoll != nil {
		s.cancelPoll()
		s.cancelPoll = nil
	}
	stopFn := s.stopManagedOpNode
	s.mu.Unlock()
	if stopFn != nil {
		c, cancel := context.WithTimeout(ctx, 5*time.Second)
		_ = stopFn(c)
		cancel()
	}

    // Roll back EL head by backN via pluggable implementation
    if err := rollbackEL(ctx, cfg.l2UserRPC, backN); err != nil {
		return err
	}

	// Restart managed op-node and polling
	s.mu.Lock()
	userRPC, stopFn2, err := s.StartManagedOpNode(cfg.l1RPC, cfg.beaconAddr, cfg.l2AuthRPC, cfg.jwtSecret, cfg.rcfg)
	if err != nil {
		s.mu.Unlock()
		return err
	}
	s.managedOpNodeUserRPC = userRPC
	s.stopManagedOpNode = stopFn2
	ctxPoll, cancel := context.WithCancel(context.Background())
	s.cancelPoll = cancel
	s.mu.Unlock()

	// Dial clients for polling
	opNodeCli, err := opclient.NewRPC(ctxPoll, s.log, userRPC)
	if err != nil {
		cancel()
		return err
	}
	l2Cli, err := opclient.NewRPC(ctxPoll, s.log, cfg.l2UserRPC)
	if err != nil {
		cancel()
		return err
	}
	l2, err := sources.NewL2Client(l2Cli, s.log, nil, sources.L2ClientDefaultConfig(cfg.rcfg, true))
	if err != nil {
		cancel()
		return err
	}
	roll := sources.NewRollupClient(opNodeCli)

	go func() {
		ticker := time.NewTicker(cfg.interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctxPoll.Done():
				return
			case <-ticker.C:
				st, err := roll.SyncStatus(ctxPoll)
				if err != nil || st == nil {
					s.log.Warn("poll: sync status error", "err", err)
					continue
				}
				localSafe := st.LocalSafeL2
				s.log.Info("poll: heads", "unsafe", st.UnsafeL2, "local_safe", localSafe, "safe", st.SafeL2, "finalized", st.FinalizedL2)
				if localSafe.Number == 0 {
					continue
				}
				if _, _, err := l2.FetchReceiptsByNumber(ctxPoll, localSafe.Number); err != nil {
					s.log.Debug("poll: fetch receipts", "num", localSafe.Number, "err", err)
				}
			}
		}
	}()
	return nil
}

// ManagedOpNodeUserRPC returns the user RPC URL of the embedded op-node if running.
func (s *Supervisor) ManagedOpNodeUserRPC() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.managedOpNodeUserRPC
}

// EnableOpNodeProxy toggles the /opnode/ reverse proxy in the HTTP handler.
func (s *Supervisor) EnableOpNodeProxy(v bool) { s.enableOpNodeProxy = v }
