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
	"strings"
	"sync"
	"time"

	"github.com/ethereum-optimism/optimism/op-node/params"
	"github.com/ethereum-optimism/optimism/op-node/rollup"
	"github.com/ethereum-optimism/optimism/op-service/apis"
	opclient "github.com/ethereum-optimism/optimism/op-service/client"
	"github.com/ethereum-optimism/optimism/op-service/eth"
	"github.com/ethereum-optimism/optimism/op-service/sources"
	"github.com/ethereum-optimism/optimism/op-supervisor/supervisor/backend/depset"
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

	// multi-chain management
	chains         map[uint64]*chainHandle
	primaryChainID uint64

	// shared linker across all chains for cross-safety checks
	linkMu       sync.Mutex
	linkChains   map[eth.ChainID]struct{}
	expiryWindow uint64
	linkChecker  depset.LinkChecker

	// L1 scope label used for cross-safe L1 confirmation depth gating (default: eth.Safe; tests may override to eth.Unsafe)
	l1ScopeLabel eth.BlockLabel

	// per-instance data directory for DBs
	dataDir string

	// finalized runner state
	cancelFinalized context.CancelFunc
	crossFinalized  uint64

	// checker registry (evaluated at cross-finalized)
	checkers []BlockValidityChecker

	// testability hooks
	fetchSyncStatus func(ctx context.Context, rpc string) (*eth.SyncStatus, error)
	runnerInterval  time.Duration
	rollbackFn      func(ctx context.Context, chainID uint64, toBlock uint64) error
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
	// initialize shared linker state
	s.linkChains = make(map[eth.ChainID]struct{})
	s.expiryWindow = params.MessageExpiryTimeSecondsInterop
	s.linkChecker = depset.LinkCheckFn(func(execInChain eth.ChainID, execTs uint64, initChain eth.ChainID, initTs uint64) bool {
		// with no chains registered yet, nothing can execute
		if _, ok := s.linkChains[execInChain]; !ok {
			return false
		}
		if _, ok := s.linkChains[initChain]; !ok {
			return false
		}
		if initTs > execTs {
			return false
		}
		// expiry check
		if execTs > initTs+s.expiryWindow {
			return false
		}
		return true
	})
	// default scope label; can be overridden via env SV2_L1_SCOPE
	s.l1ScopeLabel = eth.Safe
	switch strings.ToLower(os.Getenv("SV2_L1_SCOPE")) {
	case "unsafe":
		s.l1ScopeLabel = eth.Unsafe
	case "safe":
		s.l1ScopeLabel = eth.Safe
	case "finalized":
		s.l1ScopeLabel = eth.Finalized
	}
	// default fetcher dials the op-node and returns SyncStatus
	s.fetchSyncStatus = func(ctx context.Context, rpc string) (*eth.SyncStatus, error) {
		cli, err := opclient.NewRPC(ctx, s.log, rpc)
		if err != nil {
			return nil, err
		}
		defer cli.Close()
		roll := sources.NewRollupClient(cli)
		return roll.SyncStatus(ctx)
	}
	// runner interval (tunable for tests)
	s.runnerInterval = 500 * time.Millisecond
	if ms := os.Getenv("SV2_RUNNER_INTERVAL_MS"); ms != "" {
		if v, err := time.ParseDuration(ms + "ms"); err == nil {
			s.runnerInterval = v
		}
	}
	// rollback indirection for tests
	s.rollbackFn = s.RollbackChain

	// unique temp dir per instance
	s.dataDir = fmt.Sprintf("%s/sv2-%d-%d", os.TempDir(), os.Getpid(), time.Now().UnixNano())
	return s
}

// getDataDir returns the base data directory for chain DBs
func (s *Supervisor) getDataDir() string { return s.dataDir }

// registerChainForLinker registers a chain ID into the shared linker set.
func (s *Supervisor) registerChainForLinker(id eth.ChainID) {
	s.linkMu.Lock()
	defer s.linkMu.Unlock()
	if s.linkChains == nil {
		s.linkChains = make(map[eth.ChainID]struct{})
	}
	s.linkChains[id] = struct{}{}
}

// getLinker returns the shared LinkChecker.
func (s *Supervisor) getLinker() depset.LinkChecker {
	s.linkMu.Lock()
	defer s.linkMu.Unlock()
	return s.linkChecker
}

// SetL1ScopeLabel overrides the L1 scope label (e.g., eth.Unsafe in tests).
func (s *Supervisor) SetL1ScopeLabel(label eth.BlockLabel) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.l1ScopeLabel = label
}

func (s *Supervisor) getL1ScopeLabel() eth.BlockLabel {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.l1ScopeLabel
}

// RegisterChecker appends a block validity checker to be evaluated at cross-finalized.
func (s *Supervisor) RegisterChecker(c BlockValidityChecker) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.checkers = append(s.checkers, c)
}

// getCheckers returns a snapshot of registered checkers.
func (s *Supervisor) getCheckers() []BlockValidityChecker {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]BlockValidityChecker, len(s.checkers))
	copy(out, s.checkers)
	return out
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
		q := r.URL.Query()
		var chainID uint64
		if cidStr := q.Get("chainId"); cidStr != "" {
			_, _ = fmt.Sscanf(cidStr, "%d", &chainID)
		}
		s.mu.Lock()
		if len(s.chains) > 0 {
			// multi-chain mode
			if chainID == 0 {
				chainID = s.primaryChainID
			}
			h := s.chains[chainID]
			s.mu.Unlock()
			if h == nil {
				http.Error(w, "unknown chainId", http.StatusNotFound)
				return
			}
			h.stateMu.Lock()
			running := h.stopManagedOpNode != nil
			started := h.started
			opNodeUser := h.managedOpNodeUserRPC
			var localSafe, crossSafe any
			var unsafeHead, safeHead, finalizedHead any
			if h.localDB != nil {
				if ls, err := h.localDB.Last(); err == nil {
					localSafe = ls.Derived
				}
			}
			if h.crossDB != nil {
				if cs, err := h.crossDB.Last(); err == nil {
					crossSafe = cs.Derived
				}
			}
			// Also include current op-node heads (best-effort) for observability
			if opNodeUser != "" {
				if cli, err := opclient.NewRPC(r.Context(), s.log, opNodeUser, opclient.WithLazyDial()); err == nil {
					roll := sources.NewRollupClient(cli)
					if st, err := roll.SyncStatus(r.Context()); err == nil && st != nil {
						unsafeHead = st.UnsafeL2
						safeHead = st.SafeL2
						finalizedHead = st.FinalizedL2
					}
					cli.Close()
				}
			}
			h.stateMu.Unlock()
			_ = json.NewEncoder(w).Encode(map[string]any{
				"chain_id":         chainID,
				"op_node_running":  running,
				"started_at":       started,
				"op_node_user_rpc": opNodeUser,
				"unsafe":           unsafeHead,
				"safe":             safeHead,
				"finalized":        finalizedHead,
				"local_safe":       localSafe,
				"cross_safe":       crossSafe,
				"cross_finalized":  s.getCrossFinalized(),
			})
			return
		}
		// single-chain legacy mode
		running := s.cmd != nil
		started := s.started
		opNodeUser := s.managedOpNodeUserRPC
		s.mu.Unlock()
		_ = json.NewEncoder(w).Encode(map[string]any{
			"op_node_running":  running,
			"started_at":       started,
			"op_node_user_rpc": opNodeUser,
			"cross_finalized":  s.getCrossFinalized(),
		})
	})
	// dev-only admin rollback endpoint: POST /admin/rollback { back_n_blocks?: uint64 }
	mux.HandleFunc("/admin/rollback", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var req struct {
			ToBlockNumber *uint64 `json:"to_block_number"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		if req.ToBlockNumber == nil {
			http.Error(w, "missing to_block_number", http.StatusBadRequest)
			return
		}
		// chain-scoped when in multi-chain mode; legacy single-chain otherwise
		s.mu.Lock()
		multi := len(s.chains) > 0
		var cfg *managedConfig
		if !multi {
			cfg = s.managedCfg
		}
		s.mu.Unlock()
		var err error
		if multi {
			// require chainId in multi-chain mode
			q := r.URL.Query()
			var chainID uint64
			if cidStr := q.Get("chainId"); cidStr != "" {
				_, _ = fmt.Sscanf(cidStr, "%d", &chainID)
			}
			if chainID == 0 {
				http.Error(w, "missing chainId", http.StatusBadRequest)
				return
			}
			err = s.RollbackChain(r.Context(), chainID, *req.ToBlockNumber)
		} else {
			if cfg == nil {
				http.Error(w, "managed mode not running", http.StatusServiceUnavailable)
				return
			}
			err = s.performRollback(r.Context(), cfg, *req.ToBlockNumber)
		}
		if err != nil {
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
		// Expose embedded op-node user RPC via reverse proxy (HTTP) under /opnode/ or /opnode/{chainId}/
		mux.HandleFunc("/opnode/", func(w http.ResponseWriter, r *http.Request) {
			s.mu.Lock()
			target := s.managedOpNodeUserRPC
			// Try chain-scoped if multi-chain and a chainId segment is present
			if len(s.chains) > 0 {
				rest := strings.TrimPrefix(r.URL.Path, "/opnode/")
				seg := rest
				if i := strings.IndexByte(rest, '/'); i >= 0 {
					seg = rest[:i]
				}
				if seg != "" {
					var cid uint64
					_, _ = fmt.Sscanf(seg, "%d", &cid)
					if h := s.chains[cid]; h != nil {
						target = h.managedOpNodeUserRPC
					}
				}
			}
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
			// Forward to root of op-node RPC
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

// performRollback stops the managed op-node, rolls back the EL to an absolute block number
// then restarts the op-node and polling.
func (s *Supervisor) performRollback(ctx context.Context, cfg *managedConfig, toBlock uint64) error {
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

	// Roll back EL head to the absolute target via pluggable implementation
	if err := rollbackEL(ctx, cfg.l2UserRPC, toBlock); err != nil {
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
