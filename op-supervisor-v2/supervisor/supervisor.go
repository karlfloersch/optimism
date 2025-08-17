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

	"path/filepath"

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

	// embedded op-node mode
	embeddedOpNodeUserRPC string
	stopEmbeddedOpNode    func(ctx context.Context) error

	// restart context
	embeddedCfg *embeddedConfig

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

	// finalized runner removed; no in-memory cross_finalized or checkers

	// testability hooks
	fetchSyncStatus func(ctx context.Context, rpc string) (*eth.SyncStatus, error)
	runnerInterval  time.Duration
	rollbackFn      func(ctx context.Context, chainID uint64, toBlock uint64) error
}

type embeddedConfig struct {
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

	s := &Supervisor{log: l}
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

	// unique temp dir per instance (can be overridden via SetDataDir or CLI)
	s.dataDir = fmt.Sprintf("%s/sv2-%d-%d", os.TempDir(), os.Getpid(), time.Now().UnixNano())
	// initialize denylist under data dir by default
	s.denylist = NewDenylistStore(filepath.Join(s.dataDir, "denylist.json"))
	return s
}

// getDataDir returns the base data directory for chain DBs
func (s *Supervisor) getDataDir() string { return s.dataDir }

// SetDataDir overrides the base data directory for chain DBs and denylist persistence.
// Should be called before starting any chains or HTTP server.
func (s *Supervisor) SetDataDir(dir string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if dir == "" {
		return
	}
	s.dataDir = dir
	s.denylist = NewDenylistStore(filepath.Join(s.dataDir, "denylist.json"))
}

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
	if s.stopEmbeddedOpNode != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_ = s.stopEmbeddedOpNode(ctx)
		cancel()
		s.stopEmbeddedOpNode = nil
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
	// v1-compatible sync status endpoint
	s.addV1SyncStatusEndpoint(mux)
	// v1-compatible query endpoints
	s.addV1QueryEndpoints(mux)
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
			running := h.stopEmbeddedOpNode != nil
			started := h.started
			opNodeUser := h.embeddedOpNodeUserRPC
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
				"cross_finalized":  s.crossFinalizedFromDBOrFallback(),
				"l1_scope_label":   string(s.getL1ScopeLabel()),
			})
			return
		}
		// single-chain legacy mode
		running := s.cmd != nil
		started := s.started
		opNodeUser := s.embeddedOpNodeUserRPC
		s.mu.Unlock()
		_ = json.NewEncoder(w).Encode(map[string]any{
			"op_node_running":  running,
			"started_at":       started,
			"op_node_user_rpc": opNodeUser,
			"cross_finalized":  s.crossFinalizedFromDBOrFallback(),
			"l1_scope_label":   string(s.getL1ScopeLabel()),
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
		var cfg *embeddedConfig
		if !multi {
			cfg = s.embeddedCfg
		}
		s.mu.Unlock()
		var err error
		if multi {
			// In multi-chain mode, accept explicit chainId, or default to the primary when only one chain is registered.
			q := r.URL.Query()
			var chainID uint64
			if cidStr := q.Get("chainId"); cidStr != "" {
				_, _ = fmt.Sscanf(cidStr, "%d", &chainID)
			}
			if chainID == 0 {
				// default to primary if exactly one chain
				s.mu.Lock()
				primary := s.primaryChainID
				num := len(s.chains)
				s.mu.Unlock()
				if num == 1 && primary != 0 {
					chainID = primary
				} else {
					http.Error(w, "missing chainId", http.StatusBadRequest)
					return
				}
			}
			err = s.RollbackChain(r.Context(), chainID, *req.ToBlockNumber)
		} else {
			if cfg == nil {
				http.Error(w, "embedded mode not running", http.StatusServiceUnavailable)
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
			target := s.embeddedOpNodeUserRPC
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
						target = h.embeddedOpNodeUserRPC
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

// crossFinalizedFromDBOrFallback returns the minimum cross-safe height across chains,
// derived from the per-chain cross DB heads if available. Falls back to the in-memory
// computed value when DBs are not open yet.
func (s *Supervisor) crossFinalizedFromDBOrFallback() uint64 {
	s.mu.Lock()
	chains := make(map[uint64]*chainHandle, len(s.chains))
	for id, h := range s.chains {
		chains[id] = h
	}
	s.mu.Unlock()
	var min uint64
	for _, h := range chains {
		if h == nil {
			continue
		}
		h.stateMu.Lock()
		cross := h.crossDB
		h.stateMu.Unlock()
		if cross == nil {
			continue
		}
		if pair, err := cross.Last(); err == nil {
			num := pair.Derived.Number
			if num != 0 && (min == 0 || num < min) {
				min = num
			}
		}
	}
	if min != 0 {
		return min
	}
	return 0 // Return 0 as crossFinalized is removed.
}

// addV1SyncStatusEndpoint registers GET /v1/sync_status returning eth.SupervisorSyncStatus and 503 until ready.
func (s *Supervisor) addV1SyncStatusEndpoint(mux *http.ServeMux) {
	mux.HandleFunc("/v1/sync_status", func(w http.ResponseWriter, r *http.Request) {
		s.mu.Lock()
		chains := make(map[uint64]*chainHandle, len(s.chains))
		for id, h := range s.chains {
			chains[id] = h
		}
		s.mu.Unlock()

		ready := false
		out := eth.SupervisorSyncStatus{Chains: make(map[eth.ChainID]*eth.SupervisorChainSyncStatus)}
		var haveMinL1 bool
		var minL1 eth.L1BlockRef
		var haveSafeTs bool
		var minSafeTs uint64
		var haveFinTs bool
		var minFinTs uint64

		ctx := r.Context()
		for id, h := range chains {
			var st *eth.SyncStatus
			if h != nil && h.embeddedOpNodeUserRPC != "" {
				if ss, err := s.fetchSyncStatus(ctx, h.embeddedOpNodeUserRPC); err == nil && ss != nil {
					st = ss
				}
			}
			var localID, crossID eth.BlockID
			var crossTs uint64
			var localUnsafe eth.BlockRef
			var finalizedID eth.BlockID
			if h != nil {
				h.stateMu.Lock()
				if h.localDB != nil {
					if pair, err := h.localDB.Last(); err == nil {
						localID = pair.Derived.ID()
					}
				}
				if h.crossDB != nil {
					if pair, err := h.crossDB.Last(); err == nil {
						crossID = pair.Derived.ID()
						crossTs = pair.Derived.Timestamp
						// Cross-finalized equals cross-safe for now
						finalizedID = crossID
					}
				}
				h.stateMu.Unlock()
			}
			if st != nil {
				localUnsafe = st.UnsafeL2.BlockRef()
				if !haveMinL1 || st.CurrentL1.Number < minL1.Number || (st.CurrentL1.Number == minL1.Number && st.CurrentL1.Hash != minL1.Hash) {
					minL1 = st.CurrentL1
					haveMinL1 = true
				}
			}
			// Derive global minima from cross DB timestamps to match v1 semantics,
			// and since cross-finalized == cross-safe for now.
			if crossID.Number > 0 {
				if !haveSafeTs || crossTs < minSafeTs {
					minSafeTs = crossTs
					haveSafeTs = true
				}
				if !haveFinTs || crossTs < minFinTs {
					minFinTs = crossTs
					haveFinTs = true
				}
			}
			if st != nil && (st.UnsafeL2.Number > 0 || st.SafeL2.Number > 0 || st.FinalizedL2.Number > 0) {
				ready = true
			}
			if localID.Number > 0 || crossID.Number > 0 {
				ready = true
			}
			crossUnsafe := eth.BlockID{}
			if st != nil {
				crossUnsafe = st.UnsafeL2.ID()
			}
			out.Chains[eth.ChainIDFromUInt64(id)] = &eth.SupervisorChainSyncStatus{
				LocalUnsafe: localUnsafe,
				LocalSafe:   localID,
				CrossUnsafe: crossUnsafe,
				CrossSafe:   crossID,
				Finalized:   finalizedID,
			}
		}
		if haveMinL1 {
			out.MinSyncedL1 = minL1
		}
		if haveSafeTs {
			out.SafeTimestamp = minSafeTs
		}
		if haveFinTs {
			out.FinalizedTimestamp = minFinTs
		}
		if !ready {
			http.Error(w, "supervisor status tracker not ready", http.StatusServiceUnavailable)
			return
		}
		_ = json.NewEncoder(w).Encode(out)
	})
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
	userRPC, stopFn, err := s.StartEmbeddedOpNode(l1RPC, beaconAddr, l2AuthRPC, jwtSecret, rcfg)
	if err != nil {
		return err
	}
	s.embeddedOpNodeUserRPC = userRPC
	s.stopEmbeddedOpNode = stopFn
	s.embeddedCfg = &embeddedConfig{l1RPC: l1RPC, beaconAddr: beaconAddr, l2AuthRPC: l2AuthRPC, l2UserRPC: l2UserRPC, jwtSecret: jwtSecret, rcfg: rcfg, interval: interval, confirmDepth: confirmDepth}

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

// performRollback stops the embedded op-node, rolls back the EL to an absolute block number
// then restarts the op-node and polling.
func (s *Supervisor) performRollback(ctx context.Context, cfg *embeddedConfig, toBlock uint64) error {
	// Stop polling and op-node
	s.mu.Lock()
	if s.cancelPoll != nil {
		s.cancelPoll()
		s.cancelPoll = nil
	}
	stopFn := s.stopEmbeddedOpNode
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

	// Restart embedded op-node and polling
	s.mu.Lock()
	userRPC, stopFn2, err := s.StartEmbeddedOpNode(cfg.l1RPC, cfg.beaconAddr, cfg.l2AuthRPC, cfg.jwtSecret, cfg.rcfg)
	if err != nil {
		s.mu.Unlock()
		return err
	}
	s.embeddedOpNodeUserRPC = userRPC
	s.stopEmbeddedOpNode = stopFn2
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
	return s.embeddedOpNodeUserRPC
}

// EnableOpNodeProxy toggles the /opnode/ reverse proxy in the HTTP handler.
func (s *Supervisor) EnableOpNodeProxy(v bool) { s.enableOpNodeProxy = v }
