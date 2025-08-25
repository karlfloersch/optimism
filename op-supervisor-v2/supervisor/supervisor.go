package supervisor

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"path/filepath"

	"github.com/ethereum-optimism/optimism/op-node/params"
	"github.com/ethereum-optimism/optimism/op-node/rollup"

	opclient "github.com/ethereum-optimism/optimism/op-service/client"
	"github.com/ethereum-optimism/optimism/op-service/eth"
	"github.com/ethereum-optimism/optimism/op-service/sources"
	"github.com/ethereum-optimism/optimism/op-supervisor/supervisor/backend/depset"
	"github.com/ethereum/go-ethereum/log"
)

type Supervisor struct {
	log log.Logger
	mu  sync.Mutex

	// if true, HTTP handler exposes an /opnode/ reverse proxy to the embedded op-node user RPC
	enableOpNodeProxy bool

	// denylist
	denylist *DenylistStore

	chains       map[uint64]*chainHandle
	activeChains map[eth.ChainID]struct{}

	primaryChainID uint64

	// shared linker across all chains for cross-safety checks
	linkMu       sync.Mutex
	expiryWindow uint64
	linkChecker  depset.LinkChecker

	// L1 scope label used for cross-safe L1 confirmation depth gating (default: eth.Safe; tests may override to eth.Unsafe)
	l1ScopeLabel eth.BlockLabel

	// per-instance data directory for DBs
	dataDir string

	// finalized runner removed; no in-memory cross_finalized or checkers

	// testability hooks
	fetchSyncStatus func(ctx context.Context, rpc string) (*eth.SyncStatus, error)
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
	s.activeChains = make(map[eth.ChainID]struct{})
	s.expiryWindow = params.MessageExpiryTimeSecondsInterop
	s.linkChecker = depset.LinkCheckFn(func(execInChain eth.ChainID, execTs uint64, initChain eth.ChainID, initTs uint64) bool {
		// with no chains registered yet, nothing can execute
		if _, ok := s.activeChains[execInChain]; !ok {
			return false
		}
		if _, ok := s.activeChains[initChain]; !ok {
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
	if s.activeChains == nil {
		s.activeChains = make(map[eth.ChainID]struct{})
	}
	s.activeChains[id] = struct{}{}
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

func (s *Supervisor) Stop() {
	// Stop all chains
	s.mu.Lock()
	chains := make(map[uint64]*chainHandle)
	for id, h := range s.chains {
		chains[id] = h
	}
	s.mu.Unlock()

	for chainID := range chains {
		s.RemoveChain(chainID)
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
		// No chains registered - return empty status
		s.mu.Unlock()
		_ = json.NewEncoder(w).Encode(map[string]any{
			"cross_finalized": s.crossFinalizedFromDBOrFallback(),
			"l1_scope_label":  string(s.getL1ScopeLabel()),
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
		// chain-scoped in multi-chain mode
		s.mu.Lock()
		hasChains := len(s.chains) > 0
		s.mu.Unlock()
		var err error
		if hasChains {
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
			// No chains registered
			http.Error(w, "no chains registered", http.StatusServiceUnavailable)
			return
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
		// Expose embedded op-node user RPC via reverse proxy (HTTP) under /opnode/{chainId}/
		mux.HandleFunc("/opnode/", func(w http.ResponseWriter, r *http.Request) {
			s.mu.Lock()
			var target string
			// Extract chainId from path
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

// performRollback stops the embedded op-node, rolls back the EL to an absolute block number
// then restarts the op-node and polling.
// performRollback removed; legacy single-chain rollback is now handled within RollbackChain

// ManagedOpNodeUserRPC returns the user RPC URL of the embedded op-node for the primary chain if running.
func (s *Supervisor) ManagedOpNodeUserRPC() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.primaryChainID != 0 {
		if h := s.chains[s.primaryChainID]; h != nil {
			return h.embeddedOpNodeUserRPC
		}
	}
	return ""
}

// EnableOpNodeProxy toggles the /opnode/ reverse proxy in the HTTP handler.
func (s *Supervisor) EnableOpNodeProxy(v bool) { s.enableOpNodeProxy = v }
