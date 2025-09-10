package supervisor

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httputil"
	"net/url"
	"sort"
	"strings"

	opclient "github.com/ethereum-optimism/optimism/op-service/client"
	"github.com/ethereum-optimism/optimism/op-service/eth"
	"github.com/ethereum-optimism/optimism/op-service/sources"
	"github.com/ethereum-optimism/optimism/op-supervisor/supervisor/backend/reads"
	"github.com/ethereum-optimism/optimism/op-supervisor/supervisor/types"
)

// HTTPHandler returns the main HTTP handler for the supervisor
func (s *Supervisor) HTTPHandler() http.Handler {
	mux := http.NewServeMux()

	// Register all endpoints
	mux.HandleFunc("/healthz", s.handleHealthz)
	mux.HandleFunc("/status", s.handleStatus)
	mux.HandleFunc("/admin/rollback", s.handleAdminRollback)
	mux.HandleFunc("/denylist/v1/check", s.handleDenylistCheck)
	mux.HandleFunc("/authorize_finality/v1/check", s.handleAuthorizeFinality)

	// v1-compatible endpoints
	s.addV1SyncStatusEndpoint(mux)
	s.addV1QueryEndpoints(mux)

	// Optional op-node proxy
	if s.enableOpNodeProxy {
		mux.HandleFunc("/opnode/", s.handleOpNodeProxy)
	}

	return mux
}

// handleHealthz handles the /healthz endpoint
func (s *Supervisor) handleHealthz(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok"))
}

// handleStatus handles the /status endpoint
func (s *Supervisor) handleStatus(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	var chainID uint64
	if cidStr := q.Get("chainId"); cidStr != "" {
		_, _ = fmt.Sscanf(cidStr, "%d", &chainID)
	}

	s.mu.Lock()
	if len(s.chains) > 0 {
		// multi-chain mode
		if chainID == 0 {
			http.Error(w, "missing chainId parameter", http.StatusBadRequest)
			s.mu.Unlock()
			return
		}
		container := s.chains[chainID]
		s.mu.Unlock()
		if container == nil {
			http.Error(w, "unknown chainId", http.StatusNotFound)
			return
		}

		container.stateMu.Lock()
		running := container.stopVirtualOpNode != nil
		started := container.started
		opNodeUser := container.virtualOpNodeUserRPC
		var localSafe, crossSafe any
		var unsafeHead, safeHead, finalizedHead any

		// v2 Note: localDB and crossDB removed - these fields remain nil for API compatibility
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
		container.stateMu.Unlock()

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
}

// handleAdminRollback handles the /admin/rollback endpoint
func (s *Supervisor) handleAdminRollback(w http.ResponseWriter, r *http.Request) {
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
		// In multi-chain mode, require explicit chainId parameter.
		q := r.URL.Query()
		var chainID uint64
		if cidStr := q.Get("chainId"); cidStr != "" {
			_, _ = fmt.Sscanf(cidStr, "%d", &chainID)
		}
		if chainID == 0 {
			http.Error(w, "missing chainId parameter", http.StatusBadRequest)
			return
		}

		// Step 1: rewind logsDB to target block and collect its timestamp
		s.mu.Lock()
		h := s.chains[chainID]
		s.mu.Unlock()
		if h == nil || h.logsDB == nil {
			http.Error(w, "missing logsDB for chain", http.StatusServiceUnavailable)
			return
		}

		ref, _, _, openErr := h.logsDB.OpenBlock(*req.ToBlockNumber)
		if openErr != nil {
			http.Error(w, "target block not found in logsDB", http.StatusConflict)
			return
		}

		inv := reads.NewRegistry(s.log)
		if rewindErr := h.logsDB.Rewind(inv, ref.ID()); rewindErr != nil {
			http.Error(w, rewindErr.Error(), http.StatusInternalServerError)
			return
		}
		s.log.Info("LogsDB rewound", "chain_id", chainID, "target_block", *req.ToBlockNumber)

		// Step 2: roll back the cross-safe head to the block timestamp
		s.setCrossSafeTimestamp(ref.Time)
		s.log.Info("Cross-safe timestamp rewound", "chain_id", chainID, "timestamp", ref.Time, "target_block", *req.ToBlockNumber)

		// Existing behavior: roll back EL/op-node for the chain as well
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
}

// handleDenylistCheck handles the /denylist/v1/check endpoint
func (s *Supervisor) handleDenylistCheck(w http.ResponseWriter, r *http.Request) {
	// GET /denylist/v1/check?chainId=&id=
	q := r.URL.Query()
	chainIDStr := q.Get("chainId")
	id := q.Get("id")
	var cid uint64
	if chainIDStr != "" {
		_, _ = fmt.Sscanf(chainIDStr, "%d", &cid)
	}

	deny := s.denylist != nil && id != "" && s.denylist.Has(cid, id)
	s.log.Info("Denylist check completed", "chain_id", cid, "identifier", id, "denied", deny)
	_ = json.NewEncoder(w).Encode(map[string]any{"denylisted": deny})
}

// handleAuthorizeFinality handles the /authorize_finality/v1/check endpoint
func (s *Supervisor) handleAuthorizeFinality(w http.ResponseWriter, r *http.Request) {
	// GET /authorize_finality/v1/check?timestamp=
	q := r.URL.Query()
	timestampStr := q.Get("timestamp")

	var timestamp uint64
	if timestampStr != "" {
		_, _ = fmt.Sscanf(timestampStr, "%d", &timestamp)
	}

	authorized := s.authorizeFinalityUpdate(timestamp)
	s.log.Info("finality authorization check", "timestamp", timestamp, "authorized", authorized)
	fmt.Println("finality authorization check", "timestamp", timestamp, "authorized", authorized)
	_ = json.NewEncoder(w).Encode(map[string]any{"authorized": authorized})
}

// handleOpNodeProxy handles the /opnode/ reverse proxy endpoint
func (s *Supervisor) handleOpNodeProxy(w http.ResponseWriter, r *http.Request) {
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
		if container := s.chains[cid]; container != nil {
			target = container.virtualOpNodeUserRPC
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
}

// addV1SyncStatusEndpoint registers GET /v1/sync_status returning eth.SupervisorSyncStatus and 503 until ready.
func (s *Supervisor) addV1SyncStatusEndpoint(mux *http.ServeMux) {
	mux.HandleFunc("/v1/sync_status", s.handleV1SyncStatus)
}

// handleV1SyncStatus handles the /v1/sync_status endpoint
func (s *Supervisor) handleV1SyncStatus(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	chains := make(map[uint64]*ChainContainer, len(s.chains))
	for id, container := range s.chains {
		chains[id] = container
	}
	s.mu.Unlock()

	ready := false
	out := eth.SupervisorSyncStatus{Chains: make(map[eth.ChainID]*eth.SupervisorChainSyncStatus)}
	var haveMinL1 bool
	var minL1 eth.L1BlockRef

	ctx := r.Context()
	for id, container := range chains {
		var st *eth.SyncStatus
		if container != nil && container.virtualOpNodeUserRPC != "" {
			if ss, err := s.fetchSyncStatus(ctx, container.virtualOpNodeUserRPC); err == nil && ss != nil {
				st = ss
			}
		}

		var localID, crossID eth.BlockID
		var localUnsafe eth.BlockRef
		var finalizedID eth.BlockID
		if st != nil {
			localUnsafe = st.UnsafeL2.BlockRef()
			// Source LocalSafe directly from the op-node SafeL2
			localID = st.SafeL2.ID()
			if !haveMinL1 || st.CurrentL1.Number < minL1.Number || (st.CurrentL1.Number == minL1.Number && st.CurrentL1.Hash != minL1.Hash) {
				minL1 = st.CurrentL1
				haveMinL1 = true
			}
		}

		// Cross-safe from global timestamp
		ts := s.getCurrentCrossSafeTimestamp()
		if ts > 0 && container != nil && container.virtualCfg != nil && container.virtualCfg.Rcfg != nil {
			if num, err := container.virtualCfg.Rcfg.TargetBlockNumber(ts); err == nil {
				crossID.Number = num
				finalizedID = crossID
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

	currentTS := s.getCurrentCrossSafeTimestamp()
	out.SafeTimestamp = currentTS
	out.FinalizedTimestamp = currentTS

	if !ready {
		http.Error(w, "supervisor status tracker not ready", http.StatusServiceUnavailable)
		return
	}

	_ = json.NewEncoder(w).Encode(out)
}

// ============================================================================
// V1 Query API Handlers
// ============================================================================

// addV1QueryEndpoints registers lightweight HTTP endpoints that mirror a subset of v1 supervisor query APIs.
// These are HTTP endpoints (not JSON-RPC) for simplicity, returning the same shapes as v1 types where applicable.
func (s *Supervisor) addV1QueryEndpoints(mux *http.ServeMux) {
	mux.HandleFunc("/v1/local_safe", s.handleV1LocalSafe)
	mux.HandleFunc("/v1/cross_safe", s.handleV1CrossSafe)
	mux.HandleFunc("/v1/finalized", s.handleV1Finalized)
	mux.HandleFunc("/v1/finalized_l1", s.handleV1FinalizedL1)
	mux.HandleFunc("/v1/cross_derived_to_source", s.handleV1CrossDerivedToSource)
	mux.HandleFunc("/v1/superroot_at_ts", s.handleV1SuperrootAtTs)
}

// handleV1LocalSafe handles the /v1/local_safe endpoint
func (s *Supervisor) handleV1LocalSafe(w http.ResponseWriter, r *http.Request) {
	_, h := s.resolveChainFromQuery(w, r)
	if h == nil {
		return
	}
	// v2 Note: localDB was removed - always return empty result for API compatibility
	var out types.DerivedIDPair
	_ = json.NewEncoder(w).Encode(out)
}

// handleV1CrossSafe handles the /v1/cross_safe endpoint
func (s *Supervisor) handleV1CrossSafe(w http.ResponseWriter, r *http.Request) {
	_, h := s.resolveChainFromQuery(w, r)
	if h == nil {
		return
	}
	var out types.DerivedIDPair
	// Compute derived number from global crossSafeTimestamp; ignore hash
	ts := s.getCurrentCrossSafeTimestamp()
	if ts > 0 && h.virtualCfg != nil && h.virtualCfg.Rcfg != nil {
		if num, err := h.virtualCfg.Rcfg.TargetBlockNumber(ts); err == nil {
			out.Derived.Number = num
		}
	}
	_ = json.NewEncoder(w).Encode(out)
}

// handleV1Finalized handles the /v1/finalized endpoint
func (s *Supervisor) handleV1Finalized(w http.ResponseWriter, r *http.Request) {
	_, container := s.resolveChainFromQuery(w, r)
	if container == nil {
		return
	}
	var out eth.BlockID
	if container.virtualOpNodeUserRPC != "" {
		if st, err := s.fetchSyncStatus(r.Context(), container.virtualOpNodeUserRPC); err == nil && st != nil {
			out = st.FinalizedL2.ID()
		}
	}
	_ = json.NewEncoder(w).Encode(out)
}

// handleV1FinalizedL1 handles the /v1/finalized_l1 endpoint
func (s *Supervisor) handleV1FinalizedL1(w http.ResponseWriter, r *http.Request) {
	// Return the minimum known finalized L1 across chains if available via op-node
	var out eth.L1BlockRef
	s.mu.Lock()
	chains := make([]uint64, 0, len(s.chains))
	for id := range s.chains {
		chains = append(chains, id)
	}
	s.mu.Unlock()
	sort.Slice(chains, func(i, j int) bool { return chains[i] < chains[j] })
	for _, id := range chains {
		s.mu.Lock()
		container := s.chains[id]
		s.mu.Unlock()
		if container != nil && container.virtualOpNodeUserRPC != "" {
			if st, err := s.fetchSyncStatus(r.Context(), container.virtualOpNodeUserRPC); err == nil && st != nil {
				if out.Number == 0 || st.FinalizedL1.Number < out.Number {
					out = st.FinalizedL1
				}
			}
		}
	}
	_ = json.NewEncoder(w).Encode(out)
}

// handleV1CrossDerivedToSource handles the /v1/cross_derived_to_source endpoint
func (s *Supervisor) handleV1CrossDerivedToSource(w http.ResponseWriter, r *http.Request) {
	_, h := s.resolveChainFromQuery(w, r)
	if h == nil {
		return
	}
	q := r.URL.Query()
	var derivedNum uint64
	if _, err := fmtSscanf(q.Get("derived"), &derivedNum); err != nil {
		http.Error(w, "bad derived", http.StatusBadRequest)
		return
	}
	// v2 Note: crossDB was removed - always return empty result for API compatibility
	var out eth.BlockRef
	_ = json.NewEncoder(w).Encode(out)
}

// handleV1SuperrootAtTs handles the /v1/superroot_at_ts endpoint
func (s *Supervisor) handleV1SuperrootAtTs(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusNotImplemented)
	_ = json.NewEncoder(w).Encode(map[string]string{
		"error": "superroot endpoint is not supported in Supervisor v2",
	})
}

// ============================================================================
// Helper Functions
// ============================================================================

// resolveChainFromQuery parses chainId and returns the chain container, replying with errors if invalid.
func (s *Supervisor) resolveChainFromQuery(w http.ResponseWriter, r *http.Request) (uint64, *ChainContainer) {
	q := r.URL.Query()
	var chainID uint64
	if cidStr := q.Get("chainId"); cidStr != "" {
		_, _ = fmtSscanf(cidStr, &chainID)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.chains) == 0 {
		http.Error(w, "multi-chain not enabled", http.StatusBadRequest)
		return 0, nil
	}
	if chainID == 0 {
		http.Error(w, "missing chainId parameter", http.StatusBadRequest)
		return 0, nil
	}
	h := s.chains[chainID]
	if h == nil {
		http.Error(w, "unknown chainId", http.StatusNotFound)
		return 0, nil
	}
	return chainID, h
}

// small sscanf helper without pulling fmt here to keep imports tight in this file.
func fmtSscanf(in string, out *uint64) (int, error) {
	if in == "" {
		return 0, fmtErrEmpty()
	}
	var v uint64
	for i := 0; i < len(in); i++ {
		c := in[i]
		if c < '0' || c > '9' {
			return 0, fmtErrBad()
		}
		v = v*10 + uint64(c-'0')
	}
	*out = v
	return 1, nil
}

type simpleErr string

func (e simpleErr) Error() string { return string(e) }

func fmtErrEmpty() error { return simpleErr("empty") }
func fmtErrBad() error   { return simpleErr("bad") }
