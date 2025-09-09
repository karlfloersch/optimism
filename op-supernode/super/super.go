package super

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/log"

	"github.com/ethereum-optimism/optimism/op-node/rollup"
	opclient "github.com/ethereum-optimism/optimism/op-service/client"
	"github.com/ethereum-optimism/optimism/op-service/eth"
	"github.com/ethereum-optimism/optimism/op-service/sources"
	"github.com/ethereum-optimism/optimism/op-supernode/super/activities/cross"
	"github.com/ethereum-optimism/optimism/op-supernode/super/chain"
	"github.com/ethereum-optimism/optimism/op-supervisor/supervisor/backend/reads"
	"github.com/ethereum-optimism/optimism/op-supervisor/supervisor/types"
)

type Super struct {
	log log.Logger
	mu  sync.Mutex

	// if true, HTTP handler exposes an /opnode/ reverse proxy to the virtual op-node user RPC
	enableOpNodeProxy bool

	chains   cross.ChainDirectory
	chainsMu sync.Mutex

	// L1 scope label used for cross-safe L1 confirmation depth gating (default: eth.Safe; tests may override to eth.Unsafe)
	l1ScopeLabel eth.BlockLabel

	// per-instance data directory for DBs
	dataDir string

	// cross-validation activity
	crossService *cross.CrossService

	// testability hooks
	fetchSyncStatus func(ctx context.Context, rpc string) (*eth.SyncStatus, error)
	rollbackFn      func(ctx context.Context, chainID uint64, toBlock uint64) error
}

// ============================================================================
// Package-Level Functions & Constructor
// ============================================================================

// defaultScopeLabel returns the default L1 scope label
// it can be overridden via env SV2_L1_SCOPE
func defaultScopeLabel() eth.BlockLabel {
	switch strings.ToLower(os.Getenv("SV2_L1_SCOPE")) {
	case "unsafe":
		return eth.Unsafe
	case "safe":
		return eth.Safe
	case "finalized":
		return eth.Finalized
	}
	return eth.Safe
}

func NewSuper(l log.Logger) *Super {
	s := &Super{log: l.New("service", "super")}
	// initialize shared linker state
	s.l1ScopeLabel = defaultScopeLabel()

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

	// initialize chains directory
	s.chains = make(cross.ChainDirectory)

	// initialize cross-validation service
	s.crossService = cross.NewCrossService(s.log, s.chains, s.dataDir)
	s.crossService.SetRollbackFn(s.rollbackFn)

	// start the cross-validation activity
	go s.crossService.StartActivity(context.Background())

	return s
}

// testLogger returns a quiet logger for unit tests.
func testLogger() log.Logger {
	return log.NewLogger(slog.NewTextHandler(io.Discard, nil))
}

// ============================================================================
// Configuration & Lifecycle Management
// ============================================================================

// getDataDir returns the base data directory for chain DBs
func (s *Super) getDataDir() string { return s.dataDir }

// SetDataDir overrides the base data directory for chain DBs and cross-validation persistence.
// Should be called before starting any chains or HTTP server.
func (s *Super) SetDataDir(dir string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if dir == "" {
		return
	}
	s.dataDir = dir

	// recreate cross service with new data directory
	if s.crossService != nil {
		s.crossService.StopActivity(context.Background())
	}
	s.crossService = cross.NewCrossService(s.log, s.chains, s.dataDir)
	s.crossService.StartActivity(context.Background())
	s.crossService.SetRollbackFn(s.rollbackFn)
}

func (s *Super) getL1ScopeLabel() eth.BlockLabel {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.l1ScopeLabel
}

// SetL1ScopeLabel overrides the L1 scope label (e.g., eth.Unsafe in tests).
func (s *Super) SetL1ScopeLabel(label eth.BlockLabel) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.l1ScopeLabel = label
}

// EnableOpNodeProxy toggles the /opnode/ reverse proxy in the HTTP handler.
func (s *Super) EnableOpNodeProxy(v bool) { s.enableOpNodeProxy = v }

func (s *Super) Stop() {
	// Stop cross service
	if s.crossService != nil {
		s.crossService.StopActivity(context.Background())
	}

	// Stop all chains
	s.mu.Lock()
	chains := make(cross.ChainDirectory)
	for id, container := range s.chains {
		chains[id] = container
	}
	s.mu.Unlock()

	for chainID := range chains {
		s.RemoveChain(chainID)
	}
}

// ============================================================================
// Client Management
// ============================================================================

// EnsureL1Client lazily initializes the L1 client using the given RPC URL.
func (s *Super) EnsureL1Client(ctx context.Context, l1Cli opclient.RPC, l1 *sources.L1Client, l1RPC string, rcfg *rollup.Config) (opclient.RPC, *sources.L1Client) {
	if l1 != nil {
		return l1Cli, l1
	}
	if l1Cli == nil {
		if c, e := opclient.NewRPC(ctx, s.log, l1RPC); e == nil {
			l1Cli = c
		}
	}
	if l1Cli != nil {
		if l1Client, e := sources.NewL1Client(l1Cli, s.log, nil, sources.L1ClientDefaultConfig(rcfg, true, sources.RPCKindStandard)); e == nil {
			l1 = l1Client
		}
	}
	return l1Cli, l1
}

// crossFinalizedFromDBOrFallback returns 0 since cross DBs were removed in v2.
// Kept for API compatibility.
func (s *Super) crossFinalizedFromDBOrFallback() uint64 {
	return 0
}

// ============================================================================
// Chain Orchestration
// ============================================================================

// AddChain starts a chainHandler with virtual node for the given config.
// Returns the L2 chain ID as the container key.
func (s *Super) AddChain(vCfg *chain.VirtualNodeConfig) (uint64, error) {
	chainID := vCfg.Rcfg.L2ChainID.Uint64()

	// Start virtual op-node
	userRPC, stopFn, err := chain.StartVirtualNode(vCfg, s.log)
	if err != nil {
		return 0, err
	}

	container := chain.NewChainContainer()
	container.VirtualOpNodeUserRPC = userRPC
	container.StopVirtualOpNode = stopFn
	container.VirtualCfg = vCfg
	container.Started = time.Now()

	// Create logs DB for chain via cross service
	if err := s.crossService.AddChainLogsDB(chainID); err != nil {
		// Stop the virtual op-node before returning the error
		_ = stopFn(context.Background())
		return 0, err
	}

	s.mu.Lock()
	if s.chains == nil {
		s.chains = make(cross.ChainDirectory)
	}
	s.chains[chainID] = container
	s.mu.Unlock()

	// Update cross service with new chain directory
	if s.crossService != nil {
		s.crossService.UpdateChains(s.chains)
	}

	return chainID, nil
}

// getCrossFinalized returns the DB-backed min cross-safe height (0 if none).
func (s *Super) getCrossFinalized() uint64 { return s.crossFinalizedFromDBOrFallback() }

// RemoveChain stops and unregisters a chain by ID.
func (s *Super) RemoveChain(chainID uint64) {
	s.mu.Lock()
	container := s.chains[chainID]
	delete(s.chains, chainID)
	s.mu.Unlock()

	// Update cross service with new chain directory
	if s.crossService != nil {
		s.crossService.UpdateChains(s.chains)
		// Remove logsDB for this chain
		s.crossService.RemoveChainLogsDB(chainID)
	}

	if container == nil {
		return
	}
	container.StateMu.Lock()
	if container.CancelPoll != nil {
		container.CancelPoll()
		container.CancelPoll = nil
	}
	if container.StopVirtualOpNode != nil {
		_ = container.StopVirtualOpNode(context.Background())
		container.StopVirtualOpNode = nil
	}
	container.StateMu.Unlock()
}

// RollbackChain rolls a specific chain back to an absolute block number.
func (s *Super) RollbackChain(ctx context.Context, chainID uint64, toBlock uint64) error {
	s.mu.Lock()
	container := s.chains[chainID]
	s.mu.Unlock()
	if container == nil || container.VirtualCfg == nil {
		return nil
	}
	container.StateMu.Lock()
	defer container.StateMu.Unlock()

	// Stop polling and op-node
	if container.CancelPoll != nil {
		container.CancelPoll()
		container.CancelPoll = nil
	}
	if container.StopVirtualOpNode != nil {
		c, cancel := context.WithTimeout(ctx, 5*time.Second)
		_ = container.StopVirtualOpNode(c)
		cancel()
	}

	// Roll back logsDB to the target block number via cross service
	if s.crossService != nil {
		if logsDB := s.crossService.GetLogsDB(chainID); logsDB != nil {
			// Attempt to record the soon-to-be-invalidated block (toBlock+1) in the denylist
			invalidNum := toBlock + 1
			if _, _, _, err := logsDB.OpenBlock(invalidNum); err == nil {
				// Access denylist through cross service (we'll need to add a method for this)
				// For now, we'll skip this functionality as it should be handled by the cross service internally
			}

			// Roll back logsDB to the target block number
			if blockRef, _, _, openErr := logsDB.OpenBlock(toBlock); openErr == nil {
				inv := reads.NewRegistry(s.log)
				_ = logsDB.Rewind(inv, blockRef.ID())
			}
		}
	}

	// Roll back EL head to the absolute target via container method
	if err := container.RollbackEL(ctx, toBlock); err != nil {
		return err
	}

	// Restart virtual op-node and polling
	userRPC, stopFn2, err := chain.StartVirtualNode(container.VirtualCfg, s.log)
	if err != nil {
		return err
	}
	container.VirtualOpNodeUserRPC = userRPC
	container.StopVirtualOpNode = stopFn2
	//go s.startChainPolling(ctxPoll, h, roll, l2, chainID)
	return nil
}

// ============================================================================
// HTTP API Handlers
// ============================================================================

// HTTPHandler returns the main HTTP handler for the super
func (s *Super) HTTPHandler() http.Handler {
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
func (s *Super) handleHealthz(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok"))
}

// handleStatus handles the /status endpoint
func (s *Super) handleStatus(w http.ResponseWriter, r *http.Request) {
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

		container.StateMu.Lock()
		running := container.StopVirtualOpNode != nil
		started := container.Started
		opNodeUser := container.VirtualOpNodeUserRPC
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
		container.StateMu.Unlock()

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
func (s *Super) handleAdminRollback(w http.ResponseWriter, r *http.Request) {
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
		if h == nil {
			http.Error(w, "chain not found", http.StatusNotFound)
			return
		}

		logsDB := s.crossService.GetLogsDB(chainID)
		if logsDB == nil {
			http.Error(w, "missing logsDB for chain", http.StatusServiceUnavailable)
			return
		}

		ref, _, _, openErr := logsDB.OpenBlock(*req.ToBlockNumber)
		if openErr != nil {
			http.Error(w, "target block not found in logsDB", http.StatusConflict)
			return
		}

		inv := reads.NewRegistry(s.log)
		if rewindErr := logsDB.Rewind(inv, ref.ID()); rewindErr != nil {
			http.Error(w, rewindErr.Error(), http.StatusInternalServerError)
			return
		}
		s.log.Info("admin: logsDB rewound", "chain", chainID, "to_block", *req.ToBlockNumber)

		// Step 2: cross-safe rollback is now handled by the cross service
		s.log.Info("admin: cross-safe rollback delegated to cross service", "chain", chainID, "to_block", *req.ToBlockNumber)

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
func (s *Super) handleDenylistCheck(w http.ResponseWriter, r *http.Request) {
	// GET /denylist/v1/check?chainId=&id=
	q := r.URL.Query()
	chainIDStr := q.Get("chainId")
	id := q.Get("id")
	var cid uint64
	if chainIDStr != "" {
		_, _ = fmt.Sscanf(chainIDStr, "%d", &cid)
	}

	deny := s.crossService != nil && s.crossService.Denylisted(cid, id)
	s.log.Info("denylist check", "chainId", cid, "id", id, "denylisted", deny)
	_ = json.NewEncoder(w).Encode(map[string]any{"denylisted": deny})
}

// handleAuthorizeFinality handles the /authorize_finality/v1/check endpoint
func (s *Super) handleAuthorizeFinality(w http.ResponseWriter, r *http.Request) {
	// GET /authorize_finality/v1/check?timestamp=
	q := r.URL.Query()
	timestampStr := q.Get("timestamp")

	var timestamp uint64
	if timestampStr != "" {
		_, _ = fmt.Sscanf(timestampStr, "%d", &timestamp)
	}

	// Authorization is now handled by the cross service
	authorized := s.crossService != nil && s.crossService.Finalized(timestamp)
	s.log.Debug("finality authorization check", "timestamp", timestamp, "authorized", authorized)
	_ = json.NewEncoder(w).Encode(map[string]any{"authorized": authorized})
}

// handleOpNodeProxy handles the /opnode/ reverse proxy endpoint
func (s *Super) handleOpNodeProxy(w http.ResponseWriter, r *http.Request) {
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
			target = container.VirtualOpNodeUserRPC
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
func (s *Super) addV1SyncStatusEndpoint(mux *http.ServeMux) {
	mux.HandleFunc("/v1/sync_status", s.handleV1SyncStatus)
}

// handleV1SyncStatus handles the /v1/sync_status endpoint
func (s *Super) handleV1SyncStatus(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	chains := make(cross.ChainDirectory, len(s.chains))
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
		if container != nil && container.VirtualOpNodeUserRPC != "" {
			if ss, err := s.fetchSyncStatus(ctx, container.VirtualOpNodeUserRPC); err == nil && ss != nil {
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

		// Cross-safe from global timestamp (delegated to cross service)
		ts := uint64(0) // TODO: get from cross service when needed
		if ts > 0 && container != nil && container.VirtualCfg != nil && container.VirtualCfg.Rcfg != nil {
			if num, err := container.VirtualCfg.Rcfg.TargetBlockNumber(ts); err == nil {
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

	currentTS := uint64(0) // TODO: get from cross service when needed
	out.SafeTimestamp = currentTS
	out.FinalizedTimestamp = currentTS

	if !ready {
		http.Error(w, "super status tracker not ready", http.StatusServiceUnavailable)
		return
	}

	_ = json.NewEncoder(w).Encode(out)
}

// ============================================================================
// V1 Query API Handlers
// ============================================================================

// addV1QueryEndpoints registers lightweight HTTP endpoints that mirror a subset of v1 super query APIs.
// These are HTTP endpoints (not JSON-RPC) for simplicity, returning the same shapes as v1 types where applicable.
func (s *Super) addV1QueryEndpoints(mux *http.ServeMux) {
	mux.HandleFunc("/v1/local_safe", s.handleV1LocalSafe)
	mux.HandleFunc("/v1/cross_safe", s.handleV1CrossSafe)
	mux.HandleFunc("/v1/finalized", s.handleV1Finalized)
	mux.HandleFunc("/v1/finalized_l1", s.handleV1FinalizedL1)
	mux.HandleFunc("/v1/cross_derived_to_source", s.handleV1CrossDerivedToSource)
	mux.HandleFunc("/v1/superroot_at_ts", s.handleV1SuperrootAtTs)
}

// handleV1LocalSafe handles the /v1/local_safe endpoint
func (s *Super) handleV1LocalSafe(w http.ResponseWriter, r *http.Request) {
	_, h := s.resolveChainFromQuery(w, r)
	if h == nil {
		return
	}
	// v2 Note: localDB was removed - always return empty result for API compatibility
	var out types.DerivedIDPair
	_ = json.NewEncoder(w).Encode(out)
}

// handleV1CrossSafe handles the /v1/cross_safe endpoint
func (s *Super) handleV1CrossSafe(w http.ResponseWriter, r *http.Request) {
	_, h := s.resolveChainFromQuery(w, r)
	if h == nil {
		return
	}
	var out types.DerivedIDPair
	// Compute derived number from global crossSafeTimestamp; ignore hash (delegated to cross service)
	ts := uint64(0) // TODO: get from cross service when needed
	if ts > 0 && h.VirtualCfg != nil && h.VirtualCfg.Rcfg != nil {
		if num, err := h.VirtualCfg.Rcfg.TargetBlockNumber(ts); err == nil {
			out.Derived.Number = num
		}
	}
	_ = json.NewEncoder(w).Encode(out)
}

// handleV1Finalized handles the /v1/finalized endpoint
func (s *Super) handleV1Finalized(w http.ResponseWriter, r *http.Request) {
	_, container := s.resolveChainFromQuery(w, r)
	if container == nil {
		return
	}
	var out eth.BlockID
	if container.VirtualOpNodeUserRPC != "" {
		if st, err := s.fetchSyncStatus(r.Context(), container.VirtualOpNodeUserRPC); err == nil && st != nil {
			out = st.FinalizedL2.ID()
		}
	}
	_ = json.NewEncoder(w).Encode(out)
}

// handleV1FinalizedL1 handles the /v1/finalized_l1 endpoint
func (s *Super) handleV1FinalizedL1(w http.ResponseWriter, r *http.Request) {
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
		if container != nil && container.VirtualOpNodeUserRPC != "" {
			if st, err := s.fetchSyncStatus(r.Context(), container.VirtualOpNodeUserRPC); err == nil && st != nil {
				if out.Number == 0 || st.FinalizedL1.Number < out.Number {
					out = st.FinalizedL1
				}
			}
		}
	}
	_ = json.NewEncoder(w).Encode(out)
}

// handleV1CrossDerivedToSource handles the /v1/cross_derived_to_source endpoint
func (s *Super) handleV1CrossDerivedToSource(w http.ResponseWriter, r *http.Request) {
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
func (s *Super) handleV1SuperrootAtTs(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusNotImplemented)
	_ = json.NewEncoder(w).Encode(map[string]string{
		"error": "superroot endpoint is not supported in Super v2",
	})
}

// ============================================================================
// Helper Functions
// ============================================================================

// resolveChainFromQuery parses chainId and returns the chain container, replying with errors if invalid.
func (s *Super) resolveChainFromQuery(w http.ResponseWriter, r *http.Request) (uint64, *chain.ChainContainerImpl) {
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

// ============================================================================
// Metrics (No-op implementations)
// ============================================================================

// Minimal no-op metrics adapters to satisfy v1 DB openings.

// logsMetricsNoop implements op-super v1 logs.Metrics
type logsMetricsNoop struct{}

func (logsMetricsNoop) RecordDBEntryCount(kind string, count int64) {}
func (logsMetricsNoop) RecordDBSearchEntriesRead(count int64)       {}

// chainMetricsNoop implements fromda.ChainMetrics
type chainMetricsNoop struct{}

func (chainMetricsNoop) RecordDBEntryCount(kind string, count int64) {}
