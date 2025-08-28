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
	"github.com/ethereum-optimism/optimism/op-node/rollup/derive"

	opclient "github.com/ethereum-optimism/optimism/op-service/client"
	"github.com/ethereum-optimism/optimism/op-service/eth"
	"github.com/ethereum-optimism/optimism/op-service/sources"
	fromda "github.com/ethereum-optimism/optimism/op-supervisor/supervisor/backend/db/fromda"
	logsdb "github.com/ethereum-optimism/optimism/op-supervisor/supervisor/backend/db/logs"
	"github.com/ethereum-optimism/optimism/op-supervisor/supervisor/backend/depset"
	"github.com/ethereum-optimism/optimism/op-supervisor/supervisor/backend/reads"
	"github.com/ethereum-optimism/optimism/op-supervisor/supervisor/types"
	"github.com/ethereum/go-ethereum/log"
)

type Supervisor struct {
	log log.Logger
	mu  sync.Mutex

	// if true, HTTP handler exposes an /opnode/ reverse proxy to the embedded op-node user RPC
	enableOpNodeProxy bool

	// denylist
	denylist *DenylistStore

	chains         map[uint64]*ChainHandle
	activeChainsMu sync.Mutex
	activeChains   map[eth.ChainID]struct{}

	expiryWindow uint64
	linkChecker  depset.LinkChecker

	// L1 scope label used for cross-safe L1 confirmation depth gating (default: eth.Safe; tests may override to eth.Unsafe)
	l1ScopeLabel eth.BlockLabel

	// per-instance data directory for DBs
	dataDir string

	// finalized runner removed; no in-memory cross_finalized or checkers

	// crossSafeTimestamp is the global cross-safe timestamp (monotonic, non-decreasing)
	crossSafeTimestamp uint64

	// testability hooks
	fetchSyncStatus func(ctx context.Context, rpc string) (*eth.SyncStatus, error)
	rollbackFn      func(ctx context.Context, chainID uint64, toBlock uint64) error
}

// chainRef is a lightweight pair of chain ID and handle used to avoid repeated global lookups
type chainRef struct {
	id uint64
	h  *ChainHandle
}

func (s *Supervisor) checkExecutingMessageLink(execInChain eth.ChainID, execTs uint64, initChain eth.ChainID, initTs uint64) bool {
	s.activeChainsMu.Lock()
	defer s.activeChainsMu.Unlock()
	if _, ok := s.activeChains[execInChain]; !ok {
		return false
	}
	if _, ok := s.activeChains[initChain]; !ok {
		return false
	}
	if initTs > execTs {
		return false
	}
	if execTs > initTs+s.expiryWindow {
		return false
	}
	return true
}

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

func NewSupervisor(l log.Logger) *Supervisor {
	s := &Supervisor{log: l.New("service", "supervisor_v2")}
	// initialize shared linker state
	s.activeChains = make(map[eth.ChainID]struct{})
	s.expiryWindow = params.MessageExpiryTimeSecondsInterop
	s.linkChecker = depset.LinkCheckFn(s.checkExecutingMessageLink)
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
	// initialize denylist under data dir by default
	s.denylist = NewDenylistStore(filepath.Join(s.dataDir, "denylist.json"))
	go s.ProgressCrossSafe()
	return s
}

// ProgressCrossSafe advances a global cross-safe timestamp without using the cross DB.
//
// Overview
// - State: s.crossSafeTimestamp (monotonic, never decreases)
// - Inputs per chain:
//   - op-node SyncStatus timestamps (Unsafe/Safe/Finalized)
//   - rollup.Config (TimestampForBlock / TargetBlockNumber mapping)
//   - logsDB + localDB (for ingestion and per-chain derived mapping)
//
// - Output surface:
//   - /v1/cross_safe computes an L2 block number on-the-fly via TargetBlockNumber(s.crossSafeTimestamp)
//   - /v1/sync_status reports CrossSafe/Finalized derived from the same timestamp
//
// Steps (each tick ~500ms)
// 0) Early exit if there are no active chains (prevents pre-genesis advancement).
// 1) Initialization (once): if crossSafeTimestamp == 0, set it to min(genesis L2 time) across active chains.
// 2) Candidate: ts := crossSafeTimestamp + 1.
// 3) Gate: require each active chain to have SyncStatus.SafeL2.Time >= ts (scope can be generalized later).
// 4) For each chain:
//   - Compute target L2 block number = floor(TargetBlockNumber(ts)). If ts < genesis, wait.
//   - Ensure L1/L2 clients.
//   - Seed local DB if empty from the target (best-effort).
//   - Ingest logs/local up to the target number.
//
// 5) Validation: placeholder (log-only for now; message-link checks can be added later).
// 6) Rollback on validation errors: placeholder (log-only for now; will call rollbackFn per-chain).
// 7) Commit: if gates passed and ingest completed, set crossSafeTimestamp = ts.
//
// Invariants
// - crossSafeTimestamp only increases when all chains can model ts.
// - No adapter/cross DB usage; all derived lookups are via logsDB/localDB and rollup.Config.
func (s *Supervisor) ProgressCrossSafe() {
	// configure loop tick duration
	tick := 500 * time.Millisecond
	for {
		s.log.Info("xsafe: loop start")
		ctx := context.Background()

		// Step 0: snapshot active chains and early-exit if none
		activeChains := s.xsafeSnapshotActiveChains()
		if len(activeChains) == 0 {
			s.log.Info("xsafe: no active chains; skipping")
			time.Sleep(tick)
			continue
		}
		// Resolve chain handles once to reduce global lookups
		var refs []chainRef
		for _, id := range activeChains {
			if v, ok := id.Uint64(); ok {
				s.mu.Lock()
				h := s.chains[v]
				s.mu.Unlock()
				refs = append(refs, chainRef{id: v, h: h})
			}
		}

		// Step 1: initialization — set to min genesis if unset
		s.mu.Lock()
		ts := s.crossSafeTimestamp
		s.mu.Unlock()
		s.log.Info("xsafe: current timestamp", "ts", ts)
		if ts == 0 {
			if minGenesis := s.getMinGenesisTimestamp(refs); minGenesis != 0 {
				s.mu.Lock()
				s.crossSafeTimestamp = minGenesis
				s.mu.Unlock()
				// next tick will try to advance beyond genesis
				time.Sleep(tick)
				continue
			}
		}

		// Step 2: compute candidate timestamp
		s.mu.Lock()
		ts = s.crossSafeTimestamp + 1
		s.mu.Unlock()
		s.log.Info("xsafe: candidate next timestamp", "ts", ts)

		// Step 3: obtain L1<>L2 block pairs that meet or preceed ts
		pairs, err := s.getBlocksAtTimestamp(ctx, refs, ts)
		if err != nil {
			s.log.Info("xsafe: blocks not ready", "err", err)
			time.Sleep(tick)
			continue
		}
		s.log.Info("xsafe: blocks at ts", "ts", ts, "chains", len(pairs))

		// Step 4: per-chain target and ingest up to it (manual seals) using pairs
		ready := s.xsafeIngestLogsTo(ctx, refs, pairs)
		if !ready {
			time.Sleep(tick)
			continue
		}

		// Step 5: get executing messages, validate them, and rollback if needed
		// TODO: Fix
		execByChain := s.getExecutingMessages(refs, ts)
		valid := s.validateExecutingMessages(refs, execByChain)
		if !valid {
			s.log.Info("xsafe: validation detected issues (rollback to be handled in step 6)")
		}
		s.xsafeRollbackPlaceholder(activeChains)

		// Step 6: commit new timestamp
		s.mu.Lock()
		s.crossSafeTimestamp = ts
		s.log.Info("xsafe: committed new timestamp", "ts", ts)
		s.mu.Unlock()

		// Step end: wait for next tick
		time.Sleep(tick)
	}
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

// MarkChainActive marks a chain ID as active via the activeChains set.
func (s *Supervisor) MarkChainActive(id eth.ChainID) {
	s.activeChainsMu.Lock()
	defer s.activeChainsMu.Unlock()
	if s.activeChains == nil {
		s.activeChains = make(map[eth.ChainID]struct{})
	}
	s.activeChains[id] = struct{}{}
}

// getLinkChecker returns the supervisor's LinkChecker.
func (s *Supervisor) getLinkChecker() depset.LinkChecker {
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
	chains := make(map[uint64]*ChainHandle)
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
				http.Error(w, "missing chainId parameter", http.StatusBadRequest)
				s.mu.Unlock()
				return
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
			s.log.Info("admin: logsDB rewound", "chain", chainID, "to_block", *req.ToBlockNumber)

			// Step 2: roll back the cross-safe head to the block timestamp
			s.mu.Lock()
			s.crossSafeTimestamp = ref.Time
			s.mu.Unlock()
			s.log.Info("admin: cross-safe timestamp rewound", "chain", chainID, "ts", ref.Time, "to_block", *req.ToBlockNumber)

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
	chains := make(map[uint64]*ChainHandle, len(s.chains))
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
		chains := make(map[uint64]*ChainHandle, len(s.chains))
		for id, h := range s.chains {
			chains[id] = h
		}
		s.mu.Unlock()

		ready := false
		out := eth.SupervisorSyncStatus{Chains: make(map[eth.ChainID]*eth.SupervisorChainSyncStatus)}
		var haveMinL1 bool
		var minL1 eth.L1BlockRef

		ctx := r.Context()
		for id, h := range chains {
			var st *eth.SyncStatus
			if h != nil && h.embeddedOpNodeUserRPC != "" {
				if ss, err := s.fetchSyncStatus(ctx, h.embeddedOpNodeUserRPC); err == nil && ss != nil {
					st = ss
				}
			}
			var localID, crossID eth.BlockID
			var localUnsafe eth.BlockRef
			var finalizedID eth.BlockID
			if h != nil {
				h.stateMu.Lock()
				if h.localDB != nil {
					if pair, err := h.localDB.Last(); err == nil {
						localID = pair.Derived.ID()
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
			// Cross-safe from global timestamp
			s.mu.Lock()
			ts := s.crossSafeTimestamp
			s.mu.Unlock()
			if ts > 0 && h != nil && h.virtualCfg != nil && h.virtualCfg.Rcfg != nil {
				if num, err := h.virtualCfg.Rcfg.TargetBlockNumber(ts); err == nil {
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
		s.mu.Lock()
		out.SafeTimestamp = s.crossSafeTimestamp
		out.FinalizedTimestamp = s.crossSafeTimestamp
		s.mu.Unlock()
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

// ManagedOpNodeUserRPC returns the user RPC URL of the embedded op-node for the first available chain if running.
// Returns empty string if no chains are available.
func (s *Supervisor) ManagedOpNodeUserRPC() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	// Return the first available chain's op-node RPC
	for _, h := range s.chains {
		if h != nil {
			return h.embeddedOpNodeUserRPC
		}
	}
	return ""
}

// EnableOpNodeProxy toggles the /opnode/ reverse proxy in the HTTP handler.
func (s *Supervisor) EnableOpNodeProxy(v bool) { s.enableOpNodeProxy = v }

// EnsureL1Client lazily initializes the L1 client using the given RPC URL.
func (s *Supervisor) EnsureL1Client(ctx context.Context, l1Cli opclient.RPC, l1 *sources.L1Client, l1RPC string, rcfg *rollup.Config) (opclient.RPC, *sources.L1Client) {
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

// IngestToLocalSafe computes the ingest range and ingests up to target.
func (s *Supervisor) IngestToLocalSafe(ctx context.Context, h *ChainHandle, l1 *sources.L1Client, l2 *sources.L2Client, rcfg *rollup.Config, chainID uint64, target uint64) {
	var startLogs uint64 = 0
	if last, ok := h.logsDB.LatestSealedBlock(); ok {
		startLogs = last.Number + 1
		if last.Number >= target {
			return
		}
	}
	var startLocal uint64 = 0
	if pair, err := h.localDB.Last(); err == nil {
		startLocal = pair.Derived.Number + 1
	}
	start := startLogs
	if startLocal < start {
		start = startLocal
	}
	if start > target {
		start = target
	}
	s.log.Info("ingest: range", "chain", chainID, "start", start, "end", target)
	if err := ingestRange(ctx, l1, l2, h.logsDB, h.localDB, h.crossDB, sources.L2ClientDefaultConfig(rcfg, true), start, target); err != nil {
		s.log.Info("ingest: deferred", "err", err)
	}
}

// SeedLocalIfEmpty seeds the first derived mapping from the current target if local DB is empty.
func (s *Supervisor) SeedLocalIfEmpty(ctx context.Context, h *ChainHandle, l1 *sources.L1Client, l2 *sources.L2Client, rcfg *rollup.Config, chainID uint64, target uint64) {
	if !h.localDB.IsEmpty() {
		return
	}
	env, err := l2.PayloadByNumber(ctx, target)
	if err != nil {
		return
	}
	if br, derr := derive.PayloadToBlockRef(rcfg, env.ExecutionPayload); derr == nil {
		l1Ref, e1 := l1.BlockRefByNumber(ctx, br.L1Origin.Number)
		if e1 == nil && l1Ref.Hash == br.L1Origin.Hash {
			if did, _ := s.ensureDerived(h.localDB, l1Ref, br.BlockRef(), types.RevisionAny); did {
				s.log.Info("seed: local derived from target", "chain", chainID, "l1", l1Ref, "l2", br.BlockRef())
			}
		}
	}
}

// DebugIngestHeads logs the heads of logs/local/cross for observability.
func (s *Supervisor) DebugIngestHeads(h *ChainHandle, chainID uint64) {
	if blk, ok := h.logsDB.LatestSealedBlock(); ok {
		s.log.Info("ingest: logs head", "chain", chainID, "num", blk.Number)
	}
	if pair, err := h.localDB.Last(); err == nil {
		s.log.Info("ingest: local head", "chain", chainID, "l1", pair.Source, "l2", pair.Derived)
	} else {
		s.log.Info("ingest: local head err", "chain", chainID, "err", err)
	}
	if h.crossDB != nil {
		if pair, err := h.crossDB.Last(); err == nil {
			s.log.Info("ingest: cross head", "chain", chainID, "l1", pair.Source, "l2", pair.Derived)
		} else {
			s.log.Info("ingest: cross head err", "chain", chainID, "err", err)
		}
	}
}

// ensureDerived performs a guarded AddDerived write that only appends when advancing.
// Returns true if a write was performed.
func (s *Supervisor) ensureDerived(db *fromda.DB, l1Ref eth.BlockRef, l2Ref eth.BlockRef, rev types.Revision) (bool, error) {
	if db == nil {
		return false, nil
	}
	if pair, err := db.Last(); err == nil {
		if pair.Derived.Number >= l2Ref.Number {
			return false, nil
		}
	}
	if err := db.AddDerived(l1Ref, l2Ref, rev); err != nil {
		return false, err
	}
	return true, nil
}

// BootstrapCrossIfEmpty initializes cross DB from local DB once.
func (s *Supervisor) BootstrapCrossIfEmpty(ctx context.Context, h *ChainHandle, l1 *sources.L1Client, l2 *sources.L2Client, rcfg *rollup.Config, chainID uint64) {
	if !h.crossDB.IsEmpty() {
		return
	}
	if pair, err := h.localDB.Last(); err == nil {
		l1Ref, err1 := l1.BlockRefByNumber(ctx, pair.Source.Number)
		env, err2 := l2.PayloadByNumber(ctx, pair.Derived.Number)
		var dref eth.BlockRef
		if err2 == nil {
			if br, derr := derive.PayloadToBlockRef(rcfg, env.ExecutionPayload); derr == nil {
				dref = br.BlockRef()
			}
		}
		if err1 == nil && dref.Number == pair.Derived.Number {
			if did, _ := s.ensureDerived(h.crossDB, l1Ref, dref, types.RevisionAny); did {
				s.log.Info("bootstrap cross DB from local", "chain", chainID, "l1", l1Ref, "l2", dref)
			}
		}
	}
}

// newCrosssafeAdapter builds the adapter with closures to look up per-chain DBs.
func (s *Supervisor) newCrosssafeAdapter(h *ChainHandle, l1 *sources.L1Client, l2 *sources.L2Client, rcfg *rollup.Config, chainID uint64) *crosssafeAdapter {
	return &crosssafeAdapter{
		logger:  s.log,
		chainID: eth.ChainIDFromUInt64(chainID),
		logs:    h.logsDB,
		local:   h.localDB,
		cross:   h.crossDB,
		lookupLogs: func(cid eth.ChainID) (*logsdb.DB, error) {
			if v, ok := cid.Uint64(); ok {
				s.mu.Lock()
				h2 := s.chains[v]
				s.mu.Unlock()
				if h2 != nil {
					return h2.logsDB, nil
				}
			}
			return nil, fmt.Errorf("unknown chain %v", cid)
		},
		lookupLocal: func(cid eth.ChainID) (*fromda.DB, error) {
			if v, ok := cid.Uint64(); ok {
				s.mu.Lock()
				h2 := s.chains[v]
				s.mu.Unlock()
				if h2 != nil {
					return h2.localDB, nil
				}
			}
			return nil, fmt.Errorf("unknown chain %v", cid)
		},
		lookupCross: func(cid eth.ChainID) (*fromda.DB, error) {
			if v, ok := cid.Uint64(); ok {
				s.mu.Lock()
				h2 := s.chains[v]
				s.mu.Unlock()
				if h2 != nil {
					return h2.crossDB, nil
				}
			}
			return nil, fmt.Errorf("unknown chain %v", cid)
		},
		reads:          reads.NewRegistry(s.log),
		l1:             l1,
		l2:             l2,
		addDenylist:    func(cid uint64, id string) error { return s.denylist.Add(cid, id) },
		rollback:       s.RollbackChain,
		l1ConfirmDepth: h.virtualCfg.ConfirmDepth,
		l1ScopeLabel:   s.getL1ScopeLabel(),
	}
}

// RunCrossSafeStep executes one adapter step and logs the outcome.
func (s *Supervisor) RunCrossSafeStep(ctx context.Context, adapter *crosssafeAdapter, chainID uint64) {
	s.log.Info("crosssafe: run", "chain", chainID)
	if err := adapter.runCrossSafeOnce(s.log, s.getLinkChecker()); err != nil {
		msg := err.Error()
		if strings.Contains(msg, "future data") || strings.Contains(msg, "past last entry") {
			s.log.Info("crosssafe: waiting for ingest", "chain", chainID, "err", err)
		} else {
			s.log.Warn("crosssafe: error", "chain", chainID, "err", err)
		}
	}
	if cs, err := adapter.cross.Last(); err == nil {
		s.log.Info("crosssafe: head", "chain", chainID, "derived", cs.Derived)
	} else {
		s.log.Warn("crosssafe: last error", "chain", chainID, "err", err)
	}
}

func (s *Supervisor) xsafeSnapshotActiveChains() []eth.ChainID {
	s.activeChainsMu.Lock()
	defer s.activeChainsMu.Unlock()
	out := make([]eth.ChainID, 0, len(s.activeChains))
	for id := range s.activeChains {
		out = append(out, id)
	}
	return out
}

// getMinGenesisTimestamp computes the minimum L2 genesis timestamp across the provided chain refs.
// Returns 0 if none of the refs have a rollup config.
func (s *Supervisor) getMinGenesisTimestamp(refs []chainRef) uint64 {
	var minGenesis uint64
	for _, r := range refs {
		h := r.h
		if h != nil && h.virtualCfg != nil && h.virtualCfg.Rcfg != nil {
			gts := h.virtualCfg.Rcfg.Genesis.L2Time
			if minGenesis == 0 || gts < minGenesis {
				minGenesis = gts
			}
		}
	}
	return minGenesis
}

// (removed xsafeNextCandidate: compute candidate inline where used)

// blockAtTimestampFromConfig returns the L2 block number whose timestamp is <= ts, using rollup config.
// It clamps to genesis and accounts for non-zero genesis block numbers.
func blockAtTimestampFromConfig(rcfg *rollup.Config, ts uint64) (uint64, error) {
	if rcfg == nil {
		return 0, fmt.Errorf("nil rollup config")
	}
	if rcfg.BlockTime == 0 {
		return 0, fmt.Errorf("blockTime must be a positive integer")
	}
	genesisTime := rcfg.Genesis.L2Time
	genesisNum := rcfg.Genesis.L2.Number
	if ts <= genesisTime {
		return genesisNum, nil
	}
	return genesisNum + ((ts - genesisTime) / rcfg.BlockTime), nil
}

// getBlocksAtTimestamp returns, for each chain, the (L1,L2) block refs corresponding to the floor
// L2 block at the given timestamp. It first gates on having SafeL2 at least at that block number.
func (s *Supervisor) getBlocksAtTimestamp(ctx context.Context, refs []chainRef, ts uint64) (map[uint64]types.DerivedBlockRefPair, error) {
	pairs := make(map[uint64]types.DerivedBlockRefPair, len(refs))
	var l1Cli opclient.RPC
	var l1 *sources.L1Client
	for _, r := range refs {
		v := r.id
		h := r.h
		if h == nil || h.virtualCfg == nil || h.virtualCfg.Rcfg == nil || h.embeddedOpNodeUserRPC == "" {
			return nil, fmt.Errorf("missing handle/config/opnode for chain %d", v)
		}
		// Compute expected L2 block number at ts (floor)
		rcfg := h.virtualCfg.Rcfg
		targetNum, err := blockAtTimestampFromConfig(rcfg, ts)
		if err != nil {
			return nil, fmt.Errorf("chain %d: compute target num: %w", v, err)
		}
		// Gate: SafeL2 must be at or beyond targetNum
		st, err := s.fetchSyncStatus(ctx, h.embeddedOpNodeUserRPC)
		if err != nil || st == nil {
			return nil, fmt.Errorf("chain %d: fetch sync status: %w", v, err)
		}
		if st.SafeL2.Number < targetNum {
			return nil, fmt.Errorf("chain %d: safe head too low: have %d need %d", v, st.SafeL2.Number, targetNum)
		}
		s.log.Info("xsafe: gate ok", "chain", v, "safe_num", st.SafeL2.Number, "need_num", targetNum)
		// Ensure clients
		l1Cli, l1 = s.EnsureL1Client(ctx, l1Cli, l1, h.virtualCfg.L1RPC, rcfg)
		l2Cli, err := opclient.NewRPC(ctx, s.log, h.virtualCfg.L2UserRPC)
		if err != nil {
			return nil, fmt.Errorf("chain %d: dial L2: %w", v, err)
		}
		l2, err := sources.NewL2Client(l2Cli, s.log, nil, sources.L2ClientDefaultConfig(rcfg, true))
		if err != nil {
			l2Cli.Close()
			return nil, fmt.Errorf("chain %d: new L2 client: %w", v, err)
		}
		// Fetch L2 and its L1 origin
		env, err := l2.PayloadByNumber(ctx, targetNum)
		if err != nil {
			l2Cli.Close()
			return nil, fmt.Errorf("chain %d: payload by number %d: %w", v, targetNum, err)
		}
		br, derr := derive.PayloadToBlockRef(rcfg, env.ExecutionPayload)
		if derr != nil {
			l2Cli.Close()
			return nil, fmt.Errorf("chain %d: payload to block ref: %w", v, derr)
		}
		l1Ref, e1 := l1.BlockRefByNumber(ctx, br.L1Origin.Number)
		l2Cli.Close()
		if e1 != nil {
			return nil, fmt.Errorf("chain %d: l1 block by number %d: %w", v, br.L1Origin.Number, e1)
		}
		pairs[v] = types.DerivedBlockRefPair{Source: l1Ref, Derived: br.BlockRef()}
	}
	return pairs, nil
}

// xsafeIngestLogsTo manually seals blocks in logsDB up to the provided target pairs (no ingestRange).
// Expects pre-resolved handles and computed target blocks. Idempotent: skips blocks already sealed.
func (s *Supervisor) xsafeIngestLogsTo(ctx context.Context, refs []chainRef, pairs map[uint64]types.DerivedBlockRefPair) bool {
	ready := true
	for _, r := range refs {
		v := r.id
		h := r.h
		if h == nil || h.virtualCfg == nil || h.virtualCfg.Rcfg == nil {
			continue
		}
		targetPair, ok := pairs[v]
		if !ok {
			// No target for this chain in the pair set
			continue
		}
		targetNum := targetPair.Derived.Number
		if blk, ok := h.logsDB.LatestSealedBlock(); ok {
			s.log.Info("xsafe: logs head before", "chain", v, "num", blk.Number)
			if blk.Number >= targetNum {
				// Already at or past target; skip
				continue
			}
		}
		rcfg := h.virtualCfg.Rcfg
		s.log.Info("xsafe: target computed", "chain", v, "target", targetNum)
		// Build L2 client (EL user RPC)
		l2Cli, err := opclient.NewRPC(ctx, s.log, h.virtualCfg.L2UserRPC)
		if err != nil {
			ready = false
			break
		}
		l2, err := sources.NewL2Client(l2Cli, s.log, nil, sources.L2ClientDefaultConfig(rcfg, true))
		if err != nil {
			l2Cli.Close()
			ready = false
			break
		}
		// Determine starting point
		start := uint64(0)
		if blk, ok := h.logsDB.LatestSealedBlock(); ok {
			start = blk.Number + 1
		}
		// Manually seal blocks up to target, skipping if already sealed
		for num := start; num <= targetNum; num++ {
			env, err := l2.PayloadByNumber(ctx, num)
			if err != nil {
				ready = false
				break
			}
			br, derr := derive.PayloadToBlockRef(rcfg, env.ExecutionPayload)
			if derr != nil {
				ready = false
				break
			}
			// Seal with zero logs (we're not adding per-log entries here)
			if err := h.logsDB.SealBlock(br.ParentHash, br.BlockRef().ID(), br.Time); err != nil {
				s.log.Info("xsafe: seal failed", "chain", v, "num", num, "err", err)
				ready = false
				break
			}
		}
		if !ready {
			l2Cli.Close()
			break
		}
		if blk, ok := h.logsDB.LatestSealedBlock(); ok {
			s.log.Info("xsafe: logs head after", "chain", v, "num", blk.Number)
		}
		l2Cli.Close()
	}
	return ready
}

// getExecutingMessages returns the executing messages per chain at the target block for ts.
func (s *Supervisor) getExecutingMessages(refs []chainRef, ts uint64) map[uint64]map[uint32]*types.ExecutingMessage {
	out := make(map[uint64]map[uint32]*types.ExecutingMessage, len(refs))
	for _, r := range refs {
		v := r.id
		h := r.h
		if h == nil || h.virtualCfg == nil || h.virtualCfg.Rcfg == nil || h.logsDB == nil {
			s.log.Info("xsafe: validation skip (missing cfg/db)", "chain", v)
			continue
		}
		rcfg := h.virtualCfg.Rcfg
		targetNum, err := rcfg.TargetBlockNumber(ts)
		if err != nil {
			s.log.Info("xsafe: validation target before genesis", "chain", v, "ts", ts)
			continue
		}
		_, _, execMsgs, err := h.logsDB.OpenBlock(targetNum)
		if err != nil {
			s.log.Info("xsafe: validation open block failed", "chain", v, "block", targetNum, "err", err)
			continue
		}
		if len(execMsgs) == 0 {
			s.log.Info("xsafe: validation no executing messages", "chain", v, "block", targetNum)
		}
		for logIdx, msg := range execMsgs {
			if msg == nil {
				continue
			}
			s.log.Info("xsafe: exec found", "chain", v, "block", targetNum, "logIdx", logIdx, "init_chain", msg.ChainID, "init_block", msg.BlockNum, "init_log", msg.LogIdx, "ts", msg.Timestamp)
		}
		out[v] = execMsgs
	}
	return out
}

// validateExecutingMessages verifies that each executing message references an initiating log present on the initiating chain.
func (s *Supervisor) validateExecutingMessages(refs []chainRef, execByChain map[uint64]map[uint32]*types.ExecutingMessage) bool {
	allValid := true
	invalidCount := 0
	totalCount := 0
	for _, r := range refs {
		v := r.id
		execMsgs := execByChain[v]
		for _, msg := range execMsgs {
			if msg == nil {
				continue
			}
			totalCount++
			if initCID, ok := msg.ChainID.Uint64(); ok {
				s.mu.Lock()
				initH := s.chains[initCID]
				s.mu.Unlock()
				if initH == nil || initH.logsDB == nil {
					s.log.Info("xsafe: validation missing initiating logsDB", "init_chain", initCID)
					allValid = false
					invalidCount++
					continue
				}
				query := types.ContainsQuery{BlockNum: msg.BlockNum, LogIdx: msg.LogIdx, Timestamp: msg.Timestamp, Checksum: msg.Checksum}
				if _, err := initH.logsDB.Contains(query); err != nil {
					s.log.Info("xsafe: exec validation failed", "exec_chain", v, "init_chain", initCID, "err", err)
					allValid = false
					invalidCount++
				} else {
					s.log.Info("xsafe: exec validation ok", "exec_chain", v, "init_chain", initCID)
				}
			}
		}
	}
	s.log.Info("xsafe: exec validation summary", "total", totalCount, "invalid", invalidCount)
	return allValid
}

func (s *Supervisor) xsafeRollbackPlaceholder(chains []eth.ChainID) {
	for _, id := range chains {
		s.log.Info("xsafe: rollback TODO", "chain", id)
	}
}
