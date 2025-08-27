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

	// testability hooks
	fetchSyncStatus func(ctx context.Context, rpc string) (*eth.SyncStatus, error)
	rollbackFn      func(ctx context.Context, chainID uint64, toBlock uint64) error
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

func (s *Supervisor) ProgressCrossSafe() {
	oldMinTimestamp := uint64(0)
	for {
		s.log.Info("ProgressCrossSafe: loop start")
		ctx := context.Background()

		// copy active chains to avoid race conditions
		s.activeChainsMu.Lock()
		activeChains := make([]eth.ChainID, 0, len(s.activeChains))
		for id := range s.activeChains {
			activeChains = append(activeChains, id)
		}
		s.activeChainsMu.Unlock()

		// step 1: identify the minimum latest timestamp across all chains
		var minTimestamp uint64
		for _, id := range activeChains {
			chainID, _ := id.Uint64()
			s.mu.Lock()
			h := s.chains[chainID]
			s.mu.Unlock()

			if h == nil {
				continue
			}

			h.stateMu.Lock()
			userRPC := h.embeddedOpNodeUserRPC
			h.stateMu.Unlock()

			if userRPC == "" {
				continue
			}

			// Get latest safe timestamp from sync status
			if st, err := s.fetchSyncStatus(ctx, userRPC); err == nil && st != nil {
				latestTimestamp := st.SafeL2.Time
				s.log.Info("ProgressCrossSafe: latest timestamp", "chain", chainID, "timestamp", latestTimestamp)
				// if unset or less than minTimestamp, update minTimestamp
				if minTimestamp == 0 || (latestTimestamp > 0 && latestTimestamp < minTimestamp) {
					minTimestamp = latestTimestamp
				}
			}
		}

		// step 1.5
		// check L1 blocks based on the minimums and confirm that confirmation depth (L1) is met
		// recede any chains outside of finality space to just the L1 finality block

		// step 2: if the minimum timestamp has changed, ensure all chains have ingested up to the minimum timestamp
		if minTimestamp > 0 && minTimestamp != oldMinTimestamp {
			s.log.Info("ProgressCrossSafe: minimum timestamp changed", "old", oldMinTimestamp, "new", minTimestamp)
			oldMinTimestamp = minTimestamp

			for _, id := range activeChains {
				// identify target block based on timestamp
				chainID, _ := id.Uint64()
				h := s.chains[chainID]
				if h == nil {
					continue
				}
				rcfg := h.virtualCfg.Rcfg
				targetNumber := uint64(0)
				timestamp := uint64(0)
				for timestamp < minTimestamp {
					targetNumber++
					timestamp = rcfg.TimestampForBlock(targetNumber)
				}
				s.log.Info("ProgressCrossSafe: would ingest to local safe", "chain", id, "timestamp", timestamp, "blockHeight", targetNumber)
				// TODO: ingest to local safe
			}
		}

		// step 3: for each chain, run cross safe validation
		for _, id := range activeChains {
			s.log.Info("ProgressCrossSafe: *would be* running cross safe validation", "chain", id)
		}

		// step 4: for any failures, invalidate / denylist etc
		for _, id := range activeChains {
			s.log.Info("ProgressCrossSafe: *would be* considering invalidating / denyinglist", "chain", id)
		}

		// step 999: sleep for 1 second
		time.Sleep(1 * time.Second)
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
