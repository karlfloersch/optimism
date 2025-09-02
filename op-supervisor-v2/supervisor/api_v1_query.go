package supervisor

import (
	"encoding/json"
	"net/http"
	"sort"

	"github.com/ethereum-optimism/optimism/op-service/eth"
	"github.com/ethereum-optimism/optimism/op-supervisor/supervisor/types"
)

// addV1QueryEndpoints registers lightweight HTTP endpoints that mirror a subset of v1 supervisor query APIs.
// These are HTTP endpoints (not JSON-RPC) for simplicity, returning the same shapes as v1 types where applicable.
func (s *Supervisor) addV1QueryEndpoints(mux *http.ServeMux) {
	// GET /v1/local_safe?chainId=
	mux.HandleFunc("/v1/local_safe", func(w http.ResponseWriter, r *http.Request) {
		_, h := s.resolveChainFromQuery(w, r)
		if h == nil {
			return
		}
		// v2 Note: localDB was removed - always return empty result for API compatibility
		var out types.DerivedIDPair
		_ = json.NewEncoder(w).Encode(out)
	})

	// GET /v1/cross_safe?chainId=
	mux.HandleFunc("/v1/cross_safe", func(w http.ResponseWriter, r *http.Request) {
		_, h := s.resolveChainFromQuery(w, r)
		if h == nil {
			return
		}
		var out types.DerivedIDPair
		// Compute derived number from global crossSafeTimestamp; ignore hash
		s.mu.Lock()
		ts := s.crossSafeTimestamp
		s.mu.Unlock()
		if ts > 0 && h.virtualCfg != nil && h.virtualCfg.Rcfg != nil {
			if num, err := h.virtualCfg.Rcfg.TargetBlockNumber(ts); err == nil {
				out.Derived.Number = num
			}
		}
		_ = json.NewEncoder(w).Encode(out)
	})

	// GET /v1/finalized?chainId=
	mux.HandleFunc("/v1/finalized", func(w http.ResponseWriter, r *http.Request) {
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
	})

	// GET /v1/finalized_l1
	mux.HandleFunc("/v1/finalized_l1", func(w http.ResponseWriter, r *http.Request) {
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
	})

	// GET /v1/cross_derived_to_source?chainId=&derived=NUM
	mux.HandleFunc("/v1/cross_derived_to_source", func(w http.ResponseWriter, r *http.Request) {
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
	})

	// GET /v1/superroot_at_ts?timestamp= — not supported in SV2; return an explicit error
	mux.HandleFunc("/v1/superroot_at_ts", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotImplemented)
		_ = json.NewEncoder(w).Encode(map[string]string{
			"error": "superroot endpoint is not supported in Supervisor v2",
		})
	})
}

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
