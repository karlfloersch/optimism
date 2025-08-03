package backend

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"

	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/rpc"

	"github.com/ethereum-optimism/optimism/op-service/eth"
	"github.com/ethereum-optimism/optimism/op-service/locks"
	"github.com/ethereum-optimism/optimism/op-sync-tester/metrics"
	"github.com/ethereum-optimism/optimism/op-sync-tester/synctester/backend/config"
	"github.com/ethereum-optimism/optimism/op-sync-tester/synctester/frontend"

	sttypes "github.com/ethereum-optimism/optimism/op-sync-tester/synctester/backend/types"
)

type sessionKeyType struct{}

var ctxKeySession = sessionKeyType{}

// WithSession returns a new context with the given Session.
func WithSession(ctx context.Context, s *Session) context.Context {
	return context.WithValue(ctx, ctxKeySession, s)
}

// SessionFromContext retrieves the Session from the context, if present.
func SessionFromContext(ctx context.Context) (*Session, bool) {
	s, ok := ctx.Value(ctxKeySession).(*Session)
	return s, ok
}

type Session struct {
	SessionID string

	// Mutex for thread-safe progress updates
	mu sync.RWMutex

	// Initial offsets from real chain (for initialization)
	InitialLatestOffset    uint64
	InitialSafeOffset      uint64
	InitialFinalizedOffset uint64

	// Progress-driven heads (op-node controls advancement)
	AvailableLatestHead    uint64 // Highest block op-node can request for "latest"
	AvailableSafeHead      uint64 // Highest block op-node can request for "safe"
	AvailableFinalizedHead uint64 // Highest block op-node can request for "finalized"

	Initialized bool // Track if session has been initialized
}

// AdvanceProgress updates available heads based on op-node's actual ForkchoiceState values
func (s *Session) AdvanceProgress(latestBlock, safeBlock, finalizedBlock uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// op-node successfully processed these blocks - make next block available as "latest"
	nextLatest := latestBlock + 1
	if nextLatest > s.AvailableLatestHead {
		s.AvailableLatestHead = nextLatest
	}

	// Use op-node's actual safe and finalized values (not calculated!)
	if safeBlock > s.AvailableSafeHead {
		s.AvailableSafeHead = safeBlock
	}

	if finalizedBlock > s.AvailableFinalizedHead {
		s.AvailableFinalizedHead = finalizedBlock
	}
}

// GetAvailableHeads safely returns current available head values
func (s *Session) GetAvailableHeads() (latest, safe, finalized uint64) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.AvailableLatestHead, s.AvailableSafeHead, s.AvailableFinalizedHead
}

type APIRouter interface {
	AddRPC(route string) error
	AddAPIToRPC(route string, api rpc.API) error
}

type Backend struct {
	log log.Logger
	m   metrics.Metricer

	syncTesters locks.RWMap[sttypes.SyncTesterID, *SyncTester]
}

func (b *Backend) Stop(ctx context.Context) error {
	// We have support for ctx/error here,
	// for future improvements like awaiting txs to complete and/or storing rate-limit data to disk.
	return nil
}

func FromConfig(log log.Logger, m metrics.Metricer, cfg *config.Config, router APIRouter) (*Backend, error) {
	b := &Backend{
		log: log,
		m:   m,
	}
	var syncTesterIDs []sttypes.SyncTesterID
	for stID, stCfg := range cfg.SyncTesters {
		st, err := SyncTesterFromConfig(log, m, stID, stCfg)
		if err != nil {
			return nil, fmt.Errorf("failed to setup sync tester %q: %w", stID, err)
		}
		b.syncTesters.Set(stID, st)
		syncTesterIDs = append(syncTesterIDs, stID)
	}
	// Infer defaults for chains that were not explicitly mentioned.
	// Always use the lowest sync tester ID, so map-iteration doesn't affect defaults.
	sort.Slice(syncTesterIDs, func(i, j int) bool {
		return syncTesterIDs[i] < syncTesterIDs[j]
	})
	// Set up the sync tester routes
	var syncTesterErr error
	b.syncTesters.Range(func(id sttypes.SyncTesterID, st *SyncTester) bool {
		path := "/chain/" + st.chainID.String() + "/synctest"
		if err := router.AddRPC(path); err != nil {
			syncTesterErr = errors.Join(fmt.Errorf("failed to set up synctest route: %w", err))
			return true
		}
		if err := router.AddAPIToRPC(path, rpc.API{
			Namespace: "sync",
			Service:   frontend.NewSyncFrontend(st),
		}); err != nil {
			syncTesterErr = errors.Join(syncTesterErr, fmt.Errorf("failed to add sync API: %w", err))
		}
		if err := router.AddAPIToRPC(path, rpc.API{
			Namespace: "eth",
			Service:   frontend.NewEthFrontend(st),
		}); err != nil {
			syncTesterErr = errors.Join(syncTesterErr, fmt.Errorf("failed to add eth API: %w", err))
		}
		if err := router.AddAPIToRPC(path, rpc.API{
			Namespace: "engine",
			Service:   frontend.NewEngineFrontend(st),
		}); err != nil {
			syncTesterErr = errors.Join(syncTesterErr, fmt.Errorf("failed to add engine API: %w", err))
		}
		return true
	})
	if syncTesterErr != nil {
		return nil, fmt.Errorf("failed to set up sync tester route(s): %w", syncTesterErr)
	}
	return b, nil
}

func (b *Backend) SyncTesters() (out map[sttypes.SyncTesterID]eth.ChainID) {
	out = make(map[sttypes.SyncTesterID]eth.ChainID)
	b.syncTesters.Range(func(key sttypes.SyncTesterID, value *SyncTester) bool {
		out[key] = value.chainID
		return true
	})
	return out
}
