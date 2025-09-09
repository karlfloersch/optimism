package chain

import (
	"context"
	"sync"
	"time"

	logsdb "github.com/ethereum-optimism/optimism/op-supervisor/supervisor/backend/db/logs"
)

// ChainContainer tracks the per-chain state (virtual op-node lifecycle, DBs, and pollers).
type ChainContainer struct {
	StateMu sync.Mutex

	// runtime state
	VirtualOpNodeUserRPC string
	StopVirtualOpNode    func(ctx context.Context) error
	CancelPoll           context.CancelFunc
	Started              time.Time

	// config for restart/rollback
	VirtualCfg *VirtualNodeConfig

	// v1 DBs per chain
	LogsDB *logsdb.DB
	// Note: localDB and crossDB removed in v2 - they were never written to and always returned empty data
}
