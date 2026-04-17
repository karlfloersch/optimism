package sysgo

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/log"

	"github.com/ethereum-optimism/optimism/op-devstack/devtest"
	"github.com/ethereum-optimism/optimism/op-node/config"
	"github.com/ethereum-optimism/optimism/op-service/eth"
	snconfig "github.com/ethereum-optimism/optimism/op-supernode/config"
	"github.com/ethereum-optimism/optimism/op-supernode/supernode"
	suptypes "github.com/ethereum-optimism/optimism/op-supervisor/supervisor/types"
)

var errSupernodeNotRunning = errors.New("sysgo: supernode is not running")

type SuperNode struct {
	mu               sync.Mutex
	sn               *supernode.Supernode
	cancel           context.CancelFunc
	userRPC          string
	interopEndpoint  string
	interopJwtSecret eth.Bytes32
	p                devtest.CommonT
	logger           log.Logger
	chains           []eth.ChainID
	l1UserRPC        string
	l1BeaconAddr     string

	// Configs stored for Start().
	snCfg  *snconfig.CLIConfig
	vnCfgs map[eth.ChainID]*config.Config
}

var _ L2CLNode = (*SuperNode)(nil)

func (n *SuperNode) UserRPC() string {
	return n.userRPC
}

func (n *SuperNode) InteropRPC() (endpoint string, jwtSecret eth.Bytes32) {
	return n.interopEndpoint, n.interopJwtSecret
}

func (n *SuperNode) Start() {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.sn != nil {
		n.logger.Warn("Supernode already started")
		return
	}

	n.p.Require().NotNil(n.snCfg, "supernode CLI config required")

	ctx, cancel := context.WithCancel(n.p.Ctx())
	exitFn := func(err error) { n.p.Errorf("supernode critical error: %v", err) }
	sn, err := supernode.New(ctx, n.logger, "devstack", exitFn, n.snCfg, n.vnCfgs)
	n.p.Require().NoError(err, "supernode failed to create")
	n.sn = sn
	n.cancel = cancel

	n.p.Require().NoError(n.sn.Start(ctx))

	addr, err := n.sn.WaitRPCAddr(ctx)
	n.p.Require().NoError(err, "supernode failed to bind RPC address")
	base := "http://" + addr
	n.userRPC = base
	n.interopEndpoint = base
}

func (n *SuperNode) Stop() {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.sn == nil {
		n.logger.Warn("Supernode already stopped")
		return
	}
	if n.cancel != nil {
		n.cancel()
	}
	// Attempt graceful stop
	stopCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = n.sn.Stop(stopCtx)
	n.sn = nil
}

// PauseInteropActivity pauses the interop activity at the given timestamp.
// This function is for integration test control only.
func (n *SuperNode) PauseInteropActivity(ts uint64) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.sn != nil {
		n.sn.PauseInteropActivity(ts)
	}
}

// ResumeInteropActivity clears any pause on the interop activity.
// This function is for integration test control only.
func (n *SuperNode) ResumeInteropActivity() {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.sn != nil {
		n.sn.ResumeInteropActivity()
	}
}

// RestartInteropActivity stops the running interop activity, optionally
// wipes its on-disk logs DBs, and launches a fresh instance against the
// still-running supernode. For integration test control only.
func (n *SuperNode) RestartInteropActivity(wipeLogsDBs bool, preInjectBackfillFailures int32) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.sn == nil {
		return errSupernodeNotRunning
	}
	return n.sn.RestartInteropActivity(wipeLogsDBs, preInjectBackfillFailures)
}

// InteropBackfillAttempts returns the number of log-backfill attempts the
// running interop activity has made since its most recent (re)start.
// For integration test control only.
func (n *SuperNode) InteropBackfillAttempts() int32 {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.sn == nil {
		return 0
	}
	return n.sn.InteropBackfillAttempts()
}

// InteropBackfillCompleted reports whether the running interop activity has
// finished its log backfill phase for the current Start.
// For integration test control only.
func (n *SuperNode) InteropBackfillCompleted() bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.sn == nil {
		return false
	}
	return n.sn.InteropBackfillCompleted()
}

// InteropActivationTimestamp returns the immutable protocol activation
// timestamp of the running interop activity, or 0 if the supernode is stopped.
// For integration test control only.
func (n *SuperNode) InteropActivationTimestamp() uint64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.sn == nil {
		return 0
	}
	return n.sn.InteropActivationTimestamp()
}

// InteropRuntimeActivationTimestamp returns the runtime activation timestamp
// of the running interop activity, or 0 if the supernode is stopped.
// For integration test control only.
func (n *SuperNode) InteropRuntimeActivationTimestamp() uint64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.sn == nil {
		return 0
	}
	return n.sn.InteropRuntimeActivationTimestamp()
}

// InteropFirstSealedBlock returns the earliest block sealed in the interop
// logs DB for the given chain. For integration test control only.
func (n *SuperNode) InteropFirstSealedBlock(chainID eth.ChainID) (suptypes.BlockSeal, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.sn == nil {
		return suptypes.BlockSeal{}, errSupernodeNotRunning
	}
	return n.sn.InteropFirstSealedBlock(chainID)
}

// InteropLatestSealedBlock returns the most recent block sealed in the interop
// logs DB for the given chain. For integration test control only.
func (n *SuperNode) InteropLatestSealedBlock(chainID eth.ChainID) (suptypes.BlockSeal, bool, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.sn == nil {
		return suptypes.BlockSeal{}, false, errSupernodeNotRunning
	}
	return n.sn.InteropLatestSealedBlock(chainID)
}

// SuperNodeProxy is a thin wrapper that points to a shared supernode instance.
type SuperNodeProxy struct {
	p                devtest.CommonT
	logger           log.Logger
	userRPC          string
	interopEndpoint  string
	interopJwtSecret eth.Bytes32
}

var _ L2CLNode = (*SuperNodeProxy)(nil)

func (n *SuperNodeProxy) Start()          {}
func (n *SuperNodeProxy) Stop()           {}
func (n *SuperNodeProxy) UserRPC() string { return n.userRPC }
func (n *SuperNodeProxy) InteropRPC() (endpoint string, jwtSecret eth.Bytes32) {
	return n.interopEndpoint, n.interopJwtSecret
}

// SupernodeConfig holds configuration options for the shared supernode.
type SupernodeConfig struct {
	// InteropActivationTimestamp enables the interop activity at the given timestamp.
	// Set to nil to disable interop (default). Non-nil (including 0) enables interop.
	InteropActivationTimestamp *uint64

	// UseGenesisInterop, when true, sets InteropActivationTimestamp to the genesis
	// timestamp of the first configured chain at deploy time. Takes effect inside
	// withSharedSupernodeCLsImpl after deployment, when the genesis time is known.
	UseGenesisInterop bool
}

// SupernodeOption is a functional option for configuring the supernode.
type SupernodeOption func(*SupernodeConfig)

// WithSupernodeInterop enables the interop activity with the given activation timestamp.
func WithSupernodeInterop(activationTimestamp uint64) SupernodeOption {
	return func(cfg *SupernodeConfig) {
		ts := activationTimestamp
		cfg.InteropActivationTimestamp = &ts
	}
}

// WithSupernodeInteropAtGenesis enables interop at the genesis timestamp of the first
// configured chain. The timestamp is resolved after deployment, when genesis is known.
func WithSupernodeInteropAtGenesis() SupernodeOption {
	return func(cfg *SupernodeConfig) {
		cfg.UseGenesisInterop = true
	}
}
