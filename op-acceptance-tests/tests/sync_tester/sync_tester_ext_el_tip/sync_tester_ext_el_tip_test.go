package sync_tester_ext_el

import (
	"fmt"
	"math/big"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/ethereum-optimism/optimism/op-devstack/devtest"
	"github.com/ethereum-optimism/optimism/op-devstack/dsl"
	"github.com/ethereum-optimism/optimism/op-devstack/presets"
	"github.com/ethereum-optimism/optimism/op-devstack/shim"
	"github.com/ethereum-optimism/optimism/op-devstack/stack"
	"github.com/ethereum-optimism/optimism/op-devstack/stack/match"
	"github.com/ethereum-optimism/optimism/op-devstack/sysgo"
	"github.com/ethereum-optimism/optimism/op-node/chaincfg"
	"github.com/ethereum-optimism/optimism/op-service/eth"
	"github.com/ethereum-optimism/optimism/op-service/testlog"

	"github.com/ethereum-optimism/optimism/op-supervisor/supervisor/types"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/rpc"
)

// Configuration defaults for op-sepolia
const (
	DefaultNetworkPreset = "op-sepolia"

	// Tailscale networking endpoints
	DefaultL2ELEndpointTailscale       = "https://proxyd-l2-sepolia.primary.client.dev.oplabs.cloud"
	DefaultL1CLBeaconEndpointTailscale = "https://beacon-api-proxy-sepolia.primary.client.dev.oplabs.cloud"
	DefaultL1ELEndpointTailscale       = "https://proxyd-l1-sepolia.primary.client.dev.oplabs.cloud"
)

var (
	// Network presets for different networks against which we test op-node syncing
	networkPresets = map[string]stack.ExtNetworkConfig{
		"op-sepolia": {
			L2NetworkName:      "op-sepolia",
			L1ChainID:          eth.ChainIDFromUInt64(11155111),
			L2ELEndpoint:       "https://ci-sepolia-l2.optimism.io",
			L1CLBeaconEndpoint: "https://ci-sepolia-beacon.optimism.io",
			L1ELEndpoint:       "https://ci-sepolia-l1.optimism.io",
		},
		"base-sepolia": {
			L2NetworkName:      "base-sepolia",
			L1ChainID:          eth.ChainIDFromUInt64(11155111),
			L2ELEndpoint:       "https://base-sepolia-rpc.optimism.io",
			L1CLBeaconEndpoint: "https://ci-sepolia-beacon.optimism.io",
			L1ELEndpoint:       "https://ci-sepolia-l1.optimism.io",
		},
		"unichain-sepolia": {
			L2NetworkName:      "unichain-sepolia",
			L1ChainID:          eth.ChainIDFromUInt64(11155111),
			L2ELEndpoint:       "https://unichain-sepolia-rpc.optimism.io",
			L1CLBeaconEndpoint: "https://ci-sepolia-beacon.optimism.io",
			L1ELEndpoint:       "https://ci-sepolia-l1.optimism.io",
		},
		"op-mainnet": {
			L2NetworkName:      "op-mainnet",
			L1ChainID:          eth.ChainIDFromUInt64(1),
			L2ELEndpoint:       "https://op-mainnet-rpc.optimism.io",
			L1CLBeaconEndpoint: "https://ci-mainnet-beacon.optimism.io",
			L1ELEndpoint:       "https://ci-mainnet-l1.optimism.io",
		},
		"base-mainnet": {
			L2NetworkName:      "base-mainnet",
			L1ChainID:          eth.ChainIDFromUInt64(1),
			L2ELEndpoint:       "https://base-mainnet-rpc.optimism.io",
			L1CLBeaconEndpoint: "https://ci-mainnet-beacon.optimism.io",
			L1ELEndpoint:       "https://ci-mainnet-l1.optimism.io",
		},
	}
)

func TestSyncTesterExtELTip(gt *testing.T) {
	t := devtest.SerialT(gt)

	l := t.Logger()
	require := t.Require()
	sys, target := setupSystem(gt, t)

	// Default by EL Sync
	attempts := 5
	// Signal L2CL for triggering EL Sync
	sys.L2CL.SignalTarget(sys.L2ELReadOnly, target)

	// Test that we can get sync status from L2CL node
	l2CLSyncStatus := sys.L2CL.SyncStatus()
	require.NotNil(l2CLSyncStatus, "L2CL should have sync status")

	sys.L2CL.Reached(types.LocalUnsafe, target, attempts)

	l2CLSyncStatus = sys.L2CL.SyncStatus()
	require.NotNil(l2CLSyncStatus, "L2CL should have sync status")

	unsafeL2Ref := l2CLSyncStatus.UnsafeL2
	blk := sys.L2EL.BlockRefByNumber(unsafeL2Ref.Number)
	require.Equal(unsafeL2Ref.Hash, blk.Hash, "L2EL should be on the same block as L2CL")

	stSessions := sys.SyncTester.ListSessions()
	require.Equal(len(stSessions), 1, "expect exactly one session")

	stSession := sys.SyncTester.GetSession(stSessions[0])
	require.GreaterOrEqual(stSession.CurrentState.Latest, target, "SyncTester session Latest should be on the same block as L2CL")
	require.GreaterOrEqual(stSession.CurrentState.Safe, target, "SyncTester session Safe should be on the same block as L2CL")

	// Hack: wait until derivation pipeline is reset
	time.Sleep(time.Second * 80)

	// Now fill in the unsafe gap
	for {
		seqUnsafe := sys.L2ELReadOnly.BlockRefByLabel(eth.Unsafe)
		valUnsafe := sys.L2EL.BlockRefByLabel(eth.Unsafe)
		delta := seqUnsafe.Number - valUnsafe.Number
		l.Info("Validator unsafe head", "delta", delta, "seq", seqUnsafe.Number, "val", valUnsafe.Number)
		if valUnsafe.Number < seqUnsafe.Number {
			nextTarget := valUnsafe.Number + 1
			l.Info("Validater chasing the unsafe tip", "tip", nextTarget, "seqTip", seqUnsafe.Number)
			// for i := range 20 { // Hack
			// 	if nextTarget+uint64(i) > seqUnsafe.Number {
			// 		continue
			// 	}
			// 	sys.L2CL.SignalTarget(sys.L2ELReadOnly, nextTarget+uint64(i))
			// }
			// vibe code to parallelize
			var wg sync.WaitGroup
			for i := range 20 { // Better Hack
				currTarget := nextTarget + uint64(i)
				if currTarget > seqUnsafe.Number {
					continue
				}
				wg.Add(1)
				go func(t uint64) {
					defer wg.Done()
					sys.L2CL.SignalTarget(sys.L2ELReadOnly, t)
				}(currTarget)
			}
			wg.Wait()
		} else {
			l.Info("Validator unsafe tip reached sequencer unsafe tip", "tip", seqUnsafe.Number)
			// block Hash check
			require.Equal(sys.L2ELReadOnly.BlockRefByNumber(valUnsafe.Number).Hash, sys.L2EL.BlockRefByNumber(valUnsafe.Number).Hash, "hash mismatch :C")
			break
		}
	}
}

func setupSystem(gt *testing.T, t devtest.T) (*presets.MinimalExternalEL, uint64) {
	// Initialize orchestrator
	orch, target := setupOrchestrator(gt, t)
	system := shim.NewSystem(t)
	orch.Hydrate(system)

	// Extract the system components
	l2 := system.L2Network(match.L2ChainA)
	verifierCL := l2.L2CLNode(match.FirstL2CL)
	syncTester := l2.SyncTester(match.FirstSyncTester)

	sys := &presets.MinimalExternalEL{
		Log:          t.Logger(),
		T:            t,
		ControlPlane: orch.ControlPlane(),
		L1Network:    dsl.NewL1Network(system.L1Network(match.FirstL1Network)),
		L1EL:         dsl.NewL1ELNode(system.L1Network(match.FirstL1Network).L1ELNode(match.FirstL1EL)),
		L2Chain:      dsl.NewL2Network(l2, orch.ControlPlane()),
		L2CL:         dsl.NewL2CLNode(verifierCL, orch.ControlPlane()),
		L2ELReadOnly: dsl.NewL2ELNode(l2.L2ELNode(match.FirstL2EL), orch.ControlPlane()),
		L2EL:         dsl.NewL2ELNode(l2.L2ELNode(match.SecondL2EL), orch.ControlPlane()),
		SyncTester:   dsl.NewSyncTester(syncTester),
	}

	return sys, target
}

func setupOrchestrator(gt *testing.T, t devtest.T) (*sysgo.Orchestrator, uint64) {
	l := t.Logger()
	ctx := t.Ctx()
	require := t.Require()

	config := networkPresets[DefaultNetworkPreset]

	// Override configuration with Tailscale endpoints if Tailscale networking is enabled
	if os.Getenv("TAILSCALE_NETWORKING") == "true" {
		config.L2ELEndpoint = getEnvOrDefault("L2_EL_ENDPOINT_TAILSCALE", DefaultL2ELEndpointTailscale)
		config.L1CLBeaconEndpoint = getEnvOrDefault("L1_CL_BEACON_ENDPOINT_TAILSCALE", DefaultL1CLBeaconEndpointTailscale)
		config.L1ELEndpoint = getEnvOrDefault("L1_EL_ENDPOINT_TAILSCALE", DefaultL1ELEndpointTailscale)
	}

	if os.Getenv("NETWORK_PRESET") != "" {
		var ok bool
		config, ok = networkPresets[os.Getenv("NETWORK_PRESET")]
		if !ok {
			gt.Errorf("NETWORK_PRESET %s not found", os.Getenv("NETWORK_PRESET"))
		}
	}

	// Runtime configuration values
	l.Info("Runtime configuration values for TestSyncTesterExtEL")
	l.Info("NETWORK_PRESET", "value", os.Getenv("NETWORK_PRESET"))
	l.Info("L2_NETWORK_NAME", "value", config.L2NetworkName)
	l.Info("L1_CHAIN_ID", "value", config.L1ChainID)
	l.Info("L2_EL_ENDPOINT", "value", config.L2ELEndpoint)
	l.Info("L1_CL_BEACON_ENDPOINT", "value", config.L1CLBeaconEndpoint)
	l.Info("L1_EL_ENDPOINT", "value", config.L1ELEndpoint)
	l.Info("TAILSCALE_NETWORKING", "value", os.Getenv("TAILSCALE_NETWORKING"))

	// Setup orchestrator
	logger := testlog.Logger(gt, log.LevelInfo)
	onFail := func(now bool) {
		if now {
			gt.FailNow()
		} else {
			gt.Fail()
		}
	}
	onSkipNow := func() {
		gt.SkipNow()
	}
	p := devtest.NewP(ctx, logger, onFail, onSkipNow)
	gt.Cleanup(p.Close)

	// Fetch the latest block number from the remote L2EL node
	cl, err := ethclient.DialContext(ctx, config.L2ELEndpoint)
	require.NoError(err)
	safeBlock, err := cl.BlockByNumber(ctx, big.NewInt(rpc.SafeBlockNumber.Int64()))
	require.NoError(err)

	initial := safeBlock.NumberU64() - 10
	target := safeBlock.NumberU64()
	l.Info("LATEST_BLOCK", "session_initial_block", initial, "target_block", target)

	opt := presets.WithExternalELWithSuperchainRegistry(config)

	chainCfg := chaincfg.ChainByName(config.L2NetworkName)
	if chainCfg == nil {
		panic(fmt.Sprintf("network %s not found in superchain registry", config.L2NetworkName))
	}
	opt = stack.Combine(opt,
		presets.WithExecutionLayerSyncOnVerifiers(),
		presets.WithELSyncTarget(target),
		presets.WithSyncTesterELInitialState(eth.FCUState{
			Latest: initial,
			Safe:   0,
			// Need to set finalized to genesis to unskip EL Sync
			Finalized: chainCfg.Genesis.L2.Number,
		}),
	)
	{
		// TODO(#17564): op-node has a suspected race during EL Sync.
		// To temporarily mitigate and stabilize tests, restrict runtime
		// parallelism to 1 (no true concurrency). This masks the race;
		// remove once the underlying issue is fixed.

		// Temporal comment out this for unsafe tip sync
		// runtime.GOMAXPROCS(1)
	}

	var orch stack.Orchestrator = sysgo.NewOrchestrator(p, stack.SystemHook(opt))
	stack.ApplyOptionLifecycle(opt, orch)

	return orch.(*sysgo.Orchestrator), target
}

// getEnvOrDefault returns the environment variable value or the default if not set
func getEnvOrDefault(envVar, defaultValue string) string {
	if value := os.Getenv(envVar); value != "" {
		return value
	}
	return defaultValue
}
