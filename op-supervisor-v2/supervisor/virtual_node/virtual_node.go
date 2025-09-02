package virtual_node

import (
	"context"
	"flag"
	"fmt"
	"time"

	"github.com/ethereum/go-ethereum/log"

	altda "github.com/ethereum-optimism/optimism/op-alt-da"
	e2eopnode "github.com/ethereum-optimism/optimism/op-e2e/e2eutils/opnode"
	opNodeConfig "github.com/ethereum-optimism/optimism/op-node/config"
	opNodeFlags "github.com/ethereum-optimism/optimism/op-node/flags"
	p2pcli "github.com/ethereum-optimism/optimism/op-node/p2p/cli"
	"github.com/ethereum-optimism/optimism/op-node/rollup"
	"github.com/ethereum-optimism/optimism/op-node/rollup/driver"
	"github.com/ethereum-optimism/optimism/op-node/rollup/interop"
	nodeSync "github.com/ethereum-optimism/optimism/op-node/rollup/sync"
	opmetrics "github.com/ethereum-optimism/optimism/op-service/metrics"
	"github.com/ethereum-optimism/optimism/op-service/oppprof"
	oprpc "github.com/ethereum-optimism/optimism/op-service/rpc"
	"github.com/ethereum-optimism/optimism/op-service/sources"
	"github.com/urfave/cli/v2"

	// derive sequencer mode from env to avoid struct coupling
	"os"
	"strings"
)

type VirtualNodeConfig struct {
	L1RPC        string
	BeaconAddr   string
	L2AuthRPC    string
	L2UserRPC    string
	JwtSecret    [32]byte
	Rcfg         *rollup.Config
	Interval     time.Duration
	ConfirmDepth uint64
}

// StartVirtualNode starts a virtual op-node in-process with minimal configuration and returns the user-RPC URL
// and a function to stop the virtual op-node. The node is configured as a sequencer with local RPCs and no external P2P.
func StartVirtualNode(
	l1RPC string,
	beaconAddr string,
	l2AuthRPC string,
	jwtSecret [32]byte,
	rcfg *rollup.Config,
	logger log.Logger,
) (string, func(context.Context) error, error) {
	// Minimal P2P config (memory-only, local addresses)
	fs := flag.NewFlagSet("", flag.ContinueOnError)
	for _, f := range opNodeFlags.P2PFlags(opNodeFlags.EnvVarPrefix) {
		_ = f.Apply(fs)
	}
	_ = fs.Set(opNodeFlags.AdvertiseIPName, "127.0.0.1")
	_ = fs.Set(opNodeFlags.AdvertiseTCPPortName, "0")
	_ = fs.Set(opNodeFlags.AdvertiseUDPPortName, "0")
	_ = fs.Set(opNodeFlags.ListenIPName, "127.0.0.1")
	_ = fs.Set(opNodeFlags.ListenTCPPortName, "0")
	_ = fs.Set(opNodeFlags.ListenUDPPortName, "0")
	_ = fs.Set(opNodeFlags.DiscoveryPathName, "memory")
	_ = fs.Set(opNodeFlags.PeerstorePathName, "memory")
	// Do not set bootnodes; remain isolated
	_ = fs.Set(opNodeFlags.BootnodesName, "")
	cliCtx := cli.NewContext(&cli.App{}, fs, nil)
	p2pConfig, _ := p2pcli.NewConfig(cliCtx, rcfg.BlockTime)

	// Build op-node config
	enabled := true
	if v := strings.ToLower(os.Getenv("SV2_SEQUENCER_ENABLED")); v != "" {
		if v == "0" || v == "false" || v == "no" || v == "off" {
			enabled = false
		}
	}
	nodeCfg := &opNodeConfig.Config{
		L1: &opNodeConfig.L1EndpointConfig{
			L1NodeAddr: l1RPC,
			L1TrustRPC: false,
			// Use debug geth RPC kind to expose extra endpoints in tests
			L1RPCKind:        sources.RPCKindDebugGeth,
			RateLimit:        0,
			BatchSize:        20,
			HttpPollInterval: 100 * time.Millisecond,
			MaxConcurrency:   10,
			CacheSize:        0,
		},
		L2: &opNodeConfig.L2EndpointConfig{
			L2EngineAddr:      l2AuthRPC,
			L2EngineJWTSecret: jwtSecret,
		},
		Beacon:        &opNodeConfig.L1BeaconEndpointConfig{BeaconAddr: beaconAddr},
		Driver:        driver.Config{SequencerEnabled: enabled, SequencerConfDepth: 2},
		Rollup:        *rcfg,
		RPC:           oprpc.CLIConfig{ListenAddr: "127.0.0.1", ListenPort: 0, EnableAdmin: true},
		InteropConfig: &interop.Config{},
		P2P:           p2pConfig,
		Sync: nodeSync.Config{
			SyncMode: nodeSync.CLSync,
		},
		ConfigPersistence:               opNodeConfig.DisabledConfigPersistence{},
		Metrics:                         opmetrics.CLIConfig{},
		Pprof:                           oppprof.CLIConfig{},
		SafeDBPath:                      "",
		RollupHalt:                      "",
		AltDA:                           altda.CLIConfig{},
		IgnoreMissingPectraBlobSchedule: false,
		ExperimentalOPStackAPI:          true,
	}

	opNode, err := e2eopnode.NewOpnode(logger, nodeCfg, func(err error) {
		if err != nil {
			logger.Error("virtual op-node error", "err", err)
		}
	})
	if err != nil {
		return "", nil, fmt.Errorf("start virtual op-node: %w", err)
	}

	stopFn := func(ctx context.Context) error { return opNode.Stop(ctx) }
	return opNode.UserRPC().RPC(), stopFn, nil
}
