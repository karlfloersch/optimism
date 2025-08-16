package supervisor

import (
	"context"
	"flag"
	"fmt"
	"time"

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
)

// StartEmbeddedOpNode starts an embedded op-node in-process with minimal configuration and returns the user-RPC URL
// and a function to stop the embedded op-node. The node is configured as a sequencer with local RPCs and no external P2P.
func (s *Supervisor) StartEmbeddedOpNode(l1RPC string, beaconAddr string, l2AuthRPC string, jwtSecret [32]byte, rcfg *rollup.Config) (string, func(context.Context) error, error) {
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
		Driver:        driver.Config{SequencerEnabled: true, SequencerConfDepth: 2},
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

	opNode, err := e2eopnode.NewOpnode(s.log, nodeCfg, func(err error) {
		if err != nil {
			s.log.Error("embedded op-node error", "err", err)
		}
	})
	if err != nil {
		return "", nil, fmt.Errorf("start embedded op-node: %w", err)
	}

	// Return user RPC endpoint for polling
	stopFn := func(ctx context.Context) error { return opNode.Stop(ctx) }
	return opNode.UserRPC().RPC(), stopFn, nil
}
