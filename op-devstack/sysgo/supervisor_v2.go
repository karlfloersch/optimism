package sysgo

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/big"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"time"

	bss "github.com/ethereum-optimism/optimism/op-batcher/batcher"
	"github.com/ethereum-optimism/optimism/op-chain-ops/devkeys"
	"github.com/ethereum-optimism/optimism/op-devstack/devtest"
	"github.com/ethereum-optimism/optimism/op-devstack/shim"
	"github.com/ethereum-optimism/optimism/op-devstack/stack"
	"github.com/ethereum-optimism/optimism/op-devstack/stack/match"
	"github.com/ethereum-optimism/optimism/op-service/cliapp"
	opclient "github.com/ethereum-optimism/optimism/op-service/client"
	"github.com/ethereum-optimism/optimism/op-service/eth"
	"github.com/ethereum-optimism/optimism/op-service/retry"
	service "github.com/ethereum-optimism/optimism/op-supervisor-v2"
	"github.com/ethereum/go-ethereum/log"
)

// Default confirmation depth used by supervisor-v2 cross-safety gating in devstack presets.
const sv2SupervisorConfirmDepth uint64 = 40

// Default L1 confirmation depth used by embedded op-node (sequencer)
const opNodeConfDepth uint64 = 2

// SupervisorV2 runs the Supervisor v2 prototype in-process with an HTTP server
// and a polling loop against an existing L2CL (op-node) and L2EL (op-geth).
type SupervisorV2 struct {
	mu sync.Mutex

	id     stack.SupervisorID
	logger log.Logger
	p      devtest.P

	httpURL string
	lc      cliapp.Lifecycle

	// no extra fields needed; op-node is managed by the supervisor-v2 package
}

func (s *SupervisorV2) hydrate(sys stack.ExtensibleSystem) {
	// Register typed L2CL frontends against the per-chain embedded op-node RPC via SV2 HTTP reverse proxy.
	if s.lc == nil || s.HTTP() == "" {
		return
	}
	l2Nets := sys.L2Networks()
	if len(l2Nets) == 0 {
		return
	}
	base := s.HTTP()
	for _, net := range l2Nets {
		cid, _ := net.ID().ChainID().Uint64()
		url := fmt.Sprintf("%s/opnode/%d/", base, cid)
		cli, err := opclient.NewRPC(sys.T().Ctx(), sys.Logger(), url, opclient.WithLazyDial())
		sys.T().Require().NoError(err)
		sys.T().Cleanup(cli.Close)
		clID := stack.NewL2CLNodeID("embedded", net.ID().ChainID())
		clShim := shim.NewL2CLNode(shim.L2CLNodeConfig{
			CommonConfig: shim.NewCommonConfig(sys.T()),
			ID:           clID,
			Client:       cli,
		})
		// Link to the EL in this network and register
		clShim.(stack.LinkableL2CLNode).LinkEL(net.L2ELNode(stack.NewL2ELNodeID("sequencer", net.ID().ChainID())))
		net.(stack.ExtensibleL2Network).AddL2CLNode(clShim)
	}
}

func (s *SupervisorV2) Start(opNodeAddr, l2Addr string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.lc != nil {
		s.logger.Warn("Supervisor v2 already started")
		return
	}
	port := 0
	if p := os.Getenv("OP_SV2_HTTP_PORT"); p != "" {
		if n, err := strconv.Atoi(p); err == nil {
			port = n
		}
	}
	cfg := &service.Config{HTTPAddr: "127.0.0.1", HTTPPort: port, ProxyOpNode: true, DataDir: os.Getenv("SV2_DATA_DIR")}
	lc, err := service.New(context.Background(), cfg, s.logger, "devstack", nil)
	s.p.Require().NoError(err)
	s.p.Require().NoError(lc.Start(s.p.Ctx()))
	if addr, ok := service.HTTPAddr(lc); ok {
		s.httpURL = "http://" + addr
	}
	s.lc = lc
}

func (s *SupervisorV2) Stop() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.lc != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_ = s.lc.Stop(ctx)
		cancel()
		s.lc = nil
	}
}

// HTTP returns the base URL for the HTTP server of Supervisor v2.
func (s *SupervisorV2) HTTP() string { return s.httpURL }

// StartEmbeddedFromSys starts an op-node embedded in SV2 against the provided nodes.
func (s *SupervisorV2) StartEmbeddedFromSys(l1EL *L1ELNode, l1CL *L1CLNode, l2EL *L2ELNode) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.lc != nil {
		return
	}
	_ = os.Setenv("SV2_L1_SCOPE", "unsafe")
	port := 0
	if p := os.Getenv("OP_SV2_HTTP_PORT"); p != "" {
		if n, err := strconv.Atoi(p); err == nil {
			port = n
		}
	}
	// Build multi-chain config file with a single chain
	dir, err := os.MkdirTemp("", "sv2cfg-")
	s.p.Require().NoError(err)
	s.p.Cleanup(func() { _ = os.RemoveAll(dir) })
	// Read JWT secret from geth jwt file written earlier
	jwtHex, err := os.ReadFile(l2EL.jwtPath)
	s.p.Require().NoError(err)
	var jwtSecret [32]byte
	b, err := hex.DecodeString(string(jwtHex)[2:])
	s.p.Require().NoError(err)
	copy(jwtSecret[:], b)
	beaconAddr := ""
	if l1CL.beacon != nil {
		beaconAddr = l1CL.beacon.BeaconAddr()
	} else {
		beaconAddr = l1CL.beaconHTTPAddr
	}
	depth := uint64(sv2SupervisorConfirmDepth)
	if v := os.Getenv("OP_SV2_CONFIRM_DEPTH"); v != "" {
		if n, err := strconv.ParseUint(v, 10, 64); err == nil && n > 0 {
			depth = n
		}
	}
	// Write rollup config to temp file
	rollupPath := filepath.Join(dir, "rollup.json")
	{
		data, err := json.MarshalIndent(l2EL.l2Net.rollupCfg, "", "  ")
		s.p.Require().NoError(err)
		s.p.Require().NoError(os.WriteFile(rollupPath, data, 0o644))
	}
	// Write sv2 config file
	sv2cfgPath := filepath.Join(dir, "sv2.json")
	{
		cfgObj := map[string]any{
			"proxy_opnode":  true,
			"confirm_depth": opNodeConfDepth,
			"poll_interval": "1s",
			"chains": []map[string]any{
				{
					"l1_rpc":        l1EL.userRPC,
					"beacon_addr":   beaconAddr,
					"l2_authrpc":    l2EL.authRPC,
					"l2_userrpc":    l2EL.userRPC,
					"jwt_secret":    l2EL.jwtPath,
					"rollup_config": rollupPath,
				},
			},
		}
		data, err := json.MarshalIndent(cfgObj, "", "  ")
		s.p.Require().NoError(err)
		s.p.Require().NoError(os.WriteFile(sv2cfgPath, data, 0o644))
	}
	cfg := &service.Config{HTTPAddr: "127.0.0.1", HTTPPort: port, ProxyOpNode: true, DataDir: os.Getenv("SV2_DATA_DIR"), ConfigPath: sv2cfgPath, ConfirmDepth: depth, PollInterval: 1 * time.Second}
	lc, err := service.New(context.Background(), cfg, s.logger, "devstack", nil)
	s.p.Require().NoError(err)
	s.p.Require().NoError(lc.Start(s.p.Ctx()))
	if addr, ok := service.HTTPAddr(lc); ok {
		s.httpURL = "http://" + addr
	}
	s.lc = lc
	fmt.Printf("[sv2] http: %s\n", s.HTTP())
	_ = os.Setenv("SV2_AUTHORIZATION_URL", s.HTTP())
	// Log rollup cfg timing for debugging fork activation
	if l2EL.l2Net.rollupCfg != nil {
		g := l2EL.l2Net.rollupCfg.Genesis
		var i2 uint64
		if l2EL.l2Net.rollupCfg.Interop2Time != nil {
			i2 = *l2EL.l2Net.rollupCfg.Interop2Time
		}
		s.logger.Info("SV2 startup rollup timings", "chain", l2EL.id.ChainID(), "genesis_l2_time", g.L2Time, "interop2_time", i2)
	}
}

// StartEmbeddedFromSysNoEnv is like StartEmbeddedFromSys but does not mutate SV2_AUTHORIZATION_URL.
func (s *SupervisorV2) StartEmbeddedFromSysNoEnv(l1EL *L1ELNode, l1CL *L1CLNode, l2EL *L2ELNode) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.lc != nil {
		return
	}
	_ = os.Setenv("SV2_L1_SCOPE", "unsafe")
	port := 0
	if p := os.Getenv("OP_SV2_HTTP_PORT"); p != "" {
		if n, err := strconv.Atoi(p); err == nil {
			port = n
		}
	}
	// Build multi-chain config file with a single chain
	dir, err := os.MkdirTemp("", "sv2cfg-")
	s.p.Require().NoError(err)
	s.p.Cleanup(func() { _ = os.RemoveAll(dir) })
	// Read JWT secret
	jwtHex, err := os.ReadFile(l2EL.jwtPath)
	s.p.Require().NoError(err)
	var jwtSecret [32]byte
	b, err := hex.DecodeString(string(jwtHex)[2:])
	s.p.Require().NoError(err)
	copy(jwtSecret[:], b)
	beaconAddr := ""
	if l1CL.beacon != nil {
		beaconAddr = l1CL.beacon.BeaconAddr()
	} else {
		beaconAddr = l1CL.beaconHTTPAddr
	}
	depth := uint64(sv2SupervisorConfirmDepth)
	if v := os.Getenv("OP_SV2_CONFIRM_DEPTH"); v != "" {
		if n, err := strconv.ParseUint(v, 10, 64); err == nil && n > 0 {
			depth = n
		}
	}
	// Write rollup config to temp file
	rollupPath := filepath.Join(dir, "rollup.json")
	{
		data, err := json.MarshalIndent(l2EL.l2Net.rollupCfg, "", "  ")
		s.p.Require().NoError(err)
		s.p.Require().NoError(os.WriteFile(rollupPath, data, 0o644))
	}
	// Write sv2 config file
	sv2cfgPath := filepath.Join(dir, "sv2.json")
	{
		cfgObj := map[string]any{
			"proxy_opnode":  true,
			"confirm_depth": opNodeConfDepth,
			"poll_interval": "1s",
			"chains": []map[string]any{
				{
					"l1_rpc":        l1EL.userRPC,
					"beacon_addr":   beaconAddr,
					"l2_authrpc":    l2EL.authRPC,
					"l2_userrpc":    l2EL.userRPC,
					"jwt_secret":    l2EL.jwtPath,
					"rollup_config": rollupPath,
				},
			},
		}
		data, err := json.MarshalIndent(cfgObj, "", "  ")
		s.p.Require().NoError(err)
		s.p.Require().NoError(os.WriteFile(sv2cfgPath, data, 0o644))
	}
	cfg := &service.Config{HTTPAddr: "127.0.0.1", HTTPPort: port, ProxyOpNode: true, DataDir: os.Getenv("SV2_DATA_DIR"), ConfigPath: sv2cfgPath, ConfirmDepth: depth, PollInterval: 1 * time.Second}
	lc, err := service.New(context.Background(), cfg, s.logger, "devstack", nil)
	s.p.Require().NoError(err)
	s.p.Require().NoError(lc.Start(s.p.Ctx()))
	if addr, ok := service.HTTPAddr(lc); ok {
		s.httpURL = "http://" + addr
	}
	s.lc = lc
	fmt.Printf("[sv2] http: %s\n", s.HTTP())
}

// proxyAddr: reuse variant from l2_el.go signature
// Note: proxyAddr helper is defined in l2_el.go and available within this package; do not redeclare here.

// WithSupervisorV2OnFirstChain starts Supervisor v2 for the first L2 EL, embedding an op-node internally.
func WithSupervisorV2OnFirstChain() stack.Option[*Orchestrator] {
	// Capture orchestrator so we can register a lightweight CL handle for batcher wiring
	var captured *Orchestrator
	// Start SV2 in AfterDeploy; register CL shim in PostHydrate when we have a stack.System (devtest.T)
	after := stack.AfterDeploy(func(orch *Orchestrator) {
		captured = orch
		l2elIDs := stack.SortL2ELNodeIDs(orch.l2ELs.Keys())
		orch.p.Require().GreaterOrEqual(len(l2elIDs), 1, "need at least one L2 EL node")
		// pick first L1 EL and L1 CL
		l1elIDs := stack.SortL1ELNodeIDs(orch.l1ELs.Keys())
		l1clIDs := stack.SortL1CLNodeIDs(orch.l1CLs.Keys())
		orch.p.Require().GreaterOrEqual(len(l1elIDs), 1, "need at least one L1 EL node")
		orch.p.Require().GreaterOrEqual(len(l1clIDs), 1, "need at least one L1 CL node")

		l2el, _ := orch.l2ELs.Get(l2elIDs[0])
		l1el, _ := orch.l1ELs.Get(l1elIDs[0])
		l1cl, _ := orch.l1CLs.Get(l1clIDs[0])

		id := stack.SupervisorID("sv2-" + l2elIDs[0].Key())
		s := &SupervisorV2{id: id, logger: orch.P().Logger(), p: orch.P()}
		orch.p.Cleanup(s.Stop)

		s.StartEmbeddedFromSys(l1el, l1cl, l2el)

		// Wait for SV2 HTTP to be ready
		err := retry.Do0(orch.P().Ctx(), 10, &retry.FixedStrategy{Dur: 300 * time.Millisecond}, func() error {
			return waitHTTP(orch.P(), s.HTTP()+"/healthz")
		})
		orch.P().Require().NoError(err)
		// Register a lightweight CL handle in the orchestrator map so components (e.g., batcher) can resolve it
		url := s.HTTP()
		clID := stack.NewL2CLNodeID("embedded", l2el.id.ChainID())
		if _, ok := orch.l2CLs.Get(clID); !ok {
			if cid, ok2 := l2el.id.ChainID().Uint64(); ok2 {
				orch.l2CLs.Set(clID, &L2CLNode{
					id:      clID,
					userRPC: fmt.Sprintf("%s/opnode/%d/", url, cid),
					p:       orch.P(),
					logger:  orch.P().Logger(),
					el:      l2el.id,
				})
			}
		}
	})

	post := stack.PostHydrate[*Orchestrator](func(sys stack.System) {
		// Build CL shim against SV2 proxy for first L2 network
		nets := sys.L2Networks()
		if len(nets) == 0 {
			return
		}
		net := nets[0]
		url := os.Getenv("SV2_AUTHORIZATION_URL")
		if url == "" {
			return
		}
		var rpcURL string
		if cid, ok := net.ID().ChainID().Uint64(); ok {
			rpcURL = fmt.Sprintf("%s/opnode/%d/", url, cid)
		} else {
			rpcURL = fmt.Sprintf("%s/opnode/", url)
		}
		cli, err := opclient.NewRPC(sys.T().Ctx(), sys.Logger(), rpcURL, opclient.WithLazyDial())
		if err != nil {
			return
		}
		clID := stack.NewL2CLNodeID("embedded", net.ID().ChainID())
		clShim := shim.NewL2CLNode(shim.L2CLNodeConfig{CommonConfig: shim.NewCommonConfig(sys.T()), ID: clID, Client: cli})
		el := net.L2ELNode(match.FirstL2EL)
		clShim.(stack.LinkableL2CLNode).LinkEL(el)
		net.(stack.ExtensibleL2Network).AddL2CLNode(clShim)

		// Also register a minimal CL handle in the orchestrator map so components started after hydration
		// (like the batcher) can look it up by ID and reuse the SV2 proxy URL as Rollup RPC.
		if captured != nil {
			// Populate only the fields needed by WithBatcher (userRPC and IDs). Do not start a real op-node here.
			l2elID := el.ID()
			// Avoid double registration if already present
			if _, ok := captured.l2CLs.Get(clID); !ok {
				if cid, ok := net.ID().ChainID().Uint64(); ok {
					captured.l2CLs.Set(clID, &L2CLNode{
						id:      clID,
						userRPC: fmt.Sprintf("%s/opnode/%d/", url, cid),
						p:       captured.P(),
						logger:  captured.P().Logger(),
						el:      l2elID,
					})
				}
			}
		}
	})

	return stack.Combine[*Orchestrator](after, post)
}

// WithSupervisorV2OnAllChains starts a single Supervisor v2 and registers all L2 ELs as chains.
func WithSupervisorV2OnAllChains() stack.Option[*Orchestrator] {
	return WithSupervisorV2OnAllChainsConfirmDepth(sv2SupervisorConfirmDepth)
}

// WithSV2TwoChainMinimalDepth composes a minimal two-chain setup without CLs and starts a single SV2 across both chains,
// using a custom L1 confirmation depth for cross-safety gating.
func WithSV2TwoChainMinimalDepth(offset uint64, depth uint64) stack.Option[*Orchestrator] {
	// gate to assert the L2 network count after hydration
	gateTwo := stack.PostHydrate[*Orchestrator](func(sys stack.System) {
		sys.T().Gate().Lenf(sys.L2Networks(), 2, "Must have exactly %v chains", 2)
	})
	return stack.Combine[*Orchestrator](
		DefaultTwoMinimalSystemNoCL(&DefaultTwoMinimalSystemIDs{}),
		// ensure Interop2 activation is configured on rollup cfgs before SV2 starts
		WithInterop2ActivationOffsetForSV2(offset),
		WithSupervisorV2OnAllChainsConfirmDepth(depth),
		// Configure batchers to use SV2 /opnode/{chainId}/ proxy (set RollupRpc override)
		WithBatcherOption(func(id stack.L2BatcherID, cfg *bss.CLIConfig) {
			if v, ok := id.ChainID().Uint64(); ok {
				sv2URL := os.Getenv("SV2_AUTHORIZATION_URL")
				cfg.RollupRpc = []string{fmt.Sprintf("%s/opnode/%d/", sv2URL, v)}
			}
		}),
		// start batchers in AfterDeploy (SV2 HTTP + CL shims are ready)
		stack.AfterDeploy(func(orch *Orchestrator) {
			orch.P().Logger().Info("Starting batchers for SV2 two-chain (after-deploy)")
			optA := WithBatcher(stack.NewL2BatcherID("main", DefaultL2AID), stack.NewL1ELNodeID("l1", DefaultL1ID), stack.NewL2CLNodeID("embedded", DefaultL2AID), stack.NewL2ELNodeID("sequencer", DefaultL2AID))
			optA.AfterDeploy(orch)
			optB := WithBatcher(stack.NewL2BatcherID("main", DefaultL2BID), stack.NewL1ELNodeID("l1", DefaultL1ID), stack.NewL2CLNodeID("embedded", DefaultL2BID), stack.NewL2ELNodeID("sequencer", DefaultL2BID))
			optB.AfterDeploy(orch)
		}),
		gateTwo,
	)
}

// WithSV2TwoChainReady composes a readable two-chain preset with:
// - Interop2 activation, SV2 across both chains with confirmation depth
// - Two batchers (one per chain) wired to SV2 /opnode/{chainId}/
// - Funded dev accounts on both chains for convenient testing
func WithSV2TwoChainReady(offset uint64, depth uint64, fundCount int) stack.Option[*Orchestrator] {
	var captured *Orchestrator
	return stack.Combine[*Orchestrator](
		WithSV2TwoChainMinimalDepth(offset, depth),
		// Capture orchestrator to fund accounts post-hydrate
		stack.AfterDeploy(func(orch *Orchestrator) { captured = orch }),
		stack.PostHydrate[*Orchestrator](func(sys stack.System) {
			// Fund N accounts per chain using deterministic dev keys + faucet
			if captured == nil || fundCount <= 0 {
				return
			}
			nets := sys.L2Networks()
			if len(nets) < 2 {
				return
			}
			keys, err := devkeys.NewSaltedDevKeys(devkeys.TestMnemonic, os.Getenv("OP_DEVSTACK_SALT"))
			if err != nil {
				sys.T().Logger().Warn("devkeys init failed; skipping faucet funding", "err", err)
				return
			}
			for _, net := range nets {
				faucet := net.Faucet(match.FirstFaucet)
				for i := 0; i < fundCount; i++ {
					addr, _ := keys.Address(devkeys.UserKey(i))
					_ = faucet.API().RequestETH(sys.T().Ctx(), addr, eth.OneTenthEther)
				}
			}
		}),
	)
}

// WithSupervisorV2OnAllChainsConfirmDepth starts a single Supervisor v2 and registers all L2 ELs as chains,
// using a custom L1 confirmation depth for cross-safety gating.
func WithSupervisorV2OnAllChainsConfirmDepth(depth uint64) stack.Option[*Orchestrator] {
	return stack.AfterDeploy(func(orch *Orchestrator) {
		l2elIDs := stack.SortL2ELNodeIDs(orch.l2ELs.Keys())
		orch.p.Require().GreaterOrEqual(len(l2elIDs), 1, "need at least one L2 EL node")
		// pick first L1 EL and L1 CL (shared across chains)
		l1elIDs := stack.SortL1ELNodeIDs(orch.l1ELs.Keys())
		l1clIDs := stack.SortL1CLNodeIDs(orch.l1CLs.Keys())
		orch.p.Require().GreaterOrEqual(len(l1elIDs), 1, "need at least one L1 EL node")
		orch.p.Require().GreaterOrEqual(len(l1clIDs), 1, "need at least one L1 CL node")

		l1el, _ := orch.l1ELs.Get(l1elIDs[0])
		l1cl, _ := orch.l1CLs.Get(l1clIDs[0])

		// Create a multi-chain sv2.config with custom depth
		dir, err := os.MkdirTemp("", "sv2cfg-")
		orch.p.Require().NoError(err)
		orch.p.Cleanup(func() { _ = os.RemoveAll(dir) })
		var chains []map[string]any
		// Sort by ChainID ascending for deterministic order (A, then B)
		sort.Slice(l2elIDs, func(i, j int) bool {
			vi, _ := l2elIDs[i].ChainID().Uint64()
			vj, _ := l2elIDs[j].ChainID().Uint64()
			return vi < vj
		})
		for _, l2id := range l2elIDs {
			l2el, _ := orch.l2ELs.Get(l2id)
			rollupPath := func() string {
				if cid, ok := l2id.ChainID().Uint64(); ok {
					return filepath.Join(dir, fmt.Sprintf("rollup-%d.json", cid))
				}
				return filepath.Join(dir, fmt.Sprintf("rollup-%s.json", l2id.Key()))
			}()
			rcfg := *l2el.l2Net.rollupCfg
			if cid, ok := l2id.ChainID().Uint64(); ok {
				rcfg.L2ChainID = new(big.Int).SetUint64(cid)
			}
			data, err := json.MarshalIndent(&rcfg, "", "  ")
			orch.p.Require().NoError(err)
			orch.p.Require().NoError(os.WriteFile(rollupPath, data, 0o644))
			beaconAddr := ""
			if l1cl.beacon != nil {
				beaconAddr = l1cl.beacon.BeaconAddr()
			} else {
				beaconAddr = l1cl.beaconHTTPAddr
			}
			chains = append(chains, map[string]any{
				"l1_rpc":        l1el.userRPC,
				"beacon_addr":   beaconAddr,
				"l2_authrpc":    l2el.authRPC,
				"l2_userrpc":    l2el.userRPC,
				"jwt_secret":    l2el.jwtPath,
				"rollup_config": rollupPath,
			})
		}
		sv2cfgPath := filepath.Join(dir, "sv2.json")
		{
			cfgObj := map[string]any{"chains": chains}
			data, err := json.MarshalIndent(cfgObj, "", "  ")
			orch.p.Require().NoError(err)
			orch.p.Require().NoError(os.WriteFile(sv2cfgPath, data, 0o644))
		}

		// Create SV2 with HTTP server once
		id := stack.SupervisorID("sv2-all")
		s := &SupervisorV2{id: id, logger: orch.P().Logger(), p: orch.P()}
		orch.p.Cleanup(s.Stop)
		port := 0
		if p := os.Getenv("OP_SV2_HTTP_PORT"); p != "" {
			if n, err := strconv.Atoi(p); err == nil {
				port = n
			}
		}
		cfg := &service.Config{HTTPAddr: "127.0.0.1", HTTPPort: port, ProxyOpNode: true, DataDir: os.Getenv("SV2_DATA_DIR"), ConfigPath: sv2cfgPath, ConfirmDepth: depth, PollInterval: 1 * time.Second}
		lc, err := service.New(context.Background(), cfg, s.logger, "devstack", nil)
		orch.p.Require().NoError(err)
		orch.p.Require().NoError(lc.Start(orch.p.Ctx()))
		if addr, ok := service.HTTPAddr(lc); ok {
			s.httpURL = "http://" + addr
		}
		s.lc = lc
		fmt.Printf("[sv2] http: %s\n", s.HTTP())
		_ = os.Setenv("SV2_AUTHORIZATION_URL", s.HTTP())

		// Chains will be loaded from sv2.config by the service

		// Wait for HTTP
		err = retry.Do0(orch.P().Ctx(), 10, &retry.FixedStrategy{Dur: 300 * time.Millisecond}, func() error {
			return waitHTTP(orch.P(), s.HTTP()+"/healthz")
		})
		orch.P().Require().NoError(err)

		// Also register minimal CL handles in the orchestrator map
		for _, l2id := range l2elIDs {
			l2el, _ := orch.l2ELs.Get(l2id)
			if cid, ok := l2el.id.ChainID().Uint64(); ok {
				clID := stack.NewL2CLNodeID("embedded", l2el.id.ChainID())
				if _, ok2 := orch.l2CLs.Get(clID); !ok2 {
					orch.l2CLs.Set(clID, &L2CLNode{
						id:      clID,
						userRPC: fmt.Sprintf("%s/opnode/%d/", s.HTTP(), cid),
						p:       orch.P(),
						logger:  orch.P().Logger(),
						el:      l2el.id,
					})
				}
			}
		}

		// L2CL shims registered during hydration; batchers will be started post-hydrate.
	})
}

// WithSV2TwoChainMinimal composes a minimal two-chain setup without CLs and starts a single SV2 across both chains.
func WithSV2TwoChainMinimal(offset uint64) stack.Option[*Orchestrator] {
	// Gate to assert the L2 network count after hydration
	gateTwo := stack.PostHydrate[*Orchestrator](func(sys stack.System) {
		sys.T().Gate().Lenf(sys.L2Networks(), 2, "Must have exactly %v chains", 2)
	})
	return stack.Combine[*Orchestrator](
		DefaultTwoMinimalSystemNoCL(&DefaultTwoMinimalSystemIDs{}),
		// ensure Interop2 activation is configured on rollup cfgs before SV2 starts
		WithInterop2ActivationOffsetForSV2(offset),
		WithSupervisorV2OnAllChains(),
		gateTwo,
	)
}

// WithInterop2ActivationOffsetForSV2 sets interop2 activation to genesis + offset
// on all L2 rollup configs before starting SV2 in AfterDeploy.
func WithInterop2ActivationOffsetForSV2(offset uint64) stack.Option[*Orchestrator] {
	return stack.AfterDeploy(func(orch *Orchestrator) {
		l2elIDs := stack.SortL2ELNodeIDs(orch.l2ELs.Keys())
		for _, id := range l2elIDs {
			l2el, _ := orch.l2ELs.Get(id)
			rcfg := l2el.l2Net.rollupCfg
			if rcfg == nil {
				continue
			}
			ts := rcfg.Genesis.L2Time + offset
			rcfg.Interop2Time = &ts
			orch.P().Logger().Info("Set Interop2Time", "chain", id.ChainID(), "genesis_l2_time", rcfg.Genesis.L2Time, "interop2_time", ts)
		}
	})
}

// waitHTTP polls a URL until it returns 200 OK or times out.
func waitHTTP(p devtest.P, url string) error {
	client := &http.Client{Timeout: 500 * time.Millisecond}
	resp, err := client.Get(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status: %d", resp.StatusCode)
	}
	return nil
}

// WithSecondSupervisorV2OnFirstChain starts a SECOND SV2 instance for the same first-chain EL,
// intended to be used as a verifier by disabling the sequencer in the embedded op-node via env.
// It does not override SV2_AUTHORIZATION_URL and instead notifies the caller via callback with its base URL.
func WithSecondSupervisorV2OnFirstChain(onReady func(url string)) stack.Option[*Orchestrator] {
	return stack.AfterDeploy(func(orch *Orchestrator) {
		l2elIDs := stack.SortL2ELNodeIDs(orch.l2ELs.Keys())
		orch.p.Require().GreaterOrEqual(len(l2elIDs), 1, "need at least one L2 EL node")
		l1elIDs := stack.SortL1ELNodeIDs(orch.l1ELs.Keys())
		l1clIDs := stack.SortL1CLNodeIDs(orch.l1CLs.Keys())
		orch.p.Require().GreaterOrEqual(len(l1elIDs), 1, "need at least one L1 EL node")
		orch.p.Require().GreaterOrEqual(len(l1clIDs), 1, "need at least one L1 CL node")

		l2el, _ := orch.l2ELs.Get(l2elIDs[0])
		l1el, _ := orch.l1ELs.Get(l1elIDs[0])
		l1cl, _ := orch.l1CLs.Get(l1clIDs[0])

		id := stack.SupervisorID("sv2-" + l2elIDs[0].Key() + "-verifier")
		s := &SupervisorV2{id: id, logger: orch.P().Logger(), p: orch.P()}
		orch.p.Cleanup(s.Stop)

		// Temporarily set env to disable sequencer for this instance
		prev := os.Getenv("SV2_SEQUENCER_ENABLED")
		_ = os.Setenv("SV2_SEQUENCER_ENABLED", "false")
		s.StartEmbeddedFromSysNoEnv(l1el, l1cl, l2el)
		// Restore env
		if prev == "" {
			_ = os.Unsetenv("SV2_SEQUENCER_ENABLED")
		} else {
			_ = os.Setenv("SV2_SEQUENCER_ENABLED", prev)
		}

		// Wait for HTTP to be ready and notify caller
		err := retry.Do0(orch.P().Ctx(), 10, &retry.FixedStrategy{Dur: 300 * time.Millisecond}, func() error {
			return waitHTTP(orch.P(), s.HTTP()+"/healthz")
		})
		orch.P().Require().NoError(err)
		if onReady != nil {
			onReady(s.HTTP())
		}
	})
}

// WithSecondSupervisorV2ForEL starts a second SV2 instance for the given L2 EL ID in verifier mode (sequencer disabled).
// It does not modify SV2_AUTHORIZATION_URL; the onReady callback receives the base URL of the verifier SV2.
func WithSecondSupervisorV2ForEL(elID stack.L2ELNodeID, onReady func(url string)) stack.Option[*Orchestrator] {
	return stack.AfterDeploy(func(orch *Orchestrator) {
		l1elIDs := stack.SortL1ELNodeIDs(orch.l1ELs.Keys())
		l1clIDs := stack.SortL1CLNodeIDs(orch.l1CLs.Keys())
		orch.p.Require().GreaterOrEqual(len(l1elIDs), 1, "need at least one L1 EL node")
		orch.p.Require().GreaterOrEqual(len(l1clIDs), 1, "need at least one L1 CL node")

		l2el, ok := orch.l2ELs.Get(elID)
		orch.p.Require().True(ok, "specified L2 EL not found")
		l1el, _ := orch.l1ELs.Get(l1elIDs[0])
		l1cl, _ := orch.l1CLs.Get(l1clIDs[0])

		id := stack.SupervisorID("sv2-" + elID.Key() + "-verifier")
		s := &SupervisorV2{id: id, logger: orch.P().Logger(), p: orch.P()}
		orch.p.Cleanup(s.Stop)

		prev := os.Getenv("SV2_SEQUENCER_ENABLED")
		_ = os.Setenv("SV2_SEQUENCER_ENABLED", "false")
		s.StartEmbeddedFromSysNoEnv(l1el, l1cl, l2el)
		if prev == "" {
			_ = os.Unsetenv("SV2_SEQUENCER_ENABLED")
		} else {
			_ = os.Setenv("SV2_SEQUENCER_ENABLED", prev)
		}

		err := retry.Do0(orch.P().Ctx(), 10, &retry.FixedStrategy{Dur: 300 * time.Millisecond}, func() error {
			return waitHTTP(orch.P(), s.HTTP()+"/healthz")
		})
		orch.P().Require().NoError(err)
		if onReady != nil {
			onReady(s.HTTP())
		}
	})
}
