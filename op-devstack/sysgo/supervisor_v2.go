package sysgo

import (
	"context"
	"encoding/hex"
	"fmt"
	"net"
	"net/http"
	"os"
	"sync"
	"time"

	bss "github.com/ethereum-optimism/optimism/op-batcher/batcher"
	"github.com/ethereum-optimism/optimism/op-chain-ops/devkeys"
	"github.com/ethereum-optimism/optimism/op-devstack/devtest"
	"github.com/ethereum-optimism/optimism/op-devstack/shim"
	"github.com/ethereum-optimism/optimism/op-devstack/stack"
	"github.com/ethereum-optimism/optimism/op-devstack/stack/match"
	opclient "github.com/ethereum-optimism/optimism/op-service/client"
	"github.com/ethereum-optimism/optimism/op-service/eth"
	"github.com/ethereum-optimism/optimism/op-service/retry"
	sv2 "github.com/ethereum-optimism/optimism/op-supervisor-v2/supervisor"
	"github.com/ethereum/go-ethereum/log"
)

// SupervisorV2 runs the Supervisor v2 prototype in-process with an HTTP server
// and a polling loop against an existing L2CL (op-node) and L2EL (op-geth).
type SupervisorV2 struct {
	mu sync.Mutex

	id     stack.SupervisorID
	logger log.Logger
	p      devtest.P

	srv     *http.Server
	ln      net.Listener
	httpURL string

	sup *sv2.Supervisor

	// no extra fields needed; op-node is managed by the supervisor-v2 package
}

func (s *SupervisorV2) hydrate(sys stack.ExtensibleSystem) {
	// Register typed L2CL frontends against the per-chain embedded op-node RPC via SV2 HTTP reverse proxy.
	if s.sup == nil {
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
	if s.srv != nil {
		s.logger.Warn("Supervisor v2 already started")
		return
	}

	// Create Supervisor instance
	s.sup = sv2.NewSupervisor(s.logger)
	// For persistence tests, allow overriding data dir via env
	if dd := os.Getenv("SV2_DATA_DIR"); dd != "" {
		s.sup.SetDataDir(dd)
	}
	// Register env-driven height checker if configured
	if chk := sv2.NewHeightCheckerFromEnv(); chk != nil {
		s.sup.RegisterChecker(chk)
		_ = os.Setenv("SV2_ENABLE_CHECKERS", "true")
		// For devstack speed: use unsafe scope
		s.sup.SetL1ScopeLabel(eth.Unsafe)
	}
	// In tests, gate cross-safe against L1 Unsafe to progress quickly
	s.sup.SetL1ScopeLabel(eth.Unsafe)
	if chk := sv2.NewHeightCheckerFromEnv(); chk != nil {
		s.sup.RegisterChecker(chk)
		_ = os.Setenv("SV2_ENABLE_CHECKERS", "true")
	}
	// Expose embedded op-node user RPC via HTTP reverse proxy for tests
	s.sup.EnableOpNodeProxy(true)

	// Start HTTP server on ephemeral port
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	s.p.Require().NoError(err)
	s.ln = ln
	s.httpURL = "http://" + ln.Addr().String()
	s.srv = &http.Server{Handler: s.sup.HTTPHandler()}
	go func() {
		// Best-effort shutdown; errors are logged in test output if any
		_ = s.srv.Serve(ln)
	}()

	// Legacy path removed: embedded mode supersedes explicit op-node RPC plumbing here.
}

func (s *SupervisorV2) Stop() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.sup != nil {
		s.sup.Stop()
		s.sup = nil
	}
	if s.srv != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_ = s.srv.Shutdown(ctx)
		cancel()
		s.srv = nil
	}
	if s.ln != nil {
		_ = s.ln.Close()
		s.ln = nil
	}
}

// HTTP returns the base URL for the HTTP server of Supervisor v2.
func (s *SupervisorV2) HTTP() string { return s.httpURL }

// StartEmbeddedFromSys starts an op-node embedded in SV2 against the provided nodes.
func (s *SupervisorV2) StartEmbeddedFromSys(l1EL *L1ELNode, l1CL *L1CLNode, l2EL *L2ELNode) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.srv == nil {
		// Ensure HTTP is up to expose health/status
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		s.p.Require().NoError(err)
		s.ln = ln
		s.httpURL = "http://" + ln.Addr().String()
		s.sup = sv2.NewSupervisor(s.logger)
		// For persistence tests, allow overriding data dir via env
		if dd := os.Getenv("SV2_DATA_DIR"); dd != "" {
			s.sup.SetDataDir(dd)
		}
		// In tests, gate cross-safe against L1 Unsafe to progress quickly
		s.sup.SetL1ScopeLabel(eth.Unsafe)
		// Register env-driven height checker if configured
		if chk := sv2.NewHeightCheckerFromEnv(); chk != nil {
			s.sup.RegisterChecker(chk)
			_ = os.Setenv("SV2_ENABLE_CHECKERS", "true")
		}
		// Expose embedded op-node user RPC via HTTP reverse proxy for tests
		s.sup.EnableOpNodeProxy(true)
		s.srv = &http.Server{Handler: s.sup.HTTPHandler()}
		go func() { _ = s.srv.Serve(ln) }()
		// Expose the HTTP URL in logs for external consumers (e.g. smoke tests)
		fmt.Printf("[sv2] http: %s\n", s.HTTP())
	}
	// Export SV2 URL for op-node denylist integration in tests
	_ = os.Setenv("SV2_DENYLIST_URL", s.HTTP())

	// Read JWT secret from geth jwt file written earlier
	jwtHex, err := os.ReadFile(l2EL.jwtPath)
	s.p.Require().NoError(err)
	var jwtSecret [32]byte
	b, err := hex.DecodeString(string(jwtHex)[2:])
	s.p.Require().NoError(err)
	copy(jwtSecret[:], b)

	// Register the chain in multi-chain mode; this starts the embedded op-node and the finalized runner
	_, err = s.sup.AddChain(l1EL.userRPC, l1CL.beacon.BeaconAddr(), l2EL.authRPC, l2EL.userRPC, jwtSecret, l2EL.l2Net.rollupCfg, 1*time.Second, 40)
	s.p.Require().NoError(err)
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
		url := os.Getenv("SV2_DENYLIST_URL")
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

		// Create SV2 with HTTP server once
		id := stack.SupervisorID("sv2-all")
		s := &SupervisorV2{id: id, logger: orch.P().Logger(), p: orch.P()}
		orch.p.Cleanup(s.Stop)
		s.Start("", "")
		fmt.Printf("[sv2] http: %s\n", s.HTTP())
		_ = os.Setenv("SV2_DENYLIST_URL", s.HTTP())

		for _, l2id := range l2elIDs {
			l2el, _ := orch.l2ELs.Get(l2id)
			// Read JWT secret for this EL
			jwtHex, err := os.ReadFile(l2el.jwtPath)
			orch.p.Require().NoError(err)
			var jwtSecret [32]byte
			b, err := hex.DecodeString(string(jwtHex)[2:])
			orch.p.Require().NoError(err)
			copy(jwtSecret[:], b)

			// Add chain to supervisor
			_, err = s.sup.AddChain(l1el.userRPC, l1cl.beacon.BeaconAddr(), l2el.authRPC, l2el.userRPC, jwtSecret, l2el.l2Net.rollupCfg, 1*time.Second, 40)
			orch.p.Require().NoError(err)

			// Also register a minimal CL handle in the orchestrator map so components (e.g., batcher)
			// can resolve it by ID and use the SV2 proxy URL as Rollup RPC.
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

		// Wait for HTTP
		err := retry.Do0(orch.P().Ctx(), 10, &retry.FixedStrategy{Dur: 300 * time.Millisecond}, func() error {
			return waitHTTP(orch.P(), s.HTTP()+"/healthz")
		})
		orch.P().Require().NoError(err)

		// Note: L2CL shims are registered during system hydration (see SupervisorV2.hydrate).
	})
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

		// Create SV2 with HTTP server once
		id := stack.SupervisorID("sv2-all")
		s := &SupervisorV2{id: id, logger: orch.P().Logger(), p: orch.P()}
		orch.p.Cleanup(s.Stop)
		s.Start("", "")
		fmt.Printf("[sv2] http: %s\n", s.HTTP())
		_ = os.Setenv("SV2_DENYLIST_URL", s.HTTP())

		for _, l2id := range l2elIDs {
			l2el, _ := orch.l2ELs.Get(l2id)
			// Read JWT secret for this EL
			jwtHex, err := os.ReadFile(l2el.jwtPath)
			orch.p.Require().NoError(err)
			var jwtSecret [32]byte
			b, err := hex.DecodeString(string(jwtHex)[2:])
			orch.p.Require().NoError(err)
			copy(jwtSecret[:], b)

			// Add chain to supervisor with custom confirm depth
			_, err = s.sup.AddChain(l1el.userRPC, l1cl.beacon.BeaconAddr(), l2el.authRPC, l2el.userRPC, jwtSecret, l2el.l2Net.rollupCfg, 1*time.Second, depth)
			orch.p.Require().NoError(err)

			// Also register a minimal CL handle in the orchestrator map so components (e.g., batcher)
			// can resolve it by ID and use the SV2 proxy URL as Rollup RPC.
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

		// Wait for HTTP
		err := retry.Do0(orch.P().Ctx(), 10, &retry.FixedStrategy{Dur: 300 * time.Millisecond}, func() error {
			return waitHTTP(orch.P(), s.HTTP()+"/healthz")
		})
		orch.P().Require().NoError(err)

		// Note: L2CL shims are registered during hydration; batchers will be started post-hydrate.

		// Batchers are started in WithSV2TwoChainMinimalDepth PostHydrate hook
	})
}

// WithSV2TwoChainMinimalDepth composes a minimal two-chain setup without CLs and starts a single SV2 across both chains,
// using a custom L1 confirmation depth for cross-safety gating.
func WithSV2TwoChainMinimalDepth(offset uint64, depth uint64) stack.Option[*Orchestrator] {
	// no captured orchestrator needed in AfterDeploy variant
	// Gate to assert the L2 network count after hydration
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
				sv2URL := os.Getenv("SV2_DENYLIST_URL")
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
