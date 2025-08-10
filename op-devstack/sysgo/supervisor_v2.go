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

	"github.com/ethereum-optimism/optimism/op-devstack/devtest"
	"github.com/ethereum-optimism/optimism/op-devstack/shim"
	"github.com/ethereum-optimism/optimism/op-devstack/stack"
	opclient "github.com/ethereum-optimism/optimism/op-service/client"
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
	// Register a typed L2CL frontend against the embedded op-node RPC for DSL usage.
	if s.sup == nil {
		return
	}
	userRPC := s.sup.ManagedOpNodeUserRPC()
	if userRPC == "" {
		return
	}
	cli, err := opclient.NewRPC(sys.T().Ctx(), sys.Logger(), userRPC, opclient.WithLazyDial())
	sys.T().Require().NoError(err)
	sys.T().Cleanup(cli.Close)

	// Build a shim L2CL and link it to the existing EL
	// We don't have chain ID on supervisor ID; discover from existing L2 networks and attach to the first one.
	l2Nets := sys.L2Networks()
	if len(l2Nets) == 0 {
		return
	}
	l2Net := l2Nets[0]
	clID := stack.NewL2CLNodeID("embedded", l2Net.ID().ChainID())
	clShim := shim.NewL2CLNode(shim.L2CLNodeConfig{
		CommonConfig: shim.NewCommonConfig(sys.T()),
		ID:           clID,
		Client:       cli,
	})
	// Link to the first EL in this network, if present
	clShim.(stack.LinkableL2CLNode).LinkEL(l2Net.L2ELNode(stack.NewL2ELNodeID("sequencer", l2Net.ID().ChainID())))
	l2Net.(stack.ExtensibleL2Network).AddL2CLNode(clShim)
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

	// Legacy path removed: managed mode supersedes explicit op-node RPC plumbing here.
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
		s.srv = &http.Server{Handler: s.sup.HTTPHandler()}
		go func() { _ = s.srv.Serve(ln) }()
	}
	// Read JWT secret from geth jwt file written earlier
	jwtHex, err := os.ReadFile(l2EL.jwtPath)
	s.p.Require().NoError(err)
	var jwtSecret [32]byte
	b, err := hex.DecodeString(string(jwtHex)[2:])
	s.p.Require().NoError(err)
	copy(jwtSecret[:], b)

	// Start managed op-node inside supervisor-v2 and begin polling (use L2 user RPC for reads)
	err = s.sup.StartManaged(l1EL.userRPC, l1CL.beacon.BeaconAddr(), l2EL.authRPC, l2EL.userRPC, jwtSecret, l2EL.l2Net.rollupCfg, 1*time.Second, 40)
	s.p.Require().NoError(err)
}

// proxyAddr: reuse variant from l2_el.go signature
// Note: proxyAddr helper is defined in l2_el.go and available within this package; do not redeclare here.

// WithSupervisorV2OnFirstChain starts Supervisor v2 for the first L2 EL, embedding an op-node internally.
func WithSupervisorV2OnFirstChain() stack.Option[*Orchestrator] {
	return stack.AfterDeploy(func(orch *Orchestrator) {
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

		err := retry.Do0(orch.P().Ctx(), 10, &retry.FixedStrategy{Dur: 300 * time.Millisecond}, func() error {
			return waitHTTP(orch.P(), s.HTTP()+"/healthz")
		})
		orch.P().Require().NoError(err)
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
