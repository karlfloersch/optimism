package sysgo

import (
	"context"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/log"

	"github.com/ethereum-optimism/optimism/op-devstack/devtest"
	"github.com/ethereum-optimism/optimism/op-devstack/shim"
	"github.com/ethereum-optimism/optimism/op-devstack/stack"
	"github.com/ethereum-optimism/optimism/op-interop-filter/filter"
	"github.com/ethereum-optimism/optimism/op-interop-filter/flags"
	"github.com/ethereum-optimism/optimism/op-service/client"
	oplog "github.com/ethereum-optimism/optimism/op-service/log"
	opmetrics "github.com/ethereum-optimism/optimism/op-service/metrics"
	"github.com/ethereum-optimism/optimism/op-service/oppprof"
	oprpc "github.com/ethereum-optimism/optimism/op-service/rpc"
	"github.com/ethereum-optimism/optimism/op-service/testutils/tcpproxy"
)

// InteropFilterService wraps the interop filter service for sysgo
type InteropFilterService struct {
	mu sync.Mutex

	id      stack.InteropFilterID
	userRPC string

	cfg    *filter.Config
	p      devtest.P
	logger log.Logger

	service *filter.Service

	proxy *tcpproxy.Proxy
}

var _ stack.Lifecycle = (*InteropFilterService)(nil)

func (s *InteropFilterService) hydrate(sys stack.ExtensibleSystem) {
	tlog := sys.Logger().New("id", s.id)
	filterClient, err := client.NewRPC(sys.T().Ctx(), tlog, s.userRPC, client.WithLazyDial())
	sys.T().Require().NoError(err)
	sys.T().Cleanup(filterClient.Close)

	sys.AddInteropFilter(shim.NewInteropFilter(shim.InteropFilterConfig{
		CommonConfig: shim.NewCommonConfig(sys.T()),
		ID:           s.id,
		Client:       filterClient,
	}))
}

func (s *InteropFilterService) UserRPC() string {
	return s.userRPC
}

func (s *InteropFilterService) Start() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.service != nil {
		s.logger.Warn("InteropFilter already started")
		return
	}

	if s.proxy == nil {
		s.proxy = tcpproxy.New(s.logger.New("proxy", "interop-filter"))
		s.p.Require().NoError(s.proxy.Start())
		s.p.Cleanup(func() {
			s.proxy.Close()
		})
		s.userRPC = "http://" + s.proxy.Addr()
	}

	srv, err := filter.NewService(context.Background(), s.cfg, s.logger)
	s.p.Require().NoError(err)

	s.service = srv
	s.logger.Info("Starting interop filter")
	err = srv.Start(context.Background())
	s.p.Require().NoError(err, "interop filter failed to start")
	s.logger.Info("Started interop filter")
	s.proxy.SetUpstream(ProxyAddr(s.p.Require(), "http://"+srv.RPC().Endpoint()))
}

func (s *InteropFilterService) Stop() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.service == nil {
		s.logger.Warn("InteropFilter already stopped")
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // force-quit
	s.logger.Info("Closing interop filter")
	closeErr := s.service.Stop(ctx)
	s.logger.Info("Closed interop filter", "err", closeErr)

	s.service = nil
}

// WithInteropFilter adds an interop filter service to the orchestrator.
// It will connect to the specified L2 EL nodes and serve checkAccessList requests.
func WithInteropFilter(filterID stack.InteropFilterID, l2ELs []stack.L2ELNodeID) stack.Option[*Orchestrator] {
	return stack.AfterDeploy(func(orch *Orchestrator) {
		p := orch.P().WithCtx(stack.ContextWithID(orch.P().Ctx(), filterID))
		require := p.Require()

		require.Nil(orch.interopFilter, "can only support a single interop-filter in sysgo")

		// Build L2 RPC list from EL nodes
		l2RPCs := make([]flags.L2RPC, 0, len(l2ELs))
		for _, elID := range l2ELs {
			el, ok := orch.l2ELs.Get(elID)
			require.True(ok, "need L2 EL for interop filter", elID)
			chainID, ok := elID.ChainID().Uint64()
			require.True(ok, "chain ID must fit in uint64")
			l2RPCs = append(l2RPCs, flags.L2RPC{
				ChainID: chainID,
				RPCURL:  el.UserRPC(),
			})
		}

		cfg := &filter.Config{
			L2RPCs:           l2RPCs,
			DataDir:          p.TempDir(),
			BackfillDuration: 1 * time.Minute, // Short backfill for tests
			Version:          "dev",
			LogConfig: oplog.CLIConfig{
				Level:  log.LevelDebug,
				Format: oplog.FormatText,
			},
			MetricsConfig: opmetrics.CLIConfig{
				Enabled: false,
			},
			PprofConfig: oppprof.CLIConfig{
				ListenEnabled: false,
			},
			RPC: oprpc.CLIConfig{
				ListenAddr: "127.0.0.1",
				ListenPort: 0, // Auto-assign port
			},
		}

		plog := p.Logger()
		filterService := &InteropFilterService{
			id:      filterID,
			userRPC: "", // set on start
			cfg:     cfg,
			p:       p,
			logger:  plog,
			service: nil, // set on start
		}
		orch.interopFilter = filterService
		filterService.Start()
		orch.p.Cleanup(filterService.Stop)
	})
}
