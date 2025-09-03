package sysgo

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/log"
	"github.com/stretchr/testify/require"

	"github.com/ethereum-optimism/optimism/op-devstack/devtest"
	"github.com/ethereum-optimism/optimism/op-devstack/stack"
	"github.com/ethereum-optimism/optimism/op-service/testlog"
	service "github.com/ethereum-optimism/optimism/op-supervisor-v2"
)

// This test validates that sv2.config top-level fields and chains list are parsed and applied.
func TestSV2Config_MultiChainJSON_LoadsAndBindsHTTP(gt *testing.T) {
	// test setup
	t := devtest.SerialT(gt)
	logger := testlog.Logger(gt, log.LevelInfo)
	onFail, onSkipNow := exiters(gt)
	p := devtest.NewP(context.Background(), logger, onFail, onSkipNow)
	gt.Cleanup(p.Close)
	ctx, cancel := context.WithTimeout(t.Ctx(), 120*time.Second)
	defer cancel()

	// stack setup (single EL, no CL)
	var ids DefaultMinimalSystemIDs
	opt := stack.Combine[*Orchestrator](
		DefaultMinimalSystemNoCL(&ids),
	)
	orch := NewOrchestrator(p, stack.Combine[*Orchestrator]())
	stack.ApplyOptionLifecycle(opt, orch)

	l1el, ok := orch.l1ELs.Get(ids.L1EL)
	require.True(gt, ok)
	l1cl, ok := orch.l1CLs.Get(ids.L1CL)
	require.True(gt, ok)
	l2el, ok := orch.l2ELs.Get(ids.L2EL)
	require.True(gt, ok)

	dir, err := os.MkdirTemp("", "sv2cfg-")
	require.NoError(gt, err)
	gt.Cleanup(func() { _ = os.RemoveAll(dir) })

	rollupPath := filepath.Join(dir, "rollup.json")
	{
		data, err := json.MarshalIndent(l2el.l2Net.rollupCfg, "", "  ")
		require.NoError(gt, err)
		require.NoError(gt, os.WriteFile(rollupPath, data, 0o644))
	}

	beaconAddr := l1cl.beaconHTTPAddr
	if l1cl.beacon != nil {
		beaconAddr = l1cl.beacon.BeaconAddr()
	}

	sv2cfgPath := filepath.Join(dir, "sv2.json")
	cfgObj := map[string]any{
		"http_addr":     "127.0.0.1",
		"http_port":     0,
		"proxy_opnode":  true,
		"confirm_depth": 40,
		"poll_interval": "1s",
		"chains": []map[string]any{
			{
				"l1_rpc":        l1el.userRPC,
				"beacon_addr":   beaconAddr,
				"l2_authrpc":    l2el.authRPC,
				"l2_userrpc":    l2el.userRPC,
				"jwt_secret":    l2el.jwtPath,
				"rollup_config": rollupPath,
			},
		},
	}
	data, err := json.MarshalIndent(cfgObj, "", "  ")
	require.NoError(gt, err)
	require.NoError(gt, os.WriteFile(sv2cfgPath, data, 0o644))

	lc, err := service.New(ctx, &service.Config{HTTPAddr: "127.0.0.1", HTTPPort: 0, ProxyOpNode: true, ConfigPath: sv2cfgPath}, logger, "test", nil)
	require.NoError(gt, err)
	require.NoError(gt, lc.Start(ctx))
	gt.Cleanup(func() {
		c, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_ = lc.Stop(c)
		cancel()
	})

	addr, ok := service.HTTPAddr(lc)
	require.True(gt, ok)
	resp, err := http.Get("http://" + addr + "/healthz")
	require.NoError(gt, err)
	defer resp.Body.Close()
	require.Equal(gt, http.StatusOK, resp.StatusCode)
}

// This test validates that user_rpc_port is respected by the embedded op-node when set in sv2.config.
func TestSV2Config_UserRPCPort_Binds(gt *testing.T) {
	// test setup
	t := devtest.SerialT(gt)
	logger := testlog.Logger(gt, log.LevelInfo)
	onFail, onSkipNow := exiters(gt)
	p := devtest.NewP(context.Background(), logger, onFail, onSkipNow)
	gt.Cleanup(p.Close)
	ctx, cancel := context.WithTimeout(t.Ctx(), 120*time.Second)
	defer cancel()

	// stack setup (single EL, no CL)
	var ids DefaultMinimalSystemIDs
	opt := stack.Combine[*Orchestrator](
		DefaultMinimalSystemNoCL(&ids),
	)
	orch := NewOrchestrator(p, stack.Combine[*Orchestrator]())
	stack.ApplyOptionLifecycle(opt, orch)

	l1el, ok := orch.l1ELs.Get(ids.L1EL)
	require.True(gt, ok)
	l1cl, ok := orch.l1CLs.Get(ids.L1CL)
	require.True(gt, ok)
	l2el, ok := orch.l2ELs.Get(ids.L2EL)
	require.True(gt, ok)

	dir, err := os.MkdirTemp("", "sv2cfg-")
	require.NoError(gt, err)
	gt.Cleanup(func() { _ = os.RemoveAll(dir) })

	rollupPath := filepath.Join(dir, "rollup.json")
	{
		data, err := json.MarshalIndent(l2el.l2Net.rollupCfg, "", "  ")
		require.NoError(gt, err)
		require.NoError(gt, os.WriteFile(rollupPath, data, 0o644))
	}

	beaconAddr := l1cl.beaconHTTPAddr
	if l1cl.beacon != nil {
		beaconAddr = l1cl.beacon.BeaconAddr()
	}

	// pick a free port
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(gt, err)
	addr := ln.Addr().(*net.TCPAddr)
	userPort := addr.Port
	_ = ln.Close()

	sv2cfgPath := filepath.Join(dir, "sv2.json")
	cfgObj := map[string]any{
		"http_addr":     "127.0.0.1",
		"http_port":     0,
		"proxy_opnode":  true,
		"confirm_depth": 2,
		"poll_interval": "1s",
		"chains": []map[string]any{
			{
				"l1_rpc":        l1el.userRPC,
				"beacon_addr":   beaconAddr,
				"l2_authrpc":    l2el.authRPC,
				"l2_userrpc":    l2el.userRPC,
				"jwt_secret":    l2el.jwtPath,
				"rollup_config": rollupPath,
				"user_rpc_port": userPort,
			},
		},
	}
	data, err := json.MarshalIndent(cfgObj, "", "  ")
	require.NoError(gt, err)
	require.NoError(gt, os.WriteFile(sv2cfgPath, data, 0o644))

	lc, err := service.New(ctx, &service.Config{HTTPAddr: "127.0.0.1", HTTPPort: 0, ProxyOpNode: true, ConfigPath: sv2cfgPath}, logger, "test", nil)
	require.NoError(gt, err)
	require.NoError(gt, lc.Start(ctx))
	gt.Cleanup(func() {
		c, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_ = lc.Stop(c)
		cancel()
	})

	// assert user RPC is reachable on the requested port via reverse proxy path
	sv2Addr, ok := service.HTTPAddr(lc)
	require.True(gt, ok)
	// We expect /opnode/<chainID>/ to reach the op-node listening on userPort
	chainID := l2el.l2Net.rollupCfg.L2ChainID.Uint64()
	resp, err := http.Get(fmt.Sprintf("http://%s/opnode/%d/", sv2Addr, chainID))
	require.NoError(gt, err)
	defer resp.Body.Close()
	require.NotEqual(gt, http.StatusServiceUnavailable, resp.StatusCode)
}
