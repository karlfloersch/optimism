package sysgo

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/log"
	"github.com/stretchr/testify/require"

	"github.com/ethereum-optimism/optimism/op-devstack/devtest"
	"github.com/ethereum-optimism/optimism/op-devstack/stack"
	"github.com/ethereum-optimism/optimism/op-service/client"
	"github.com/ethereum-optimism/optimism/op-service/sources"
	"github.com/ethereum-optimism/optimism/op-service/testlog"
	service "github.com/ethereum-optimism/optimism/op-supervisor-v2"
)

// Test that two virtual op-nodes (from separate supervisor-v2 instances) can connect over P2P
// and report each other as connected peers via the P2P RPC.
func TestSV2_P2P_ConnectVirtualNodes(gt *testing.T) {
	// test setup
	t := devtest.SerialT(gt)
	logger := testlog.Logger(gt, log.LevelInfo)
	onFail, onSkipNow := exiters(gt)
	p := devtest.NewP(context.Background(), logger, onFail, onSkipNow)
	gt.Cleanup(p.Close)
	ctx, cancel := context.WithTimeout(t.Ctx(), 180*time.Second)
	defer cancel()

	// Ensure both virtual nodes run as verifiers (no sequencing conflicts)
	oldSeq := os.Getenv("SV2_SEQUENCER_ENABLED")
	_ = os.Setenv("SV2_SEQUENCER_ENABLED", "0")
	gt.Cleanup(func() {
		_ = os.Setenv("SV2_SEQUENCER_ENABLED", oldSeq)
	})

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

	// Write rollup config to disk
	dir, err := os.MkdirTemp("", "sv2p2p-")
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

	// Build shared chain object
	chain := map[string]any{
		"l1_rpc":        l1el.userRPC,
		"beacon_addr":   beaconAddr,
		"l2_authrpc":    l2el.authRPC,
		"l2_userrpc":    l2el.userRPC,
		"jwt_secret":    l2el.jwtPath,
		"rollup_config": rollupPath,
		// user_rpc_port omitted to bind random port per instance
	}

	// Create two separate sv2 config files (distinct HTTP ports)
	sv2cfgPathA := filepath.Join(dir, "sv2-a.json")
	cfgObjA := map[string]any{
		"http_addr":     "127.0.0.1",
		"http_port":     0,
		"proxy_opnode":  true,
		"sv2_data_dir":  filepath.Join(dir, "sv2-a"),
		"confirm_depth": 2,
		"poll_interval": "500ms",
		"chains":        []map[string]any{chain},
	}
	dataA, err := json.MarshalIndent(cfgObjA, "", "  ")
	require.NoError(gt, err)
	require.NoError(gt, os.WriteFile(sv2cfgPathA, dataA, 0o644))

	sv2cfgPathB := filepath.Join(dir, "sv2-b.json")
	cfgObjB := map[string]any{
		"http_addr":     "127.0.0.1",
		"http_port":     0,
		"proxy_opnode":  true,
		"sv2_data_dir":  filepath.Join(dir, "sv2-b"),
		"confirm_depth": 2,
		"poll_interval": "500ms",
		"chains":        []map[string]any{chain},
	}
	dataB, err := json.MarshalIndent(cfgObjB, "", "  ")
	require.NoError(gt, err)
	require.NoError(gt, os.WriteFile(sv2cfgPathB, dataB, 0o644))

	// Start both supervisor-v2 instances
	lcA, err := service.New(ctx, &service.Config{HTTPAddr: "127.0.0.1", HTTPPort: 0, ProxyOpNode: true, ConfigPath: sv2cfgPathA}, logger, "sv2-a", nil)
	require.NoError(gt, err)
	require.NoError(gt, lcA.Start(ctx))
	gt.Cleanup(func() {
		c, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_ = lcA.Stop(c)
		cancel()
	})

	lcB, err := service.New(ctx, &service.Config{HTTPAddr: "127.0.0.1", HTTPPort: 0, ProxyOpNode: true, ConfigPath: sv2cfgPathB}, logger, "sv2-b", nil)
	require.NoError(gt, err)
	require.NoError(gt, lcB.Start(ctx))
	gt.Cleanup(func() {
		c, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_ = lcB.Stop(c)
		cancel()
	})

	// Helper to fetch the embedded op-node user RPC from /status
	type statusResp struct {
		OpNodeUserRPC string `json:"op_node_user_rpc"`
	}
	fetchUserRPC := func(base string, chainID uint64) string {
		url := fmt.Sprintf("http://%s/status?chainId=%d", base, chainID)
		var out statusResp
		require.Eventually(gt, func() bool {
			resp, err := http.Get(url)
			if err != nil {
				return false
			}
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				return false
			}
			dec := json.NewDecoder(resp.Body)
			if err := dec.Decode(&out); err != nil {
				return false
			}
			return out.OpNodeUserRPC != ""
		}, 30*time.Second, 250*time.Millisecond)
		return out.OpNodeUserRPC
	}

	// Resolve HTTP addresses and op-node RPCs
	addrA, ok := service.HTTPAddr(lcA)
	require.True(gt, ok)
	addrB, ok := service.HTTPAddr(lcB)
	require.True(gt, ok)
	chainID := l2el.l2Net.rollupCfg.L2ChainID.Uint64()
	opnodeRPCA := fetchUserRPC(addrA, chainID)
	opnodeRPCB := fetchUserRPC(addrB, chainID)

	// Build P2P API clients directly to op-node RPCs
	mkP2P := func(endpoint string) *sources.P2PClient {
		rpc, err := client.NewRPC(ctx, logger, endpoint)
		require.NoError(gt, err)
		gt.Cleanup(func() { rpc.Close() })
		return sources.NewP2PClient(rpc)
	}
	p2pA := mkP2P(opnodeRPCA)
	p2pB := mkP2P(opnodeRPCB)

	// Get self info (addresses) of both nodes
	selfB, err := p2pB.Self(ctx)
	require.NoError(gt, err)
	require.NotEmpty(gt, selfB.Addresses)

	// Connect A -> B using one of B's multiaddrs
	err = p2pA.ConnectPeer(ctx, selfB.Addresses[0])
	require.NoError(gt, err)

	// Assert both report each other as connected peers
	require.Eventually(gt, func() bool {
		dumpA, err := p2pA.Peers(ctx, true)
		if err != nil {
			return false
		}
		dumpB, err := p2pB.Peers(ctx, true)
		if err != nil {
			return false
		}
		// Keys are peer IDs as strings
		_, seenByA := dumpA.Peers[selfB.PeerID.String()]
		// Fetch A's self to find its peer ID to look for on B
		selfA, err := p2pA.Self(ctx)
		if err != nil {
			return false
		}
		_, seenByB := dumpB.Peers[selfA.PeerID.String()]
		return seenByA && seenByB
	}, 30*time.Second, 250*time.Millisecond)
}

// Test that after rollback (stop/restart of the virtual node), the P2P peer ID remains stable
// when sv2_data_dir is set, and that nodes can reconnect.
func TestSV2_P2P_RollbackKeepsPeerID(gt *testing.T) {
	// test setup
	t := devtest.SerialT(gt)
	logger := testlog.Logger(gt, log.LevelInfo)
	onFail, onSkipNow := exiters(gt)
	p := devtest.NewP(context.Background(), logger, onFail, onSkipNow)
	gt.Cleanup(p.Close)
	ctx, cancel := context.WithTimeout(t.Ctx(), 180*time.Second)
	defer cancel()

	oldSeq := os.Getenv("SV2_SEQUENCER_ENABLED")
	_ = os.Setenv("SV2_SEQUENCER_ENABLED", "0")
	gt.Cleanup(func() { _ = os.Setenv("SV2_SEQUENCER_ENABLED", oldSeq) })

	var ids DefaultMinimalSystemIDs
	opt := stack.Combine[*Orchestrator](DefaultMinimalSystemNoCL(&ids))
	orch := NewOrchestrator(p, stack.Combine[*Orchestrator]())
	stack.ApplyOptionLifecycle(opt, orch)

	l1el, ok := orch.l1ELs.Get(ids.L1EL)
	require.True(gt, ok)
	l1cl, ok := orch.l1CLs.Get(ids.L1CL)
	require.True(gt, ok)
	l2el, ok := orch.l2ELs.Get(ids.L2EL)
	require.True(gt, ok)

	dir, err := os.MkdirTemp("", "sv2p2p-rollback-")
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

	chain := map[string]any{
		"l1_rpc":        l1el.userRPC,
		"beacon_addr":   beaconAddr,
		"l2_authrpc":    l2el.authRPC,
		"l2_userrpc":    l2el.userRPC,
		"jwt_secret":    l2el.jwtPath,
		"rollup_config": rollupPath,
	}

	sv2cfgPathA := filepath.Join(dir, "sv2-a.json")
	cfgObjA := map[string]any{
		"http_addr":     "127.0.0.1",
		"http_port":     0,
		"proxy_opnode":  true,
		"sv2_data_dir":  filepath.Join(dir, "sv2-a"),
		"confirm_depth": 2,
		"poll_interval": "500ms",
		"chains":        []map[string]any{chain},
	}
	dataA, err := json.MarshalIndent(cfgObjA, "", "  ")
	require.NoError(gt, err)
	require.NoError(gt, os.WriteFile(sv2cfgPathA, dataA, 0o644))

	sv2cfgPathB := filepath.Join(dir, "sv2-b.json")
	cfgObjB := map[string]any{
		"http_addr":     "127.0.0.1",
		"http_port":     0,
		"proxy_opnode":  true,
		"sv2_data_dir":  filepath.Join(dir, "sv2-b"),
		"confirm_depth": 2,
		"poll_interval": "500ms",
		"chains":        []map[string]any{chain},
	}
	dataB, err := json.MarshalIndent(cfgObjB, "", "  ")
	require.NoError(gt, err)
	require.NoError(gt, os.WriteFile(sv2cfgPathB, dataB, 0o644))

	lcA, err := service.New(ctx, &service.Config{HTTPAddr: "127.0.0.1", HTTPPort: 0, ProxyOpNode: true, ConfigPath: sv2cfgPathA}, logger, "sv2-a", nil)
	require.NoError(gt, err)
	require.NoError(gt, lcA.Start(ctx))
	gt.Cleanup(func() {
		c, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_ = lcA.Stop(c)
		cancel()
	})

	lcB, err := service.New(ctx, &service.Config{HTTPAddr: "127.0.0.1", HTTPPort: 0, ProxyOpNode: true, ConfigPath: sv2cfgPathB}, logger, "sv2-b", nil)
	require.NoError(gt, err)
	require.NoError(gt, lcB.Start(ctx))
	gt.Cleanup(func() {
		c, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_ = lcB.Stop(c)
		cancel()
	})

	type statusResp struct {
		OpNodeUserRPC string `json:"op_node_user_rpc"`
	}
	fetchUserRPC := func(base string, chainID uint64) string {
		url := fmt.Sprintf("http://%s/status?chainId=%d", base, chainID)
		var out statusResp
		require.Eventually(gt, func() bool {
			resp, err := http.Get(url)
			if err != nil {
				return false
			}
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				return false
			}
			dec := json.NewDecoder(resp.Body)
			if err := dec.Decode(&out); err != nil {
				return false
			}
			return out.OpNodeUserRPC != ""
		}, 30*time.Second, 250*time.Millisecond)
		return out.OpNodeUserRPC
	}

	addrA, ok := service.HTTPAddr(lcA)
	require.True(gt, ok)
	addrB, ok := service.HTTPAddr(lcB)
	require.True(gt, ok)
	chainID := l2el.l2Net.rollupCfg.L2ChainID.Uint64()
	opnodeRPCA := fetchUserRPC(addrA, chainID)
	opnodeRPCB := fetchUserRPC(addrB, chainID)

	mkP2P := func(endpoint string) *sources.P2PClient {
		rpc, err := client.NewRPC(ctx, logger, endpoint)
		require.NoError(gt, err)
		gt.Cleanup(func() { rpc.Close() })
		return sources.NewP2PClient(rpc)
	}
	p2pA := mkP2P(opnodeRPCA)
	p2pB := mkP2P(opnodeRPCB)

	// Connect A -> B
	selfB1, err := p2pB.Self(ctx)
	require.NoError(gt, err)
	require.NotEmpty(gt, selfB1.Addresses)
	err = p2pA.ConnectPeer(ctx, selfB1.Addresses[0])
	require.NoError(gt, err)
	require.Eventually(gt, func() bool {
		dumpA, err := p2pA.Peers(ctx, true)
		if err != nil {
			return false
		}
		selfA, err := p2pA.Self(ctx)
		if err != nil {
			return false
		}
		dumpB, err := p2pB.Peers(ctx, true)
		if err != nil {
			return false
		}
		_, okA := dumpA.Peers[selfB1.PeerID.String()]
		_, okB := dumpB.Peers[selfA.PeerID.String()]
		return okA && okB
	}, 30*time.Second, 250*time.Millisecond)

	// Rollback A by one block (or to zero if needed)
	// Use the service helper to call supervisor rollback
	err = service.RollbackChain(lcA, chainID, 0)
	require.NoError(gt, err)

	// Fetch B's self ID for comparison after A restarts
	selfBPeer := selfB1.PeerID

	// Wait for A to come back and assert peer ID of B stays the same and reconnection works
	require.Eventually(gt, func() bool {
		// re-fetch endpoints (A's port might have changed)
		addrA2, ok := service.HTTPAddr(lcA)
		if !ok {
			return false
		}
		opnodeRPCA2 := fetchUserRPC(addrA2, chainID)
		p2pA2 := mkP2P(opnodeRPCA2)
		// B identity should be stable across restart
		selfB2, err := p2pB.Self(ctx)
		if err != nil {
			return false
		}
		if selfB2.PeerID != selfBPeer {
			return false
		}
		// Reconnect A -> B
		if err := p2pA2.ConnectPeer(ctx, selfB2.Addresses[0]); err != nil {
			return false
		}
		dumpA2, err := p2pA2.Peers(ctx, true)
		if err != nil {
			return false
		}
		_, okA2 := dumpA2.Peers[selfB2.PeerID.String()]
		return okA2
	}, 60*time.Second, 500*time.Millisecond)
}

// Test that static peers configured via sv2.config connect automatically without opp2p_connectPeer
func TestSV2_P2P_StaticPeersAutoConnect(gt *testing.T) {
	t := devtest.SerialT(gt)
	logger := testlog.Logger(gt, log.LevelInfo)
	onFail, onSkipNow := exiters(gt)
	p := devtest.NewP(context.Background(), logger, onFail, onSkipNow)
	gt.Cleanup(p.Close)
	ctx, cancel := context.WithTimeout(t.Ctx(), 120*time.Second)
	defer cancel()

	// Run verifiers
	oldSeq := os.Getenv("SV2_SEQUENCER_ENABLED")
	_ = os.Setenv("SV2_SEQUENCER_ENABLED", "0")
	gt.Cleanup(func() { _ = os.Setenv("SV2_SEQUENCER_ENABLED", oldSeq) })

	var ids DefaultMinimalSystemIDs
	opt := stack.Combine[*Orchestrator](DefaultMinimalSystemNoCL(&ids))
	orch := NewOrchestrator(p, stack.Combine[*Orchestrator]())
	stack.ApplyOptionLifecycle(opt, orch)

	l1el, ok := orch.l1ELs.Get(ids.L1EL)
	require.True(gt, ok)
	l1cl, ok := orch.l1CLs.Get(ids.L1CL)
	require.True(gt, ok)
	l2el, ok := orch.l2ELs.Get(ids.L2EL)
	require.True(gt, ok)

	dir, err := os.MkdirTemp("", "sv2p2p-static-")
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

	chain := map[string]any{
		"l1_rpc":        l1el.userRPC,
		"beacon_addr":   beaconAddr,
		"l2_authrpc":    l2el.authRPC,
		"l2_userrpc":    l2el.userRPC,
		"jwt_secret":    l2el.jwtPath,
		"rollup_config": rollupPath,
	}

	// Step 1: start B to learn its multiaddrs
	cfgB := map[string]any{
		"http_addr":     "127.0.0.1",
		"http_port":     0,
		"proxy_opnode":  true,
		"sv2_data_dir":  filepath.Join(dir, "sv2-b"),
		"confirm_depth": 2,
		"poll_interval": "500ms",
		"chains":        []map[string]any{chain},
	}
	sv2cfgPathB := filepath.Join(dir, "sv2-b.json")
	dataB, err := json.MarshalIndent(cfgB, "", "  ")
	require.NoError(gt, err)
	require.NoError(gt, os.WriteFile(sv2cfgPathB, dataB, 0o644))
	lcB, err := service.New(ctx, &service.Config{HTTPAddr: "127.0.0.1", HTTPPort: 0, ProxyOpNode: true, ConfigPath: sv2cfgPathB}, logger, "sv2-b", nil)
	require.NoError(gt, err)
	require.NoError(gt, lcB.Start(ctx))
	gt.Cleanup(func() {
		c, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_ = lcB.Stop(c)
		cancel()
	})

	// Fetch B opnode RPC
	addrB, ok := service.HTTPAddr(lcB)
	require.True(gt, ok)
	chainID := l2el.l2Net.rollupCfg.L2ChainID.Uint64()
	type statusResp struct {
		OpNodeUserRPC string `json:"op_node_user_rpc"`
	}
	fetchUserRPC := func(base string, chainID uint64) string {
		url := fmt.Sprintf("http://%s/status?chainId=%d", base, chainID)
		var out statusResp
		require.Eventually(gt, func() bool {
			resp, err := http.Get(url)
			if err != nil {
				return false
			}
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				return false
			}
			dec := json.NewDecoder(resp.Body)
			if err := dec.Decode(&out); err != nil {
				return false
			}
			return out.OpNodeUserRPC != ""
		}, 30*time.Second, 250*time.Millisecond)
		return out.OpNodeUserRPC
	}
	opnodeRPCB := fetchUserRPC(addrB, chainID)
	rpcB, err := client.NewRPC(ctx, logger, opnodeRPCB)
	require.NoError(gt, err)
	gt.Cleanup(func() { rpcB.Close() })
	p2pB := sources.NewP2PClient(rpcB)
	selfB, err := p2pB.Self(ctx)
	require.NoError(gt, err)
	require.NotEmpty(gt, selfB.Addresses)

	// Step 2: start A with static peers set to B's address
	cfgA := map[string]any{
		"http_addr":     "127.0.0.1",
		"http_port":     0,
		"proxy_opnode":  true,
		"sv2_data_dir":  filepath.Join(dir, "sv2-a"),
		"confirm_depth": 2,
		"poll_interval": "500ms",
		"chains":        []map[string]any{chain},
	}
	// Inject p2p_static at the top-level chains[0]
	cfgAChains := cfgA["chains"].([]map[string]any)
	cfgAChains[0]["p2p_static"] = []string{selfB.Addresses[0]}
	cfgA["chains"] = cfgAChains

	sv2cfgPathA := filepath.Join(dir, "sv2-a.json")
	dataA, err := json.MarshalIndent(cfgA, "", "  ")
	require.NoError(gt, err)
	require.NoError(gt, os.WriteFile(sv2cfgPathA, dataA, 0o644))

	lcA, err := service.New(ctx, &service.Config{HTTPAddr: "127.0.0.1", HTTPPort: 0, ProxyOpNode: true, ConfigPath: sv2cfgPathA}, logger, "sv2-a", nil)
	require.NoError(gt, err)
	require.NoError(gt, lcA.Start(ctx))
	gt.Cleanup(func() {
		c, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_ = lcA.Stop(c)
		cancel()
	})

	// Assert A connects to B automatically without manual opp2p_connectPeer
	addrA, ok := service.HTTPAddr(lcA)
	require.True(gt, ok)
	opnodeRPCA := fetchUserRPC(addrA, chainID)
	rpcA, err := client.NewRPC(ctx, logger, opnodeRPCA)
	require.NoError(gt, err)
	gt.Cleanup(func() { rpcA.Close() })
	p2pA := sources.NewP2PClient(rpcA)

	require.Eventually(gt, func() bool {
		dumpA, err := p2pA.Peers(ctx, true)
		if err != nil {
			return false
		}
		_, ok := dumpA.Peers[selfB.PeerID.String()]
		return ok
	}, 30*time.Second, 250*time.Millisecond)
}

// Test that bootnode-based discovery connects peers without manual RPC connect
func TestSV2_P2P_BootnodeDiscoveryConnect(gt *testing.T) {
	t := devtest.SerialT(gt)
	logger := testlog.Logger(gt, log.LevelInfo)
	onFail, onSkipNow := exiters(gt)
	p := devtest.NewP(context.Background(), logger, onFail, onSkipNow)
	gt.Cleanup(p.Close)
	ctx, cancel := context.WithTimeout(t.Ctx(), 150*time.Second)
	defer cancel()

	oldSeq := os.Getenv("SV2_SEQUENCER_ENABLED")
	_ = os.Setenv("SV2_SEQUENCER_ENABLED", "0")
	gt.Cleanup(func() { _ = os.Setenv("SV2_SEQUENCER_ENABLED", oldSeq) })

	var ids DefaultMinimalSystemIDs
	opt := stack.Combine[*Orchestrator](DefaultMinimalSystemNoCL(&ids))
	orch := NewOrchestrator(p, stack.Combine[*Orchestrator]())
	stack.ApplyOptionLifecycle(opt, orch)

	l1el, ok := orch.l1ELs.Get(ids.L1EL)
	require.True(gt, ok)
	l1cl, ok := orch.l1CLs.Get(ids.L1CL)
	require.True(gt, ok)
	l2el, ok := orch.l2ELs.Get(ids.L2EL)
	require.True(gt, ok)

	dir, err := os.MkdirTemp("", "sv2p2p-bootnode-")
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

	chain := map[string]any{
		"l1_rpc":        l1el.userRPC,
		"beacon_addr":   beaconAddr,
		"l2_authrpc":    l2el.authRPC,
		"l2_userrpc":    l2el.userRPC,
		"jwt_secret":    l2el.jwtPath,
		"rollup_config": rollupPath,
	}

	// Start A (bootnode provider)
	cfgA := map[string]any{
		"http_addr":     "127.0.0.1",
		"http_port":     0,
		"proxy_opnode":  true,
		"sv2_data_dir":  filepath.Join(dir, "sv2-a"),
		"confirm_depth": 2,
		"poll_interval": "500ms",
		"chains":        []map[string]any{chain},
	}
	sv2cfgPathA := filepath.Join(dir, "sv2-a.json")
	dataA, err := json.MarshalIndent(cfgA, "", "  ")
	require.NoError(gt, err)
	require.NoError(gt, os.WriteFile(sv2cfgPathA, dataA, 0o644))
	lcA, err := service.New(ctx, &service.Config{HTTPAddr: "127.0.0.1", HTTPPort: 0, ProxyOpNode: true, ConfigPath: sv2cfgPathA}, logger, "sv2-a", nil)
	require.NoError(gt, err)
	require.NoError(gt, lcA.Start(ctx))
	gt.Cleanup(func() {
		c, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_ = lcA.Stop(c)
		cancel()
	})

	// Get A's ENR via P2P RPC Self
	type statusResp struct {
		OpNodeUserRPC string `json:"op_node_user_rpc"`
	}
	fetchUserRPC := func(base string, chainID uint64) string {
		url := fmt.Sprintf("http://%s/status?chainId=%d", base, chainID)
		var out statusResp
		require.Eventually(gt, func() bool {
			resp, err := http.Get(url)
			if err != nil {
				return false
			}
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				return false
			}
			dec := json.NewDecoder(resp.Body)
			if err := dec.Decode(&out); err != nil {
				return false
			}
			return out.OpNodeUserRPC != ""
		}, 30*time.Second, 250*time.Millisecond)
		return out.OpNodeUserRPC
	}
	addrA, ok := service.HTTPAddr(lcA)
	require.True(gt, ok)
	chainID := l2el.l2Net.rollupCfg.L2ChainID.Uint64()
	opnodeRPCA := fetchUserRPC(addrA, chainID)
	rpcA, err := client.NewRPC(ctx, logger, opnodeRPCA)
	require.NoError(gt, err)
	gt.Cleanup(func() { rpcA.Close() })
	p2pA := sources.NewP2PClient(rpcA)
	selfA, err := p2pA.Self(ctx)
	require.NoError(gt, err)
	require.NotEmpty(gt, selfA.ENR)

	// Start B with bootnodes set to A's ENR
	cfgB := map[string]any{
		"http_addr":     "127.0.0.1",
		"http_port":     0,
		"proxy_opnode":  true,
		"sv2_data_dir":  filepath.Join(dir, "sv2-b"),
		"confirm_depth": 2,
		"poll_interval": "500ms",
		"chains":        []map[string]any{chain},
	}
	cfgBChains := cfgB["chains"].([]map[string]any)
	cfgBChains[0]["p2p_bootnodes"] = []string{selfA.ENR}
	cfgB["chains"] = cfgBChains

	sv2cfgPathB := filepath.Join(dir, "sv2-b.json")
	dataB, err := json.MarshalIndent(cfgB, "", "  ")
	require.NoError(gt, err)
	require.NoError(gt, os.WriteFile(sv2cfgPathB, dataB, 0o644))
	lcB, err := service.New(ctx, &service.Config{HTTPAddr: "127.0.0.1", HTTPPort: 0, ProxyOpNode: true, ConfigPath: sv2cfgPathB}, logger, "sv2-b", nil)
	require.NoError(gt, err)
	require.NoError(gt, lcB.Start(ctx))
	gt.Cleanup(func() {
		c, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_ = lcB.Stop(c)
		cancel()
	})

	// Assert B discovers and connects to A without manual connect
	addrB, ok := service.HTTPAddr(lcB)
	require.True(gt, ok)
	opnodeRPCB := fetchUserRPC(addrB, chainID)
	rpcB, err := client.NewRPC(ctx, logger, opnodeRPCB)
	require.NoError(gt, err)
	gt.Cleanup(func() { rpcB.Close() })
	p2pB := sources.NewP2PClient(rpcB)

	require.Eventually(gt, func() bool {
		dumpB, err := p2pB.Peers(ctx, true)
		if err != nil {
			return false
		}
		_, ok := dumpB.Peers[selfA.PeerID.String()]
		return ok
	}, 60*time.Second, 500*time.Millisecond)
}
