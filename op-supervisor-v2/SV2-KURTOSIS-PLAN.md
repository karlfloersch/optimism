# Supervisor v2: single process for multi-chain with fixed per-chain op-node ports

## Goals
- Run a single op-supervisor-v2 managing multiple chains in Kurtosis.
- Prefer direct per-chain op-node ports; retain the HTTP reverse proxy (/opnode/{chainId}/) for tests and optional usage.
- Provide configuration for chains, EL endpoints, L1 endpoints, and desired per-chain op-node ports.
- Support interop mempool filtering toggle centrally via SV2.

## High-level design
- Extend the SV2 CLI multi-chain JSON config with optional user_rpc_port and interop_mempool_filtering per chain.
- Extend virtual_node.StartVirtualNode to accept user RPC listen address/port.
- Extend ChainContainer to store the chosen userRPCPort and expose it via /status.
- Keep reverse proxy available (behind a flag); default off in Kurtosis.
- Refactor CLI to op-node style lifecycle: a minimal main delegates to `SupervisorMain(ctx, closeApp)` returning `cliapp.Lifecycle`, enabling in-process sysgo tests to invoke startup/shutdown cleanly.

## Config shape (example)
{
  "http_addr": "0.0.0.0",
  "http_port": 9750,
  "proxy_opnode": false,
  "sv2_data_dir": "/data",
  "confirm_depth": 15,
  "poll_interval": "1s",
  "chains": [
    {
      "l1_rpc": "http://l1-el:8545",
      "beacon_addr": "http://l1-cl:5052",
      "l2_authrpc": "http://l2a-geth:8551",
      "l2_userrpc": "http://l2a-geth:8545",
      "jwt_secret": "/secrets/jwt-901.hex",
      "rollup_config": "/artifacts/chain-901/rollup.json",
      "user_rpc_port": 9701,
      "interop_mempool_filtering": true
    },
    {
      "l1_rpc": "http://l1-el:8545",
      "beacon_addr": "http://l1-cl:5052",
      "l2_authrpc": "http://l2b-geth:8551",
      "l2_userrpc": "http://l2b-geth:8545",
      "jwt_secret": "/secrets/jwt-902.hex",
      "rollup_config": "/artifacts/chain-902/rollup.json",
      "user_rpc_port": 9702,
      "interop_mempool_filtering": false
    }
  ]
}

## Code changes
1) op-supervisor-v2/cmd/main.go
- Refactor to op-node lifecycle pattern: define `SupervisorMain(ctx *cli.Context, closeApp context.CancelCauseFunc) (cliapp.Lifecycle, error)`. Keep `main()` minimal: set up logging defaults, wire flags, and `cliapp.LifecycleCmd(SupervisorMain)`.
- Parse user_rpc_port and interop_mempool_filtering per chain; pass into AddChain/VirtualNodeConfig.

2) supervisor/virtual_node/virtual_node.go
- Add `StartVirtualNode(..., userRPCListenAddr, userRPCPort, interopMempoolFiltering)`.
- Set nodeCfg.RPC.ListenAddr/ListenPort to requested values (default 127.0.0.1:0 today).
- If interopMempoolFiltering is true, set the op-node config accordingly (mirror ethCfg.InteropMempoolFiltering wiring in tests or add a temporary flag until upstream exposes it).

3) supervisor/chain_orchestrator.go
- Extend VirtualNodeConfig to include UserRPCListenAddr, UserRPCPort, InteropMempoolFiltering.
- Capture selected port in ChainContainer.virtualOpNodeUserRPC as http://<addr>:<port>.
- Include chosen port in /status.

4) Reverse proxy
- Keep proxy; default proxy.opnode=false for Kurtosis.

## Kurtosis wiring
- One SV2 service; per chain only EL (no standalone op-node).
- Provide SV2 config with user_rpc_port per chain.
- Batchers/proposers point directly to http://sv2:<port>.

## Risks
- Interop mempool flag: may require upstream expose; fallback to build tag/temporary config.
- Port conflicts: fail fast with clear logs.

## Milestones
- Milestone 1: Lifecycle refactor + config plumbing
  - [ ] Refactor CLI to lifecycle pattern (SupervisorMain + cliapp.LifecycleCmd)
  - [ ] Extend JSON schema: user_rpc_port, interop_mempool_filtering
  - [ ] Parse and validate in CLI; wire into AddChain/VirtualNodeConfig
  - Testing:
    - [ ] Sysgo (in-process): invoke SupervisorMain with two chains; wait for /healthz, validate /status
- Milestone 2: Virtual node port binding
  - [ ] Update StartVirtualNode to bind fixed addr/port
  - [ ] Return effective user RPC URL; store in ChainContainer
  - [ ] Expose per-chain RPC in /status
  - Testing:
    - [ ] Sysgo: two chains bind two distinct ports; TCP connect succeeds on both; /status shows exact URLs
    - [ ] Sysgo: simple JSON-RPC (e.g., eth_blockNumber) succeeds on each port
- Milestone 3: Interop mempool filtering
  - [ ] Add flag to VirtualNodeConfig
  - [ ] Apply to op-node config; add tests
  - Testing:
    - [ ] Sysgo: enable flag; exercise path by sending interop-relevant txs and verifying expected behavior (no mocks)
- Milestone 4: Proxy behavior
  - [ ] Keep reverse proxy behind flag; default off in Kurtosis
  - [ ] Test both direct and proxied paths
  - Testing:
    - [ ] Sysgo: proxy disabled → proxy endpoints 503; enabled → parity with direct endpoints for basic methods
- Milestone 5: Kurtosis integration
  - [ ] Add SV2 service with multi-chain config template
  - [ ] Only EL per chain; remove standalone op-node
  - [ ] Point batchers/proposers to per-chain SV2 ports
  - Testing:
    - [ ] Sysgo/Kurtosis: bring up devnet; SV2 health OK; /status shows both chains
    - [ ] Sysgo/Kurtosis: batchers post via direct ports; safe advances on both chains
    - [ ] Sysgo/Kurtosis: rollback admin endpoint works with fixed ports
- Milestone 6: Docs and examples
  - [ ] Document config fields and endpoints
  - [ ] Example Kurtosis YAML snippet and env vars
  - Testing:
    - [ ] Sysgo smoke: minimal script validates example config runs to healthy state

## Test implementation location
- Prefer sysgo-based tests alongside existing SV2 tests.
- Add: op-devstack/sysgo/sv2_kurtosis_features_test.go to cover per-chain ports, proxy toggling, mempool filtering, and rollback behavior without mocks.
