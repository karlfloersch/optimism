# Supervisor v2: single process for multi-chain with fixed per-chain op-node ports

## Goals
- Run a single op-supervisor-v2 managing multiple chains in Kurtosis.
- Prefer direct per-chain op-node ports; retain the HTTP reverse proxy (/opnode/{chainId}/) for tests and optional usage.
- Provide configuration for chains, EL endpoints, L1 endpoints, and desired per-chain op-node ports.
- Support interop mempool filtering toggle centrally via SV2.

## Current status
- Milestone 1 complete: library lifecycle + minimal main wired; service loads chains from sv2.config at startup.
- Devstack presets now generate per-chain rollup JSON files (named by numeric ChainID), populate `sv2.json` with all chains, and start SV2 with that config; no manual AddChain in presets.
- Rollup `L2ChainID` is set to match each EL `eth_chainId` in the generated JSON; chain-ID normalization fallback in the service was removed (mismatches fail during startup via downstream validation).
- All tests in `op-devstack/sysgo/supervisor_v2_system_test.go` pass.

## High-level design
- Extend the SV2 CLI multi-chain JSON config with optional user_rpc_port and interop_mempool_filtering per chain.
- Extend virtual_node.StartVirtualNode to accept user RPC listen address/port.
- Extend ChainContainer to store the chosen userRPCPort and expose it via /status.
- Keep reverse proxy available (behind a flag); default off in Kurtosis.
- Adopt the op-node style, cycle-safe CLI structure: keep `SupervisorMain(ctx, closeApp) (cliapp.Lifecycle, error)` in `cmd/main.go` (like `op-node`). The library exposes `NewConfig(...)` and a `New(...)` constructor that returns a `cliapp.Lifecycle`. Sysgo imports the library and calls the constructor directly, avoiding any dependency on `cmd` and thus avoiding circular dependencies.

## Package layout (op-node style, cycle-safe)
- `op-supervisor-v2/` (library)
  - `service.go`: exports `NewConfig(ctx, log)` and `New(ctx context.Context, cfg, log, version, metrics) (cliapp.Lifecycle, error)` similar to `op-node/service.go` + `op-node/node.New` pattern.
  - `flags/`: CLI flag definitions (mirrors `op-node/flags`).
  - `supervisor/...`: runtime logic (virtual node, chain orchestrator, status, etc.).
- `op-supervisor-v2/cmd/main.go` (binary)
  - Minimal wrapper: sets up logging/version, `app.Flags = cliapp.ProtectFlags(flags.Flags)`, `app.Action = cliapp.LifecycleCmd(SupervisorMain)` and defines `SupervisorMain(ctx, closeApp)` which uses the library `NewConfig(...)` and `New(...)` to build the lifecycle, mirroring `op-node/cmd/main.go:RollupNodeMain`.

Dependency flow (no cycles):
`sysgo tests -> op-supervisor-v2 (service, flags, supervisor)`
`cmd/main.go -> op-supervisor-v2`
There is no `op-supervisor-v2 -> sysgo` or `sysgo -> cmd` edge.

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
1) op-supervisor-v2/service.go (new)
- Implement `NewConfig(ctx, log)` in the library (like `op-node/service.go:NewConfig`).
- Implement `New(ctx context.Context, cfg, log, version, metrics) (cliapp.Lifecycle, error)` that constructs and returns the lifecycle instance.
- Parse `sv2.config` (multi-chain JSON) and call AddChain internally for each chain during startup.
- Future: parse `user_rpc_port` and `interop_mempool_filtering` per chain; pass into AddChain/VirtualNodeConfig.

2) op-supervisor-v2/cmd/main.go
- Minimal main: logging defaults, version, `app.Flags = cliapp.ProtectFlags(flags.Flags)`, `app.Action = cliapp.LifecycleCmd(SupervisorMain)`; define `SupervisorMain(ctx, closeApp)` that wires logging/metrics, calls `service.NewConfig(...)`, sets `cfg.Cancel = closeApp`, and returns `service.New(...)`.

3) supervisor/virtual_node/virtual_node.go
- Add `StartVirtualNode(..., userRPCListenAddr, userRPCPort, interopMempoolFiltering)`.
- Set nodeCfg.RPC.ListenAddr/ListenPort to requested values (default 127.0.0.1:0 today).
- If interopMempoolFiltering is true, set the op-node config accordingly (mirror ethCfg.InteropMempoolFiltering wiring in tests or add a temporary flag until upstream exposes it).

4) supervisor/chain_orchestrator.go
- Extend VirtualNodeConfig to include UserRPCListenAddr, UserRPCPort, InteropMempoolFiltering.
- Capture selected port in ChainContainer.virtualOpNodeUserRPC as http://<addr>:<port>.
- Include chosen port in /status.

5) Reverse proxy
- Keep proxy; default proxy.opnode=false for Kurtosis.

## Sysgo integration (in-process, no cycles)
- Sysgo imports the library package, not the `cmd` package.
- Presets now generate per-chain rollup JSON (with `L2ChainID` set from EL) and a populated `sv2.json` for multi-chain startup. The service registers chains internally from JSON.

## Kurtosis wiring
- One SV2 service; per chain only EL (no standalone op-node).
- Provide SV2 config with user_rpc_port per chain.
- Batchers/proposers point directly to http://sv2:<port>.

## Risks
- Config correctness is strict: mismatched `L2ChainID` and EL `eth_chainId` will cause startup to fail via downstream validation. Keep JSON and EL aligned.
- Port conflicts: fail fast with clear logs.

## Milestones
- Milestone 1: Library constructor + minimal main + sysgo harness
  - [x] Keep `SupervisorMain` in `cmd/main.go`; implement `NewConfig` and `New(...)` in library
  - [x] Use in-process sysgo presets that import the library (no `cmd` import)
  - Testing:
    - [x] System: all tests in `supervisor_v2_system_test.go` pass; health/status exercised indirectly
- Milestone 2: Config plumbing (user_rpc_port, interop_mempool_filtering)
  - [ ] Extend JSON schema and flags as needed (no dummies)
  - [ ] Wire fields end-to-end (CLI → AddChain/VirtualNodeConfig → StartVirtualNode)
  - Testing:
    - [ ] Sysgo: two distinct ports bound; /status + JSON-RPC checks
- Milestone 3: Interop mempool filtering
  - [ ] Apply to embedded op-node config where applicable
  - Testing:
    - [ ] Sysgo: enable flag; exercise path with relevant txs
- Milestone 4: Cross-safe persistence
  - [ ] Add cross_db config/flag and default under sv2_data_dir
  - [ ] Implement atomic load/save of cross-safe timestamp and metadata
  - Testing:
    - [ ] Sysgo: advance, restart restore; rollback persists; corrupt DB recovers
- Milestone 5: Kurtosis integration
  - [ ] Add SV2 service with multi-chain config template
  - [ ] Only EL per chain; remove standalone op-node
  - [ ] Point batchers/proposers to per-chain SV2 ports
  - Testing:
    - [ ] Sysgo/Kurtosis: health, safe advancement, rollback behavior
- Milestone 6: Docs and examples
  - [ ] Document config fields and endpoints
  - [ ] Example Kurtosis YAML snippet and env vars
  - Testing:
    - [ ] Sysgo smoke: example config reaches healthy state

Note: We will not land milestones with placeholder or dummy values routed through functions; each milestone completes its wiring fully before moving on.

## Test implementation location
- Prefer sysgo-based tests alongside existing SV2 tests.
- Existing coverage in `op-devstack/sysgo/supervisor_v2_system_test.go` ensures end-to-end behavior under presets; add focused tests for new config fields as they land.
