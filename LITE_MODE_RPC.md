## External Safe-Blocks RPC for Safe/Finalized Heads (op-node)

> Note: This plan is written before implementation. As we learn more during development and testing, we may change or disregard specific implementation details outlined here.

This document describes enabling op-node to source safe and finalized L2 heads from an external RPC by configuring a new CLI option. This is not a new "mode"; the behavior is enabled implicitly when the external safe-blocks RPC is configured.

### Implementation update (current)
- Single-block advancement per tick for both safe and finalized to favor simplicity and correctness.
- Commit-based ingestion: when a needed block is not present locally, build an `eth.ExecutionPayloadEnvelope` from RPC using `op-service/sources.RPCBlock.ExecutionPayloadEnvelope(false)` and ingest it with `Engine.CommitBlock` before applying labels.
- Common-ancestor reconciliation: start from the local safe tip, walk back comparing remote hashes at the same height to find a common ancestor, then only ingest/apply the next block (`ancestor.Number + 1`).
- Finalized advances conservatively: never beyond local unsafe or local safe and only when the block exists locally.
- EngineController guards: derivation-based promotions for safe/finalized are inert while SAFE_BLOCKS_RPC is enabled; unsafe remains unchanged. Head-setting methods log changes and trigger FCU via `TryUpdateEngine`.

### Usage
Run unit tests for safeblocks:
```bash
cd /Users/karl/workspace/optimism-1/op-node/safeblocks
go test -v -count=1
```

Run the acceptance test (external EL):
```bash
cd /Users/karl/workspace/optimism-1/op-acceptance-tests/tests/sync_tester/sync_tester_ext_el
OP_NODE_SAFE_BLOCKS_RPC=xxx \
  CIRCLECI_PARAMETERS_SYNC_TEST_OP_NODE_DISPATCH=true \
  TAILSCALE_NETWORKING=true \
  NETWORK_PRESET=op-sepolia \
  GOMAXPROCS=5 \
  go test -run '^TestSyncTesterExtEL$' -v -count=1 | tee test_safe_blocks.log | cat
```

Sync-to-tip variant:
```bash
cd /Users/karl/workspace/optimism-1/op-acceptance-tests/tests/sync_tester/sync_tester_ext_el_tip
OP_NODE_SAFE_BLOCKS_RPC=xxx \
  CIRCLECI_PARAMETERS_SYNC_TEST_OP_NODE_DISPATCH=true \
  TAILSCALE_NETWORKING=true \
  NETWORK_PRESET=op-sepolia \
  GOMAXPROCS=5 \
  go test -run '^TestSyncTesterExtELTip$' -v -count=1 | tee test_safe_blocks_tip.log | cat
```

Quick verification greps:
```bash
grep -n "SAFE_BLOCKS_RPC test env detected\|Safe-blocks RPC enabled: skipping local finalizer wiring" test_safe_blocks*.log | head -n 20 | cat
grep -n "Applying safe block" test_safe_blocks*.log | head -n 20 | cat
grep -n "Set finalized head\|Set safe head\|Set local safe head" test_safe_blocks*.log | head -n 20 | cat
grep -n "NewPayloadV[0-9]\|ForkchoiceUpdatedV[0-9]" test_safe_blocks*.log | head -n 20 | cat
```

### 1) Goals and constraints
- Disable existing safe derivation and finality logic when an external safe-blocks RPC is configured.
- Do not touch unsafe logic/paths.
- Source safe/finalized from an external RPC; apply locally and call FCU.
- On mismatch with previous safe: reorg EL to the external safe (or shared ancestor if desired), then FCU.
- Explicitly exclude Interop/indexing; the external safe-blocks RPC feature must not run there.
- Minimize diffs: handful of if-guards + a small poller + flags/wiring.

### 2) Flags and config
- Added to op-node CLI flags (implemented):
  - --safe-blocks-rpc (string; stubbed; logs and exits)
  - Env: OP_NODE_SAFE_BLOCKS_RPC (for in-process tests)
- Pending (not yet implemented):
  - --safe-blocks-rpc-poll-interval (duration; default 2s)
- Add to op-node CLI flags:
  - `--safe-blocks-rpc` (string; if set, enables external safe/finalized sourcing)
  - `--safe-blocks-rpc-poll-interval` (duration; default 2s)
- Startup constraint:
  - If `--safe-blocks-rpc` is set AND interop/indexing is configured (e.g., `--interop.rpc.addr` set or supervisor indexing mode), error and exit.

Example additions in `op-node/flags/flags.go`:
```go
// new flags (add to optional flags)
SafeBlocksRPC = &cli.StringFlag{
    Name:     "safe-blocks-rpc",
    Usage:    "External L2 RPC endpoint to query safe/finalized heads from",
    EnvVars:  prefixEnvVars("SAFE_BLOCKS_RPC"),
    Category: RollupCategory,
}
SafeBlocksRPCPollInterval = &cli.DurationFlag{
    Name:     "safe-blocks-rpc-poll-interval",
    Usage:    "Polling interval for external safe-blocks RPC updates",
    EnvVars:  prefixEnvVars("SAFE_BLOCKS_RPC_POLL_INTERVAL"),
    Value:    time.Second * 2,
    Category: RollupCategory,
}

// ensure to append to optionalFlags in init():
optionalFlags = append(optionalFlags, SafeBlocksRPC, SafeBlocksRPCPollInterval)
```

Interop exclusion at service startup (pseudocode):
```go
if ctx.IsSet(flags.SafeBlocksRPC.Name) {
    if ctx.IsSet(flags.InteropRPCAddr.Name) || indexingMode {
        return fmt.Errorf("safe-blocks RPC cannot run with interop/indexing enabled")
    }
}
```

### 3) EngineController: guard and minimal changes
- Add a field to `EngineController`:
```go
type EngineController struct {
    // ... existing fields ...
    safeBlocksRPCEnabled bool
}
```
- Set in constructor wiring (see Driver wiring below).
- Add early-returns at the top of these methods (one-liners), so safe/finality code paths never hydrate when the safe-blocks RPC is configured:

```go
func (e *EngineController) TryUpdatePendingSafe(ctx context.Context, ref eth.L2BlockRef, concluding bool, source eth.L1BlockRef) {
    if e.safeBlocksRPCEnabled { return }
    // existing body
}

func (e *EngineController) TryUpdateLocalSafe(ctx context.Context, ref eth.L2BlockRef, concluding bool, source eth.L1BlockRef) {
    if e.safeBlocksRPCEnabled { return }
    // existing body
}

func (e *EngineController) PromoteSafe(ctx context.Context, ref eth.L2BlockRef, source eth.L1BlockRef) {
    if e.safeBlocksRPCEnabled { return }
    // existing body
}

func (e *EngineController) PromoteFinalized(ctx context.Context, ref eth.L2BlockRef) {
    if e.safeBlocksRPCEnabled { return }
    // existing body
}
```

Notes:
- We intentionally do not gate `TryUpdateUnsafe`, leaving unsafe behavior unchanged.
- We do not rely on any `IsInterop(...)` checks; the feature simply does not run in interop environments.

### 4) SyncDeriver: avoid attributes hydration when using safe-blocks RPC
In `SyncDeriver.SyncStep()` guard the pending-safe poke when the safe-blocks RPC is configured:
```go
if s.Engine.SafeBlocksRPCEnabled() { // add a getter or expose the bool
    return
}
s.Engine.RequestPendingSafeUpdate(s.Ctx)
```

### 5) Driver wiring
- In `driver.NewDriver(...)`:
  - Determine `safeBlocksRPCEnabled := cfg.SafeBlocksRPC != ""` and pass it through.
  - Pass `safeBlocksRPCEnabled` into the `EngineController` (via constructor arg or setter).
  - If enabled (and not interop/indexing), do NOT construct/register the `Finalizer`.
  - Construct the poller (below) and store it on the driver.
- In `Driver.Start()`, if enabled, start the poller goroutine.

Example driver wiring (pseudocode):
```go
ec := engine.NewEngineController(driverCtx, l2, log, metrics, cfg, syncCfg, sys.Register("engine-controller", nil))
ec.SetSafeBlocksRPCEnabled(driverCfg.SafeBlocksRPCEnabled)

var finalizer Finalizer
if !driverCfg.SafeBlocksRPCEnabled {
    // existing finalizer wiring
}

if driverCfg.SafeBlocksRPCEnabled {
    sb := safeblocks.New(safeblocks.Config{RPC: cfg.SafeBlocksRPC, Interval: cfg.SafeBlocksRPCPollInterval}, log, ec, l2)
    s.safeblocks = sb
}
```

### 6) Safe-Blocks RPC poller: minimal skeleton
Create `op-node/safeblocks/safeblocks.go`.

```go
package safeblocks

import (
    "context"
    "time"

    "github.com/ethereum/go-ethereum/common"
    "github.com/ethereum/go-ethereum/log"

    "github.com/ethereum-optimism/optimism/op-service/client"
    "github.com/ethereum-optimism/optimism/op-service/eth"
)

type Engine interface {
    // Minimal surface used by the poller
    UnsafeL2Head() eth.L2BlockRef
    SafeL2Head() eth.L2BlockRef
    Finalized() eth.L2BlockRef
    SetSafeHead(eth.L2BlockRef)
    SetLocalSafeHead(eth.L2BlockRef)
    SetFinalizedHead(eth.L2BlockRef)
    TryUpdateEngine(ctx context.Context)
}

type L2 interface {
    L2BlockRefByHash(ctx context.Context, hash common.Hash) (eth.L2BlockRef, error)
}

type Config struct {
    RPC      string
    Interval time.Duration
}

type Client struct {
    log    log.Logger
    cfg    Config
    eng    Engine
    l2     L2
    cancel context.CancelFunc
}

func New(cfg Config, log log.Logger, eng Engine, l2 L2) *Client {
    return &Client{cfg: cfg, log: log, eng: eng, l2: l2}
}

func (c *Client) Start(ctx context.Context) error {
    if c.cfg.RPC == "" { return nil }
    cctx, cancel := context.WithCancel(ctx)
    c.cancel = cancel
    cli := client.NewBaseRPCClient(c.cfg.RPC)
    ticker := time.NewTicker(c.cfg.Interval)
    go func() {
        defer ticker.Stop()
        for {
            select {
            case <-cctx.Done():
                return
            case <-ticker.C:
                c.tick(cctx, cli)
            }
        }
    }()
    return nil
}

func (c *Client) Close() { if c.cancel != nil { c.cancel() } }

func (c *Client) tick(ctx context.Context, cli *client.BaseRPCClient) {
    extSafe, ok1 := fetchBlockByTag(ctx, cli, "safe")
    extFin, ok2 := fetchBlockByTag(ctx, cli, "finalized")
    if !ok1 && !ok2 { return }

    // Apply finalized first, then safe; ensure finalized ≤ safe
    if ok2 { c.applyFinalized(ctx, extFin) }
    if ok1 { c.applySafe(ctx, extSafe) }
}

// fetchBlockByTag queries external RPC for a block reference by tag ("safe"/"finalized").
func fetchBlockByTag(ctx context.Context, cli *client.BaseRPCClient, tag string) (eth.L2BlockRef, bool) {
    // Implement using eth_getBlockByNumber(tag,false) and map to L2BlockRef (hash, parent, number, time, l1origin fields if available)
    return eth.L2BlockRef{}, false
}

func (c *Client) applySafe(ctx context.Context, ext eth.L2BlockRef) {
    localSafe := c.eng.SafeL2Head()
    if localSafe.Hash == ext.Hash { return }
    if _, err := c.l2.L2BlockRefByHash(ctx, ext.Hash); err == nil {
        // Known locally; snap safe to ext and FCU
        c.eng.SetLocalSafeHead(ext)
        c.eng.SetSafeHead(ext)
        c.eng.TryUpdateEngine(ctx)
        return
    }
    // Unknown or conflicting: simplest snap to external safe
    c.eng.SetLocalSafeHead(ext)
    c.eng.SetSafeHead(ext)
    c.eng.TryUpdateEngine(ctx)
}

func (c *Client) applyFinalized(ctx context.Context, ext eth.L2BlockRef) {
    localFin := c.eng.Finalized()
    if localFin.Hash == ext.Hash { return }
    // Ensure not ahead of safe; the engine controller already validates FCU state
    c.eng.SetFinalizedHead(ext)
    c.eng.TryUpdateEngine(ctx)
}
```

### 7) External RPC details
- Recommended: external EL-compatible L2 RPC supporting tags:
  - `eth_getBlockByNumber("safe", false)`
  - `eth_getBlockByNumber("finalized", false)`
- Alternative: external OP Node RPC (`rollup_syncStatus`) plus `eth_getBlockByNumber` for block hashes.
- Poll interval: configurable (default 2s).

### 8) Reorg logic (simple and minimal)
- If external safe hash ≠ local safe hash:
  - If external block is known locally and is a descendant of local safe: set safe to external and FCU.
  - Else: snap to external safe (or implement shared-ancestor search if preferred). Then FCU.
- Apply finalized similarly; enforce `finalized ≤ safe`.

### 9) Interop exclusion
- Safe-blocks RPC must not run when:
  - Interop RPC (`--interop.rpc.addr`) is configured, or
  - Node is in indexing/supervisor-managed mode.
- Enforce at startup; error if `--safe-blocks-rpc` is set alongside these configs.

### 10) Tests
- Unit tests:
  - `EngineController` with safe-blocks enabled: the 4 guarded methods are inert.
  - `SyncDeriver.SyncStep` with safe-blocks enabled: does not call `RequestPendingSafeUpdate`.
- Integration tests:
  - Safe-blocks RPC advances safe/finalized from external source → `ForkchoiceUpdateEvent` reflects updates, FCU returns valid.
  - Reorg case: external safe jumps branches → EL snaps/reorgs accordingly, FCU valid.
  - Interop: enabling safe-blocks with interop settings errors at startup.
- Non-regression: unsafe payload/FCU paths unchanged.

### 11) Estimated diff size
- `engine_controller.go`: +1 field, +4 one-line guards.
- `sync_deriver.go`: +1 guard.
- `driver.go`: wiring for safe-blocks enable + poller start, skip finalizer.
- `flags.go`: 2 flags.
- `safeblocks.go`: ~150–250 LOC.

Total: ~180–300 LOC, mostly additive; unsafe logic untouched.


### End-to-end testing plan (Sepolia)

This complements unit/integration tests with practical end-to-end checks on Sepolia before and after changes.

- Baseline goal (pre-implementation): Run a standard op-geth + op-node, consensus-sync Sepolia to a small height, and verify correctness by comparing a specific block with a trusted remote.
- Post-change goal: Repeat the same steps with safe-blocks RPC configured; confirm behavior is preserved. Then extend to sync-to-tip including unsafe P2P gossip.

Prereqs
- A non-committed env file `op-up/external-l1.env` containing at least:
  - `L1_RPC_URL` (Sepolia L1 RPC)
  - `REMOTE_L2_RPC_URL` (trusted external L2 RPC for block comparison)
  - `L2_JWT_PATH` (path to the JWT secret used by op-geth/op-node)
  - Optional ports: `OP_GETH_HTTP_PORT`, `OP_NODE_RPC_PORT` (or defaults)

Baseline script (no safe-blocks RPC)
```bash
#!/usr/bin/env bash
set -euo pipefail

source op-up/external-l1.env

: "${L1_RPC_URL:?set in env}"     # Sepolia L1
: "${REMOTE_L2_RPC_URL:?set in env}" # Trusted L2 for comparison
: "${L2_JWT_PATH:?set in env}"

OP_GETH_HTTP_PORT=${OP_GETH_HTTP_PORT:-9545}
OP_NODE_RPC_PORT=${OP_NODE_RPC_PORT:-9546}

# 1) Start op-geth (execution)
# Example (adjust paths/flags as needed):
# op-geth \
#   --http --http.addr 127.0.0.1 --http.port ${OP_GETH_HTTP_PORT} \
#   --authrpc.addr 127.0.0.1 --authrpc.port 8551 --authrpc.jwtsecret ${L2_JWT_PATH} \
#   --sepolia

# 2) Start op-node pointing at L1 + op-geth (no safe-blocks RPC)
# op-node \
#   --l1 ${L1_RPC_URL} \
#   --l2 http://127.0.0.1:8551 \
#   --l2.jwt-secret ${L2_JWT_PATH} \
#   --rpc.addr 0.0.0.0 --rpc.port ${OP_NODE_RPC_PORT}

# 3) Wait until local EL reaches block 20
TARGET_HEX=0x14
until curl -s -X POST localhost:${OP_GETH_HTTP_PORT} \
  -H 'content-type: application/json' \
  --data '{"jsonrpc":"2.0","id":1,"method":"eth_getBlockByNumber","params":["'${TARGET_HEX}'",false]}' | jq -e '.result.hash' >/dev/null; do
  sleep 1
done

# 4) Compare local vs remote block 20 hashes
LOCAL_HASH=$(curl -s -X POST localhost:${OP_GETH_HTTP_PORT} \
  -H 'content-type: application/json' \
  --data '{"jsonrpc":"2.0","id":1,"method":"eth_getBlockByNumber","params":["'${TARGET_HEX}'",false]}' | jq -r '.result.hash')
REMOTE_HASH=$(curl -s -X POST ${REMOTE_L2_RPC_URL} \
  -H 'content-type: application/json' \
  --data '{"jsonrpc":"2.0","id":1,"method":"eth_getBlockByNumber","params":["'${TARGET_HEX}'",false]}' | jq -r '.result.hash')

test "${LOCAL_HASH}" = "${REMOTE_HASH}" || { echo "hash mismatch at block 20"; exit 1; }
echo "Baseline OK: block 20 matches (${LOCAL_HASH})"
```

Post-change script (safe-blocks RPC enabled)
- Repeat the baseline with the op-node started using:
  - `--safe-blocks-rpc ${REMOTE_L2_RPC_URL}`
  - `--safe-blocks-rpc-poll-interval 2s` (or as configured)
- Validate the same comparison at block 20 succeeds.

Final test: sync to tip with P2P gossip
- Start op-node with P2P enabled (standard P2P flags) and ensure connection to peers.
- Let the node sync to tip, including unsafe sequencer gossip, and observe:
  - Local unsafe head advancing steadily
  - Safe-blocks RPC still updating safe/finalized labels without interfering with unsafe
  - No regressions in FCU results (no invalid forkchoice state errors)

Outcome
- Baseline parity at block 20 without safe-blocks RPC
- Parity at block 20 with safe-blocks RPC
- Sustained sync-to-tip with P2P gossip while safe-blocks RPC is active


### Milestones
- [x] Milestone 1: Baseline Sepolia sync
  - Run a test that proves we can sync Sepolia
  - SOLVED: ./op-acceptance-tests/tests/sync_tester/sync_tester_ext_el and sync_tester_ext_el_tip can be run to prove safe head progression.
- [x] Milestone 2: Introduce minimal guards (no behavior change yet)
  - Implemented --safe-blocks-rpc (default off). When set, op-node logs and exits (stub). Baseline passes when unset.
  - Add inert feature-flag plumbing; ensure baseline still passes when off and `--safe-blocks-rpc` is unset.
- [ ] Milestone 3: Enable safe-blocks RPC, preserve baseline behavior
  - Start with `--safe-blocks-rpc` and re-run block 20 parity check.
- [ ] Milestone 4: Full sync-to-tip with P2P
  - Enable P2P and verify sustained sync to tip, including unsafe gossip.



