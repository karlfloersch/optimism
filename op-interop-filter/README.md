# `op-interop-filter`

Issues: [monorepo](https://github.com/ethereum-optimism/optimism/issues?q=is%3Aissue%20state%3Aopen%20label%3AA-op-interop-filter)

Pull requests: [monorepo](https://github.com/ethereum-optimism/optimism/pulls?q=is%3Aopen+is%3Apr+label%3AA-op-interop-filter)

Specs:
- [interop specs](https://github.com/ethereum-optimism/specs/tree/main/specs/interop)

`op-interop-filter` is a lightweight service that validates interop executing messages for op-geth transaction filtering. It maintains a local LogsDB of recent blocks and serves `supervisor_checkAccessList` requests.

This is a simplified alternative to [op-supervisor] for deployments that only need transaction filtering without full cross-chain safety tracking.

[op-supervisor]: ../op-supervisor/README.md

## Quickstart

```bash
make op-interop-filter

./bin/op-interop-filter \
  --l2-rpcs="11155420:https://your-op-sepolia-rpc" \
  --backfill-duration=5m \
  --rpc.port=8560 \
  --metrics.enabled \
  --metrics.port=7300
```

## Usage

### Build from source

```bash
# from repo root:
make op-interop-filter
./bin/op-interop-filter --help
```

### Run from source

```bash
# from op-interop-filter dir:
go run ./cmd --help
```

### Build docker image

See `op-interop-filter` docker-bake target.

## Overview

### Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                     op-interop-filter                       │
│                                                             │
│  ┌──────────────────────────────────────────────────────┐  │
│  │                  Chain Ingesters                      │  │
│  │  ┌─────────┐  ┌─────────┐  ┌─────────┐              │  │
│  │  │Chain A  │  │Chain B  │  │Chain C  │  ...         │  │
│  │  │Ingester │  │Ingester │  │Ingester │              │  │
│  │  └────┬────┘  └────┬────┘  └────┬────┘              │  │
│  │       │            │            │                    │  │
│  │       ▼            ▼            ▼                    │  │
│  │  ┌─────────┐  ┌─────────┐  ┌─────────┐              │  │
│  │  │ LogsDB  │  │ LogsDB  │  │ LogsDB  │              │  │
│  │  │(Chain A)│  │(Chain B)│  │(Chain C)│              │  │
│  │  └─────────┘  └─────────┘  └─────────┘              │  │
│  └──────────────────────────────────────────────────────┘  │
│                           │                                 │
│                           ▼                                 │
│  ┌──────────────┐   ┌──────────┐   ┌─────────────────────┐ │
│  │  RPC Handler │──▶│Contains()│   │ Failsafe (atomic)   │ │
│  │              │   │          │   │ - set on reorg      │ │
│  │checkAccessLst│   │          │   │ - checked on RPC    │ │
│  └──────────────┘   └──────────┘   └─────────────────────┘ │
└─────────────────────────────────────────────────────────────┘
```

### How It Works

**Startup & Backfill:**
1. Connect to each configured L2 RPC
2. Get current head block
3. Calculate start block based on `--backfill-duration`
4. Backfill historical blocks into LogsDB
5. Mark chain as "ready"

**Steady State:**
1. Poll for new blocks every 2 seconds
2. For each new block, fetch receipts and add logs to LogsDB
3. Seal block with parent hash validation
4. If parent hash doesn't match (reorg), trigger failsafe

**Request Handling:**
1. Check failsafe - if enabled, reject with `ErrFailsafeEnabled`
2. Check all chains are ready - if not, reject with `ErrUninitialized`
3. Validate safety level is `"unsafe"`
4. Parse and validate each access entry via LogsDB

## Configuration

### Required Flags

| Flag | Description | Example |
|------|-------------|---------|
| `--l2-rpcs` | Comma-separated `chainID:rpcURL` pairs | `11155420:https://rpc.example.com` |

### Optional Flags

| Flag | Default | Description |
|------|---------|-------------|
| `--data-dir` | (temp dir) | Directory for LogsDB storage |
| `--backfill-duration` | `24h` | How far back to backfill |
| `--rpc.addr` | `0.0.0.0` | RPC listen address |
| `--rpc.port` | `8545` | RPC listen port |
| `--metrics.enabled` | `false` | Enable Prometheus metrics |
| `--metrics.port` | `7300` | Metrics port |

## RPC Interface

| Method | Description |
|--------|-------------|
| `supervisor_checkAccessList(entries, safety, execDesc)` | Validates interop access list entries |
| `admin_getFailsafeEnabled()` | Returns failsafe status |
| `admin_setFailsafeEnabled(bool)` | Not supported (returns error) |

**Note:** Only `"unsafe"` safety level is currently supported.

## Design Decisions

### Why Not Integrate with op-conductor?

We considered integrating with op-conductor to ensure we always talk to the leader sequencer. However, we feel confident without it because:

1. **Gossiped blocks go through proxyd anyway** - All unsafe blocks that reach public RPCs have already been gossiped through the p2p network, which provides consistency.

2. **The failure case is narrow** - The main scenario where talking to a non-leader causes issues is if an RPC is misconfigured. But a misconfigured sequencer node would have the same problem.

3. **Simplicity** - Direct RPC connection is simpler to configure and operate.

For HA sequencer deployments needing stronger guarantees, see [Future Improvements](#future-improvements).

### Why a Separate Service from op-supervisor?

`op-interop-filter` is designed for simpler deployments that only need transaction filtering:

- **Lighter weight** - No cross-chain safety tracking, no L1 derivation
- **Simpler operations** - Single binary, minimal configuration
- **Faster startup** - Only backfills recent blocks, not full history

Use `op-supervisor` when you need full cross-chain safety verification.

## Failure Modes

### Reorg Detection

When block ingestion fails due to parent hash mismatch:
- Failsafe is enabled atomically
- All `checkAccessList` requests return `ErrFailsafeEnabled`
- op-geth will reject all interop transactions
- **Recovery:** Restart the service

### Backfill In Progress

During initial backfill:
- `checkAccessList` returns `ErrUninitialized`
- Metrics indicate backfill progress
- Service becomes ready once all chains complete backfill

### RPC Unavailability

If L2 RPC becomes unavailable:
- Block ingestion stalls
- Existing LogsDB data continues to serve requests
- New blocks won't be validated until RPC recovers

## Future Improvements

### Leader Proxy

For high-availability sequencer setups, a transparent proxy that:
- Polls `admin_sequencerActive` on multiple op-node backends
- Forwards requests to whichever node is the current leader
- Automatically fails over when leadership changes

This keeps conductor logic separate from the filter service.

```
tx-filter → leader-proxy → op-node-1 (leader)
                         → op-node-2
                         → op-node-3
```

### Consensus Proxy (Multi-RPC Validation)

A proxy that queries multiple RPCs and compares responses:
- Returns result only if backends agree
- Errors on disagreement
- Configurable quorum requirements

### Additional Safety Levels

Support for `"safe"` and `"finalized"` safety levels, requiring L1 derivation tracking.

### Cross-Chain Execution Validation

Full validation of `executingDescriptor` timing constraints and cross-chain message validity windows.

### LogsDB Pruning

Automatic pruning of blocks older than a configurable threshold.

## Testing

### Unit Tests

```bash
go test ./op-interop-filter/...
```

### Testing Tools

The repo includes tools for manual testing:

```bash
# Dashboard - view metrics
go build -o ./bin/filter-dashboard ./op-interop-filter/cmd/dashboard
./bin/filter-dashboard --filter-metrics=http://localhost:7300/metrics

# Spammer - validation testing
go build -o ./bin/filter-spammer ./op-interop-filter/cmd/spammer
./bin/filter-spammer --l2-rpc="https://rpc" --filter-rpc="http://localhost:8560" --chain-id=11155420

# Overnight test script
./op-interop-filter/scripts/run-overnight.sh
```

### Integration Testing

Integration with op-geth can be tested by:
1. Running op-interop-filter with a testnet RPC
2. Configuring op-geth to use the filter's RPC endpoint
3. Submitting interop transactions and verifying filtering behavior
