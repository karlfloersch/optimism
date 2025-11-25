# op-interop-filter

A service that validates interop executing messages for op-geth transaction filtering, using LogsDB for storage and built-in reorg detection.

## Overview

This service implements `supervisor_checkAccessList` RPC that op-geth calls to validate interop transactions. It ingests blocks from L2 chains into LogsDB and serves validation requests from the cached data.

**Key design**: Uses op-supervisor's LogsDB for storage. Reorg detection is automatic - if block ingestion fails due to parent hash mismatch, failsafe triggers.

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                     op-interop-filter                        │
│                                                              │
│  ┌──────────────────────────────────────────────────────┐   │
│  │                  Chain Ingesters                      │   │
│  │  ┌─────────┐  ┌─────────┐  ┌─────────┐              │   │
│  │  │Chain A  │  │Chain B  │  │Chain C  │  ...         │   │
│  │  │Ingester │  │Ingester │  │Ingester │              │   │
│  │  └────┬────┘  └────┬────┘  └────┬────┘              │   │
│  │       │            │            │                    │   │
│  │       ▼            ▼            ▼                    │   │
│  │  ┌─────────┐  ┌─────────┐  ┌─────────┐              │   │
│  │  │ LogsDB  │  │ LogsDB  │  │ LogsDB  │              │   │
│  │  │(Chain A)│  │(Chain B)│  │(Chain C)│              │   │
│  │  └─────────┘  └─────────┘  └─────────┘              │   │
│  └──────────────────────────────────────────────────────┘   │
│                           │                                  │
│                           ▼                                  │
│  ┌──────────────┐   ┌──────────┐   ┌─────────────────────┐  │
│  │  RPC Handler │──▶│Contains()│   │ Failsafe (atomic)   │  │
│  │              │   │          │   │ - set on reorg      │  │
│  │checkAccessLst│   │          │   │ - checked on RPC    │  │
│  └──────────────┘   └──────────┘   └─────────────────────┘  │
└─────────────────────────────────────────────────────────────┘
```

## How It Works

### Startup & Backfill
1. Connect to each L2 RPC
2. Get current head block
3. Calculate start block (24 hours ago based on block time)
4. `forceBlock()` to set LogsDB starting point
5. Backfill: fetch all blocks from start to head, ingest into LogsDB
6. Mark chain as "ready" (not backfilling)

### Steady State
1. Subscribe to new blocks via `eth.WatchHeadChanges()` (or polling for HTTP)
2. For each new block:
   - Fetch receipts
   - `AddLog()` for each log
   - `SealBlock()` with parent hash
3. If `SealBlock()` fails (parent mismatch = reorg) → **failsafe = true**

### Serving Requests
1. `checkAccessList` RPC comes in
2. Check failsafe - if true, return `ErrFailsafeEnabled`
3. Check all chains are ready (not backfilling) - if not, return error
4. Parse access entries via `types.ParseAccess()`
5. For each entry, call `logsDB.Contains(query)`
6. Return nil if all valid, error if any invalid

## Directory Structure

```
op-interop-filter/
├── cmd/
│   └── main.go              # CLI entry point
├── filter/
│   ├── service.go           # Service lifecycle
│   ├── config.go            # Config from CLI
│   ├── backend.go           # Coordinates chains + failsafe
│   ├── chain.go             # Per-chain ingester + LogsDB
│   └── frontend.go          # RPC handlers
├── flags/
│   └── flags.go             # CLI flags
├── metrics/
│   └── metrics.go           # Prometheus metrics
└── CLAUDE.md
```

## RPC Interface

| Method | Behavior |
|--------|----------|
| `supervisor_checkAccessList(entries, safety, execDesc)` | Validates via LogsDB.Contains() |
| `admin_getFailsafeEnabled()` | Returns failsafe atomic bool |
| `admin_setFailsafeEnabled(bool)` | No-op or error (we don't support manual set) |

## Failsafe Behavior

**Triggers**:
- Block ingestion fails due to parent hash mismatch (reorg detected)
- Any unrecoverable error during ingestion

**Effect**:
- `admin_getFailsafeEnabled()` returns `true`
- `checkAccessList()` returns `ErrFailsafeEnabled`
- All interop transactions will be rejected by op-geth

**Recovery**: Restart service. Failsafe is in-memory only.

## Backfill Behavior

During backfill:
- `checkAccessList()` returns error (chain not ready)
- Metrics indicate backfill progress
- Logs show backfill status

After backfill:
- Chain marked as ready
- Requests served normally

## Configuration

**Required**:
```
--l2-rpcs=chainID:rpcURL,chainID:rpcURL,...
```

**Optional**:
```
--data-dir=/path/to/data          # LogsDB storage (default: in-memory)
--backfill-hours=24               # How far back to backfill (default: 24)
--rpc.addr=0.0.0.0                # RPC listen address
--rpc.port=8560                   # RPC listen port
--metrics.enabled                 # Enable Prometheus metrics
--metrics.port=7300               # Metrics port
```

## Dependencies

**From op-supervisor** (import, don't copy):
- `op-supervisor/supervisor/backend/db/logs` - LogsDB
- `op-supervisor/supervisor/types` - Access, ParseAccess, ChecksumArgs, etc.

**From op-service**:
- `op-service/cliapp` - Lifecycle
- `op-service/rpc` - RPC server
- `op-service/eth` - WatchHeadChanges, types
- `op-service/sources` - EthClient
- `op-service/log`, `op-service/metrics` - Standard infra

## Metrics

```
op_interop_filter_info{version}
op_interop_filter_up
op_interop_filter_failsafe_enabled              # 1 if failsafe triggered
op_interop_filter_chain_ready{chain_id}         # 1 if chain finished backfill
op_interop_filter_chain_head{chain_id}          # Latest ingested block number
op_interop_filter_backfill_progress{chain_id}   # Blocks backfilled / total
op_interop_filter_check_access_list_total       # Total requests
op_interop_filter_check_access_list_errors      # Failed validations
op_interop_filter_reorg_detected{chain_id}      # Counter, increments on reorg
```

## Implementation Notes

### LogsDB Usage

```go
// Startup - set starting point
db.forceBlock(startBlock, timestamp)

// Backfill - add historical blocks
for block := startBlock+1; block <= head; block++ {
    receipts := fetchReceipts(block)
    for _, receipt := range receipts {
        for logIdx, log := range receipt.Logs {
            db.AddLog(logHash, parentBlock, logIdx, execMsg)
        }
    }
    db.SealBlock(parentHash, blockID, timestamp)
}

// Steady state - same pattern with subscribed blocks

// Serving - just call Contains
seal, err := db.Contains(types.ContainsQuery{
    Timestamp: access.Timestamp,
    BlockNum:  access.BlockNumber,
    LogIdx:    access.LogIndex,
    Checksum:  access.Checksum,
})
```

### Block Subscription

```go
// Using eth.WatchHeadChanges with auto-reconnect
sub := gethevent.ResubscribeErr(10*time.Second, func(ctx context.Context, err error) (gethevent.Subscription, error) {
    return eth.WatchHeadChanges(ctx, ethClient, func(ctx context.Context, head eth.L1BlockRef) {
        c.ingestBlock(ctx, head)
    })
})
```

### Reorg Detection

Built into LogsDB - `SealBlock()` checks:
```go
if l.blockHash != parent {
    return fmt.Errorf("%w: cannot apply block...", types.ErrConflict)
}
```

When this fails, we set failsafe and stop ingesting.

## Testing Strategy

1. **Unit tests**: Test config parsing, access list parsing
2. **Integration tests**:
   - Spin up anvil/geth
   - Ingest blocks
   - Verify Contains() works
   - Simulate reorg, verify failsafe triggers
3. **Sysgo tests**: Full system test with op-geth calling our service
