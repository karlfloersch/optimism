# Lite Mode Design Doc

## Overview

Lite Mode is a new synchronization mode for op-node that disables L1 derivation and instead sources safe/finalized block heads from an external execution client RPC. This allows nodes to operate without performing derivation while still maintaining safe/finalized head progression.

## Motivation

### Why Lite Mode?

1. **Reduced Resource Requirements**: Eliminates the need to process L1 data for derivation
2. **Simplified Operation**: Trust an external source for safe/finalized heads instead of deriving them
3. **Faster Sync**: Can quickly catch up by importing blocks from a trusted source
4. **Flexible Architecture**: Enables new deployment patterns (e.g., lightweight verifier nodes)

### Use Cases

- Lightweight RPC nodes that trust a sequencer or full node
- Quick sync scenarios where full derivation is unnecessary
- Development/testing environments
- Nodes in trusted environments

## Architecture

### High-Level Components

```
┌─────────────────────────────────────────────────────────┐
│ Driver Event Loop                                       │
│  - Disabled: L1 Derivation (RequestPendingSafeUpdate)  │
│  - Enabled: CL Sync (unsafe blocks via P2P)            │
│  - New: LiteModeSync (safe/finalized from remote)      │
└─────────────────────────────────────────────────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────────────┐
│ LiteModeSync Component                                  │
│  1. Poll remote EL for safe/finalized heads            │
│  2. Find common ancestor via parent hash walking        │
│  3. Import blocks into local EL                         │
│  4. Promote safe/finalized heads                        │
└─────────────────────────────────────────────────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────────────┐
│ EngineController                                        │
│  - InsertUnsafePayload() inserts blocks                │
│  - PromoteSafe() updates safe head                     │
│  - PromoteFinalized() updates finalized head           │
└─────────────────────────────────────────────────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────────────┐
│ Local Execution Client                                  │
│  - Stores blocks                                        │
│  - May perform EL sync to fill gaps                    │
└─────────────────────────────────────────────────────────┘
```

### What Changes

**Disabled:**
- ✗ L1 Derivation Pipeline
- ✗ Finalizer Component (not needed in standard mode)
- ✗ Safe head calculation from L1 data

**Enabled:**
- ✓ CL Sync (unsafe block gossip via P2P)
- ✓ EL Sync (execution client snap sync)
- ✓ LiteModeSync (new component)

**Flow:**
1. **Unsafe blocks**: Still received via CL sync (P2P gossip)
2. **Safe blocks**: Fetched from remote EL, validated, and imported
3. **Finalized blocks**: Fetched from remote EL and promoted

## Sync Loop Algorithm

### Core Algorithm

The sync loop runs on a timer (default: 1 second) and performs these steps:

#### Step 1: Check EL Sync Status
```
IF local EL is syncing:
    Skip this iteration (let EL finish syncing)
    RETURN
```

#### Step 2: Find and Import Next Safe Block
```
currentNum = localSafeHead.Number + 1

LOOP:
    remoteBlock = FetchBlockFromRemote(currentNum)

    IF remoteBlock NOT FOUND:
        // Remote doesn't have this block, walk back
        currentNum = currentNum - 1
        IF currentNum == 0:
            ERROR: "reached genesis without finding common ancestor"
        CONTINUE

    localParent = FetchLocalBlock(currentNum - 1)

    IF localParent NOT FOUND:
        ERROR: "local chain corrupted - missing block"

    IF remoteBlock.ParentHash == localParent.Hash:
        // Found common ancestor!
        payload = FetchPayloadFromRemote(currentNum)
        InsertUnsafePayload(payload, remoteBlock)
        PromoteSafe(remoteBlock, DUMMY_L1_ORIGIN)
        RETURN SUCCESS
    ELSE:
        // Hash mismatch - reorg detected, walk back
        currentNum = currentNum - 1
        IF currentNum == 0:
            ERROR: "reached genesis without finding common ancestor"
```

#### Step 3: Update Finalized Head
```
remoteFin = FetchFinalizedHeadFromRemote()

IF remoteFin.Number > localFinalized.Number:
    localFin = FetchLocalBlock(remoteFin.Number)

    IF localFin.Hash == remoteFin.Hash:
        PromoteFinalized(remoteFin)
    ELSE:
        LOG WARNING: "finalized hash mismatch - reorg detected"
        // Will resolve naturally as safe head progresses
```

### Key Invariants

1. **Local blocks always exist**: We should always have blocks 0 through `localSafeHead.Number`
2. **Safe ≥ Finalized**: Safe head is always ahead of or equal to finalized head
3. **One block per iteration**: At most one block is imported per sync loop iteration
4. **Parent hash validation**: We walk back both chains to find where they converge
5. **EL sync gate**: If EL is syncing, we wait before progressing safe/finalized

### Reorg Handling

**Scenario: Remote chain diverges from local chain**

```
Local:  ... → 1000 (hash A) → 1001 (hash B) → ...
Remote: ... → 1000 (hash A) → 1001 (hash C) → ...
```

**Resolution:**
1. Start at `currentNum = 1001` (localSafe + 1)
2. Fetch remote block 1001 (hash C)
3. Fetch local block 1000 (hash A)
4. Check: `remoteBlock.ParentHash == localBlock.Hash`?
   - If NO: Walk back to 1000
5. Fetch remote block 1000 (hash A)
6. Fetch local block 999
7. Check: `remoteBlock.ParentHash == localBlock.Hash`?
   - If YES: Import remote block 1000 (but it's already our safe head, so skip)
   - Next iteration will import 1001 with the new hash

The algorithm naturally handles reorgs by walking back until finding the common ancestor.

## Configuration

### CLI Flags

```bash
--rollup.lite-mode                    # Enable lite mode (boolean)
--rollup.lite-mode-rpc <url>          # Remote EL RPC endpoint (required if lite-mode enabled)
--rollup.lite-mode-poll-interval <d>  # Polling interval (default: 1s)
```

### Config Struct

```go
// op-node/rollup/driver/config.go
type Config struct {
    // ... existing fields ...

    // LiteModeEnabled disables derivation and sources safe/finalized from remote EL
    LiteModeEnabled bool `json:"lite_mode_enabled"`

    // LiteModeRPC is the remote execution client RPC endpoint
    LiteModeRPC string `json:"lite_mode_rpc"`

    // LiteModePollInterval is how often to poll the remote EL (default: 1s)
    LiteModePollInterval time.Duration `json:"lite_mode_poll_interval"`
}
```

## Implementation Details

### Component: LiteModeSync

**Location:** `op-node/rollup/driver/lite_mode.go`

```go
type LiteModeSync struct {
    log       log.Logger
    ctx       context.Context

    // Remote EL to poll for safe/finalized heads
    remoteEL L2Chain

    // Local EL to check existence and import blocks
    localEL L2Chain

    // Engine controller to update heads
    engine EngineController

    cfg *rollup.Config

    // Track last seen heads
    lastSafeHead      eth.L2BlockRef
    lastFinalizedHead eth.L2BlockRef

    // Polling interval
    pollInterval time.Duration
}
```

**Key Methods:**

- `Start()`: Launches the sync loop goroutine
- `syncStep()`: Main sync step (checks EL status, imports safe, updates finalized)
- `isELSyncing()`: Queries local EL `eth_syncing` status
- `findAndImportNextSafe()`: Walks back to find common ancestor and imports
- `insertAndPromoteBlock()`: Fetches payload, inserts as unsafe, promotes to safe

### Derivation Pipeline Changes

**Location:** `op-node/rollup/engine/engine_controller.go`

```go
func (d *EngineController) RequestPendingSafeUpdate(ctx context.Context) {
    if d.liteMode {
        return // Derivation disabled in lite mode
    }
    // ... existing derivation code ...
}
```

### Remote EL Interface

Uses existing `L2Chain` interface:

```go
type L2Chain interface {
    L2BlockRefByLabel(ctx context.Context, label eth.BlockLabel) (eth.L2BlockRef, error)
    L2BlockRefByNumber(ctx context.Context, num uint64) (eth.L2BlockRef, error)
    PayloadByNumber(ctx context.Context, number uint64) (*eth.ExecutionPayloadEnvelope, error)
}
```

## Edge Cases & Error Handling

### 1. Remote RPC Unavailable
- **Behavior**: Log warning, retry on next poll
- **Duration**: Retry indefinitely (no timeout)
- **Impact**: Safe/finalized progression pauses until RPC recovers

### 2. Local Block Missing (Should Never Happen)
- **Detection**: `L2BlockRefByNumber(currentNum - 1)` returns error
- **Behavior**: Return error, log critical message
- **Impact**: Indicates corrupted local state, requires investigation

### 3. Remote Block Doesn't Exist
- **Detection**: `L2BlockRefByNumber(currentNum)` returns NotFound
- **Behavior**: Walk back to find common ancestor
- **Impact**: Normal - indicates we're ahead or on different fork

### 4. Hash Mismatch (Reorg)
- **Detection**: `remoteBlock.ParentHash != localParent.Hash`
- **Behavior**: Walk back both chains to find common ancestor
- **Impact**: Normal - algorithm handles reorgs gracefully

### 5. EL Syncing
- **Detection**: `eth_syncing` returns sync status object
- **Behavior**: Skip sync iteration, let EL finish
- **Impact**: Safe/finalized progression pauses temporarily

### 6. Finalized Hash Mismatch
- **Detection**: Local block at finalized number has different hash than remote
- **Behavior**: Log warning, don't promote
- **Impact**: Will resolve as safe head progresses and overwrites the conflicting block

### 7. InsertUnsafePayload Fails
- **Detection**: Error returned from `InsertUnsafePayload()`
- **Behavior**: Log error, retry on next poll
- **Impact**: Block import retried until successful

## L1 Origin Handling

### The L1BlockRef Parameter

`PromoteSafe()` requires an `L1BlockRef` parameter indicating which L1 block the L2 block was derived from. In lite mode:

**Decision: Use Dummy/Zero L1BlockRef**

```go
lm.engine.PromoteSafe(ctx, blockRef, eth.L1BlockRef{})
```

**Rationale:**
- The L1 origin is only used by the Finalizer component for tracking L1→L2 derivation
- In lite mode (standard, non-interop), the Finalizer doesn't track derivation relationships
- The finalizer is effectively bypassed since we get finalized heads from remote RPC
- No other components require the L1 origin for safe blocks in lite mode

**Impact:**
- ✓ No functional impact on safe head progression
- ✓ Simplifies implementation
- ✗ Metrics/logs won't show L1 origin (acceptable tradeoff)
- ✗ If we ever query "which L1 block was this L2 block derived from" it will return zero (not used in lite mode)

## Security Considerations

### Trust Model

**Lite mode requires trusting the remote RPC endpoint:**
- ✓ Remote RPC can influence safe/finalized heads
- ✓ Node will follow whatever the remote RPC reports
- ✗ No independent verification of safe/finalized status
- ✗ No L1 data validation

**Suitable for:**
- Trusted infrastructure deployments
- Development/testing environments
- Lightweight RPC nodes trusting a sequencer

**NOT suitable for:**
- Trustless/permissionless verification
- Bridge validators
- Critical infrastructure requiring independent verification

### Attack Vectors

1. **Malicious Remote RPC**
   - Could provide incorrect safe/finalized heads
   - Mitigation: Use trusted RPC sources only

2. **Remote RPC Compromise**
   - Attacker gains control of remote RPC
   - Mitigation: Monitor for unexpected reorgs, use multiple sources

3. **Network MITM**
   - Attacker intercepts RPC calls
   - Mitigation: Use TLS/authenticated RPC endpoints

## Testing Strategy

### Test Infrastructure

**Primary Test:** `TestSyncTesterExtEL`
- **Location:** `op-acceptance-tests/tests/sync_tester/sync_tester_ext_el/`
- **Purpose:** Validates node syncing against external execution layer endpoints
- **Network:** OP Sepolia testnet
- **Requirements:** Tailscale connection with exit node `oplabs-tools-tailscale-tunnel`

### Running Tests

**Using the Test Runner Script:**

```bash
# Standard mode (derivation-based sync)
./op-acceptance-tests/tests/sync_tester/sync_tester_ext_el/run_test.sh

# Lite mode (RPC-based sync)
OP_NODE_LITE_MODE_RPC=https://ci-sepolia-l2.optimism.io \
  ./op-acceptance-tests/tests/sync_tester/sync_tester_ext_el/run_test.sh
```

**Manual Test Execution:**

```bash
cd op-acceptance-tests/tests/sync_tester/sync_tester_ext_el

# Ensure Tailscale is configured
tailscale set --exit-node=oplabs-tools-tailscale-tunnel

# Run the test
CIRCLECI_PARAMETERS_SYNC_TEST_OP_NODE_DISPATCH=true \
  TAILSCALE_NETWORKING=true \
  NETWORK_PRESET=op-sepolia \
  GOMAXPROCS=5 \
  OP_NODE_LITE_MODE_RPC=<remote-rpc-url> \
  go test -run '^TestSyncTesterExtEL$' -v -count=1
```

### Test Validation Checklist

When validating lite mode implementation, verify:

- [ ] **Test passes** with exit code 0
- [ ] **Target block reached** (unsafe head matches target)
- [ ] **Safe head progresses** independently from derivation
- [ ] **Finalized head progresses** independently from derivation
- [ ] **No derivation activity** in logs (pipeline should be idle)
- [ ] **Sync time comparable** to standard mode (or faster)
- [ ] **No errors** related to missing L1 origin data
- [ ] **Clean shutdown** with no panics or critical errors

### Expected Test Behavior

**Standard Mode (Baseline):**
- Derivation pipeline processes L1 data
- Safe/finalized heads progress via L1 finality signals
- Logs show "Derived attributes" messages
- Runtime: ~3-4 minutes for 20 blocks

**Lite Mode (Target):**
- No derivation pipeline activity
- Safe/finalized heads polled from remote RPC
- Logs show "Lite mode" or similar messages
- Runtime: Should be similar or faster

### Test Monitoring

The test can take 5-15 minutes depending on network conditions. Monitor progress by:

1. **Check sync status** in logs:
   ```
   grep "Chain sync status" test_run_*.log | tail -n 10
   ```

2. **Watch for completion**:
   ```
   grep -E "PASS|FAIL" test_run_*.log
   ```

3. **Check for errors**:
   ```
   grep -E "ERROR|FATAL" test_run_*.log
   ```

### Automated Test Agent

For long-running tests, use the general-purpose agent to:
- Run test in background
- Monitor progress every 60 seconds
- Report final results

**Agent Task Prompt:**
```
Run the sync test using run_test.sh script in background.
Monitor the log file every 60 seconds.
Report when test completes with PASS/FAIL status.
Include final block sync status in report.
```

### Unit Tests

In addition to acceptance tests, create unit tests for:

**LiteModeSync Component:**
```go
// op-node/rollup/driver/lite_mode_test.go
func TestLiteModeSync_FindCommonAncestor(t *testing.T) { /* ... */ }
func TestLiteModeSync_ImportBlock(t *testing.T) { /* ... */ }
func TestLiteModeSync_HandleReorg(t *testing.T) { /* ... */ }
func TestLiteModeSync_ELSyncingCheck(t *testing.T) { /* ... */ }
func TestLiteModeSync_FinalizedUpdate(t *testing.T) { /* ... */ }
```

**Mock Setup:**
- Mock `L2Chain` interface for remote/local EL
- Mock `EngineController` for head updates
- Test error conditions (RPC failures, missing blocks, reorgs)

### CI Integration

Tests should run in CI when:
- PRs are opened against base branch
- Changes are made to `op-node/rollup/driver/` or `op-node/rollup/engine/`
- Manual trigger for full test suite

**Environment Requirements:**
- Tailscale network access
- Access to CI endpoints (ci-sepolia-l2.optimism.io, etc.)
- Go 1.21+
- Sufficient timeout (15+ minutes)

---

**Document Status:** Design v1.0
**Last Updated:** 2025-10-01
**Branch:** feat/xxx-node
