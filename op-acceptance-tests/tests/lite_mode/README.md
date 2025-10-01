# Lite Mode Acceptance Tests

This directory contains acceptance tests for the lite mode feature in op-node.

## What is Lite Mode?

Lite mode is a new sync mode where the op-node sources safe/finalized heads from a remote RPC endpoint instead of deriving them from L1. It's designed for resource-constrained nodes that want to sync quickly without performing L1 derivation.

### Key Characteristics

- **Disables L1 derivation pipeline** for safe head progression
- **Polls a remote RPC endpoint** for safe/finalized blocks
- **Imports blocks from remote** and promotes them locally
- **CL sync (P2P gossip)** still handles unsafe blocks
- **Lower resource requirements** compared to full derivation

## Test Structure

### Test Files

- `init_test.go` - Test setup and configuration
- `lite_mode_test.go` - Test cases for lite mode functionality

### Test Cases

1. **TestLiteModeBasicSync** - Verifies basic safe head synchronization
   - Tests that lite mode verifier syncs safe heads from sequencer
   - Ensures safe head progression matches the sequencer

2. **TestLiteModeFinalizedSync** - Verifies finalized head synchronization
   - Tests that lite mode verifier syncs finalized heads from sequencer
   - Ensures finalized head progression matches the sequencer

3. **TestLiteModeUnsafeViaP2P** - Verifies P2P gossip still works
   - Tests that unsafe blocks are still received via P2P
   - Ensures CL sync remains functional in lite mode

4. **TestLiteModeContinuousSync** - Verifies continuous operation
   - Tests that lite mode continues to sync over multiple rounds
   - Ensures the verifier stays in sync with the sequencer

## Implementation Details

### Configuration

The lite mode tests use a custom system preset that:

1. Creates a standard single-chain multi-node setup (sequencer + verifier)
2. Configures the verifier to run in lite mode
3. Sets the verifier's remote RPC to the sequencer's RPC endpoint
4. Maintains P2P connections for unsafe block sync

### Code Organization

#### Preset Layer (`op-devstack/presets/`)

- **`lite_mode.go`** - Defines the `LiteMode` preset and `NewLiteMode()` constructor
- **`cl_config.go`** - Contains `WithLiteMode()` option for configuring lite mode

#### System Layer (`op-devstack/sysgo/`)

- **`system_lite_mode.go`** - Defines `LiteModeSystem()` that creates the test infrastructure
- **`l2_cl.go`** - Extended `L2CLConfig` with `LiteModeEnabled` and `LiteModeRemoteRPC` fields
- **`l2_cl_opnode.go`** - Modified to use lite mode config from `L2CLConfig`

### How It Works

1. **System Creation**: `LiteModeSystem()` creates a minimal system with sequencer
2. **Dynamic Configuration**: After sequencer is created, retrieves its RPC URL
3. **Verifier Setup**: Creates verifier CL node with lite mode enabled, pointing to sequencer RPC
4. **P2P Setup**: Connects nodes via P2P for unsafe block gossip
5. **Test Execution**: Tests verify sync behavior across different safety levels

## Running the Tests

```bash
# Run all lite mode tests
go test ./op-acceptance-tests/tests/lite_mode/...

# Run a specific test
go test ./op-acceptance-tests/tests/lite_mode/ -run TestLiteModeBasicSync

# Run with verbose output
go test ./op-acceptance-tests/tests/lite_mode/... -v
```

## Design Decisions

### Why AfterDeploy Hook?

The system uses `stack.AfterDeploy()` to configure the lite mode verifier because:
- The sequencer must be created first to get its RPC endpoint
- The verifier needs the sequencer's RPC URL for lite mode configuration
- This ensures proper ordering of node creation and configuration

### Why Keep P2P Connections?

Even in lite mode, P2P connections are maintained because:
- Unsafe blocks are still received via P2P gossip
- This ensures the node can participate in consensus layer sync
- It provides a more complete syncing experience

### Configuration Approach

The implementation supports two configuration methods:
1. **Programmatic**: Via `L2CLConfig` fields (preferred for tests)
2. **Environment Variables**: Via `OP_NODE_ROLLUP_LITE_MODE*` (backward compatible)

This dual approach ensures:
- Clean, testable code with explicit configuration
- Backward compatibility with existing environment-based setups

## Future Enhancements

Potential improvements to these tests:

1. **Reorg Testing** - Verify behavior during chain reorganizations
2. **Connection Failure** - Test recovery when remote RPC is unavailable
3. **Performance Metrics** - Measure sync speed vs. full derivation
4. **Multiple Verifiers** - Test multiple lite mode nodes syncing from same source
5. **Mixed Mode** - Test systems with both lite and full derivation verifiers

## Related Files

- `/root/optimism-2/op-node/rollup/driver/lite_mode.go` - Core lite mode implementation
- `/root/optimism-2/op-node/rollup/driver/driver.go` - Integration with driver
- `/root/optimism-2/op-node/rollup/sync/config.go` - Sync configuration
