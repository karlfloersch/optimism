# Safe Head Gossip Protocol Specification

## Overview

The Safe Head Gossip Protocol enables P2P propagation of safe L2 blocks between Optimism nodes, allowing follower nodes to bypass expensive derivation and receive safe heads directly via gossip. This specification defines the protocol structure, message formats, validation rules, and operational modes.

## Operational Modes

### Normal Mode (Default)
- **Flag**: `--mode=normal` or no mode specified
- **Behavior**: Standard op-node operation with full derivation pipeline
- **P2P Role**: Neither publishes nor consumes safe head gossip

### Prover Mode
- **Flag**: `--mode=prover`
- **Behavior**: Runs full derivation pipeline AND publishes safe heads to P2P network
- **P2P Role**: Publisher - signs and broadcasts safe heads when cross-safe head advances
- **Trigger**: Emits `GossipSafeHeadEvent` on `PromoteSafeEvent` in derivation pipeline

### Follower Mode
- **Flag**: `--mode=follower`
- **Behavior**: Disables derivation pipeline, receives safe heads exclusively via P2P
- **P2P Role**: Consumer - validates signatures and applies received safe heads
- **Derivation**: Uses `NoOpDerivationPipeline` stub

## Protocol Architecture

### P2P Topics

Safe heads are broadcast on version-specific topics similar to unsafe blocks:

```
/optimism/{chainId}/0/safe-heads  # safeHeadsV1 - Pre-Canyon
/optimism/{chainId}/1/safe-heads  # safeHeadsV2 - Canyon+
/optimism/{chainId}/2/safe-heads  # safeHeadsV3 - Ecotone+
/optimism/{chainId}/3/safe-heads  # safeHeadsV4 - Isthmus+
```

**Topic Selection Logic**:
```go
if cfg.IsIsthmus(timestamp) {
    return safeHeadsV4
} else if cfg.IsEcotone(timestamp) {
    return safeHeadsV3
} else if cfg.IsCanyon(timestamp) {
    return safeHeadsV2
} else {
    return safeHeadsV1
}
```

### Message Format

Safe head messages follow the same structure as unsafe block messages:

```
[65 bytes signature][payload bytes]
```

**Signature**: 65-byte ECDSA signature using existing P2P sequencer key infrastructure
**Payload**: Snappy-compressed SSZ-encoded `ExecutionPayloadEnvelope`

**Validation Steps**:
1. Snappy decompression validation
2. Signature verification against configured P2P sequencer address
3. SSZ decoding of `ExecutionPayloadEnvelope`
4. Block hash verification
5. Timestamp bounds checking (not more than 60s old, not more than 5s future)
6. Fork-specific validation (withdrawals, blob properties, parent beacon block root)
7. Duplicate detection via LRU cache (up to 5 blocks per height)

## Core Components

### SafeHeadGossipPublisher (`op-node/p2p/safe_head_gossip.go`)

**Purpose**: Handles safe head publishing in prover mode
**Event Subscription**: `GossipSafeHeadEvent`
**Key Methods**:
- `publishSafeHead(ctx, envelope, signer)`: Signs and publishes safe head to P2P network
- Topic selection based on block timestamp and fork configuration

### FollowerModeDeriver (`op-node/rollup/driver/follower.go`)

**Purpose**: Processes received safe heads in follower mode
**Event Subscription**: `ReceivedSafeHeadEvent`
**Key Methods**:
- `applySafeHead(ctx, ref)`: Validates and applies received safe head to engine
- `validateSafeHeadWithReorg(current, received)`: Basic reorg detection
- `handleSafeHeadReorg(ctx, newRef)`: Simple reorg recovery

**State Management**:
- Initializes execution engine finalized head to genesis
- Updates unsafe head to match safe head when safe head advances
- Emits `TryUpdateEngineEvent` to sync forkchoice state with execution engine

### SafeHeadsHandler (`op-node/p2p/gossip.go`)

**Purpose**: P2P message validation and routing
**Validation**: Implements `BuildSafeHeadsValidator` with same security model as unsafe blocks
**LRU Cache**: Tracks seen blocks (1000 block heights, 5 blocks per height max)

## Critical Implementation Details

### Execution Engine Synchronization

**Problem Solved**: Follower nodes experienced forkchoice initialization errors because finalized head was never set.

**Solution**:
```go
// Initialize finalized head to genesis when processing first safe head
if engine.Finalized() == (eth.L2BlockRef{}) {
    genesisRef := eth.L2BlockRef{
        Hash:           cfg.Genesis.L2.Hash,
        Number:         cfg.Genesis.L2.Number,
        ParentHash:     common.Hash{},
        Time:           0,
        L1Origin:       cfg.Genesis.L1,
        SequenceNumber: 0,
    }
    engine.SetFinalizedHead(genesisRef)
}

// Update unsafe head to match safe head for proper P2P unsafe sync
if engine.UnsafeL2Head().Number < safeHead.Number {
    engine.SetUnsafeHead(safeHead)
}

// Trigger forkchoice update to sync with execution engine
emitter.Emit(ctx, engine.TryUpdateEngineEvent{})
```

**Result**: Enables proper unsafe P2P sync alongside safe head gossip

### Event Flow

**Prover Mode**:
1. Derivation pipeline advances cross-safe head
2. `PromoteSafeEvent` emitted in `engine/events.go`
3. `GossipSafeHeadEvent` emitted with `ExecutionPayloadEnvelope`
4. `SafeHeadGossipPublisher` signs and publishes to P2P network

**Follower Mode**:
1. P2P safe head message received and validated
2. `ReceivedSafeHeadEvent` emitted by `BlockReceiver`
3. `FollowerModeDeriver` processes event via `applySafeHead`
4. Engine controller updated, forkchoice synchronized with execution engine

## Current Limitations vs Unsafe Block Propagation

### ⚠️ Missing: Advanced Gap Filling

**Current Implementation**:
- Safe heads expected to arrive sequentially via gossip
- No buffering of out-of-order blocks
- Passive waiting if blocks are missing

**Unsafe Block Implementation** (for reference):
- `SyncClient` with LRU quarantine for out-of-order blocks
- `RequestL2Range(start, end)` for active gap filling
- `PayloadByNumberProtocolID` request/response protocol
- Trust-based promotion from quarantine to main chain

**Missing Components**:
```go
// NOT IMPLEMENTED for safe heads:
type SafeHeadSyncClient struct {
    quarantine *simplelru.LRU[common.Hash, safeHeadResult]
    quarantineByNum map[uint64]common.Hash
    trusted *simplelru.LRU[common.Hash, struct{}]
    // ... active P2P request mechanisms
}

func (c *SafeHeadSyncClient) RequestSafeHeadRange(start, end uint64) error {
    // NOT IMPLEMENTED: Active requesting of missing safe heads
}
```

### ⚠️ Missing: Advanced Reorg Handling

**Current Implementation**:
- Basic same-height conflict detection in `validateSafeHeadWithReorg`
- Simple rollback using `backupSafeHead`
- Single-step reorg recovery

**Unsafe Block Implementation** (for reference):
- Trust propagation (parent blocks become trusted when child is promoted)
- Multiple candidate handling (up to 5 competing blocks per height)
- Chain reconstruction after deep reorgs
- Conflict resolution and chain rebuilding

**Missing Components**:
```go
// NOT IMPLEMENTED for safe heads:
func (f *FollowerModeDeriver) handleDeepSafeHeadReorg(ctx context.Context, conflictDepth int) {
    // Complex reorg recovery spanning multiple blocks
    // Chain reconstruction similar to unsafe block SyncClient
}

func (f *FollowerModeDeriver) promoteQuarantinedSafeHeads(trustedHash common.Hash) {
    // Trust-based promotion of buffered safe heads
    // Parent-hash verification chain
}
```

## Security Model

### Signature Requirements
- All safe head messages MUST be signed with valid P2P sequencer key
- Signature verification uses `opsigner.SignedP2PBlock.VerifySignature`
- Same trust model as unsafe block gossip

### Validation Rules
- Block hash MUST match computed hash of execution payload
- Timestamp MUST be within acceptable bounds (±60s/+5s)
- Fork-specific properties MUST be valid for block timestamp
- Maximum 5 different blocks allowed per block height

### Failure Handling
- Invalid signatures → `ValidationReject` (peer downscored)
- Malformed messages → `ValidationReject`
- Duplicate messages → `ValidationIgnore`
- Timestamp violations → `ValidationReject`

## Testing Requirements

### Core Functionality Tests
```go
func TestSafeHeadSync()                    // Basic gossip flow
func TestExecutionEngineStateConsistency() // Engine synchronization
func TestUnsafeHeadProgression()          // Unsafe sync compatibility
```

### Missing Test Coverage (Planned)
```go
func TestSafeHeadGapFilling()     // Out-of-order block handling
func TestSafeHeadDeepReorg()      // Multi-block reorg scenarios
func TestSafeHeadQuarantine()     // Block buffering and promotion
func TestSafeHeadActiveRequest()  // P2P request/response for gaps
```

## Backwards Compatibility

- **Default behavior unchanged**: Nodes continue normal operation unless explicitly configured
- **Existing P2P infrastructure reused**: Same validation, signing, and topic management
- **No impact on non-follower nodes**: Safe head gossip ignored unless in follower mode
- **Graceful degradation**: Follower nodes can fall back to normal operation (not implemented)

## Performance Characteristics

### Network Impact
- **Additional P2P traffic**: Safe head messages ~1KB per L2 block finalization
- **Topic overhead**: 4 additional topics per chain (version-specific)
- **Validation cost**: Comparable to unsafe block validation

### Node Performance
- **Prover nodes**: Minimal overhead (~1ms per safe head publish)
- **Follower nodes**: Significant CPU savings (no derivation pipeline)
- **Memory usage**: Minimal additional state tracking

## Future Enhancements

### Priority 1: Gap Filling Implementation
```go
type SafeHeadSyncClient struct {
    // Quarantine system for out-of-order safe heads
    // Active P2P request/response protocol
    // Trust-based promotion mechanism
}
```

### Priority 2: Advanced Reorg Support
```go
func (f *FollowerModeDeriver) handleComplexReorg(reorgDepth int) {
    // Multi-block reorg recovery
    // Chain reconstruction
    // Conflict resolution between multiple safe head sources
}
```

### Priority 3: Production Readiness
- Real metrics implementation (replace no-op metrics)
- Comprehensive monitoring and alerting
- Performance optimization and bandwidth management
- Modular prover support (configurable signature acceptance)

## Implementation Status

### ✅ Fully Implemented
- P2P gossip infrastructure
- Signature validation and security model
- Basic follower mode operation
- Execution engine synchronization
- Unsafe P2P compatibility

### 🔄 Partially Implemented
- Basic reorg handling (same-height conflicts only)
- Test infrastructure (core functionality covered)

### 📋 Not Implemented
- Advanced gap filling with quarantine system
- Deep reorg recovery and chain reconstruction
- Active P2P request/response for missing blocks
- Production metrics and monitoring
- Comprehensive reorg testing scenarios

---

This specification serves as the authoritative reference for implementing and extending the Safe Head Gossip Protocol within the Optimism ecosystem.
