# Super Dispute Game Factory Integration Plan

## Overview

This document outlines the plan for implementing a two-factory architecture for DelegatedDisputeGame, where:
1. A **Super Dispute Game Factory** at the superchain level creates `SuperFaultDisputeGame` instances
2. Each chain has its own **DisputeGameFactory** for `DelegatedDisputeGame` instances
3. Invalidation of SuperGames automatically propagates to DelegatedDisputeGames

---

## Final Architecture

### Key Insight: Just Deploy a Normal SystemConfig

Both superchain and per-chain levels use the **same AnchorStateRegistry contract** initialized with a **normal SystemConfig**. For the superchain level, we simply deploy a SystemConfig with zero/minimal values - no special interface or contract changes needed.

This matches the existing pattern used by SuperFaultDisputeGame tests, which already deploy a real SystemConfig and point the AnchorStateRegistry at it.

### Visual Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│  SUPERCHAIN LEVEL                                               │
│                                                                 │
│  SystemConfig (with zero/minimal values)                        │
│  ├── Points to SuperchainConfig for paused()/guardian()        │
│  └── Standard SystemConfig, no modifications needed             │
│                                                                 │
│  AnchorStateRegistry                                            │
│  ├── initialized with SystemConfig (standard)                  │
│  ├── blacklist/retirement for SuperGames                       │
│  └── respectedGameType for SuperGames                          │
│                                                                 │
│  DisputeGameFactory (SuperDisputeGameFactory)                   │
│  ├── creates SuperFaultDisputeGame                             │
│  └── creates SuperPermissionedDisputeGame                      │
└─────────────────────────────────────────────────────────────────┘
                         │
                         │ isGameProper() checks SuperGame validity
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│  PER-CHAIN LEVEL                                                │
│                                                                 │
│  SystemConfig (per-chain, standard)                             │
│                                                                 │
│  AnchorStateRegistry                                            │
│  ├── initialized with SystemConfig                              │
│  ├── blacklist/retirement for per-chain games                  │
│  ├── isGameProper() checks SuperGame validity  ← IMPLEMENTED   │
│  └── anchor state for this chain                               │
│                                                                 │
│  DisputeGameFactory                                             │
│  └── creates DelegatedDisputeGame                              │
│                                                                 │
│  DelegatedDisputeGame                                           │
│  ├── delegates status()/resolvedAt() to SuperGame              │
│  └── NO registry mismatch check (registries are different)     │
└─────────────────────────────────────────────────────────────────┘
```

---

## Implementation Status

### Completed

#### 1. SuperGame Validity Check in AnchorStateRegistry

**File:** `src/dispute/AnchorStateRegistry.sol`

Added try/catch in `isGameProper()` to check SuperGame validity:

```solidity
// For DelegatedDisputeGames, also check SuperGame validity.
// If the SuperGame is blacklisted or retired, the DelegatedDisputeGame is also invalid.
try IDelegatedDisputeGame(address(_game)).superGame() returns (ISuperFaultDisputeGame superGame) {
    IAnchorStateRegistry superRegistry = superGame.anchorStateRegistry();

    // SuperGame must not be blacklisted.
    if (superRegistry.isGameBlacklisted(IDisputeGame(address(superGame)))) {
        return false;
    }

    // SuperGame must not be retired.
    if (superRegistry.isGameRetired(IDisputeGame(address(superGame)))) {
        return false;
    }
} catch {
    // Not a DelegatedDisputeGame, that's fine.
}
```

#### 2. Created IDelegatedDisputeGame Interface

**File:** `interfaces/dispute/IDelegatedDisputeGame.sol`

Minimal interface for SuperGame detection:

```solidity
interface IDelegatedDisputeGame is IDisputeGame {
    function superGame() external view returns (ISuperFaultDisputeGame superGame_);
    function chainId() external view returns (uint256 chainId_);
    function anchorStateRegistry() external view returns (IAnchorStateRegistry registry_);
}
```

#### 3. Updated DelegatedDisputeGame

**File:** `src/dispute/DelegatedDisputeGame.sol`

Removed the registry mismatch check since registries will be different (per-chain vs superchain):

```solidity
// Note: We intentionally do NOT check that the SuperGame uses the same AnchorStateRegistry.
// The DelegatedDisputeGame may use a per-chain AnchorStateRegistry while the SuperGame uses
// a superchain-level AnchorStateRegistry. Invalidation propagates through isGameProper()
// which checks the SuperGame's registry for blacklist/retirement status.
```

### Remaining (Deployment)

Deploy superchain-level infrastructure:
1. Deploy a SystemConfig with zero/minimal values for superchain use
2. Deploy AnchorStateRegistry initialized with that SystemConfig
3. Deploy DisputeGameFactory (SuperDisputeGameFactory)
4. Register SuperFaultDisputeGame implementation

---

## Invalidation Propagation

### How It Works

When Guardian blacklists or retires a SuperGame in the superchain AnchorStateRegistry:

1. SuperGame is marked as blacklisted/retired in superchain AnchorStateRegistry
2. Per-chain AnchorStateRegistry.isGameProper(delegatedGame) is called
3. The try/catch detects it's a DelegatedDisputeGame
4. It queries the SuperGame's registry (superchain AnchorStateRegistry)
5. If SuperGame is blacklisted/retired, returns false
6. Portal's withdrawal validation fails

### What Gets Checked

| Check | Where | Scope |
|-------|-------|-------|
| DelegatedGame blacklisted | Per-chain registry | Single game |
| DelegatedGame retired | Per-chain registry | Games before timestamp |
| SuperGame blacklisted | Superchain registry | Propagates to all DelegatedGames |
| SuperGame retired | Superchain registry | Propagates to all DelegatedGames |
| Global pause | SuperchainConfig | All games everywhere |

---

## Files Modified

| File | Change Type | Description |
|------|-------------|-------------|
| `interfaces/dispute/IDelegatedDisputeGame.sol` | **NEW** | Minimal interface for SuperGame detection |
| `src/dispute/AnchorStateRegistry.sol` | Modified | Added SuperGame validity check in isGameProper() |
| `src/dispute/DelegatedDisputeGame.sol` | Modified | Removed registry mismatch check |

---

## Testing Strategy

### Unit Tests

1. isGameProper() correctly detects DelegatedDisputeGame and checks SuperGame
2. Non-DelegatedDisputeGame passes through isGameProper() normally

### Integration Tests

1. SuperGame blacklist propagates to DelegatedDisputeGames
2. SuperGame retirement propagates to DelegatedDisputeGames
3. Per-chain blacklist works independently of SuperGame status
4. Full withdrawal flow with new architecture

---

## Summary

The key insight is that we don't need any special interface changes. The superchain level just deploys a normal SystemConfig (with zero/minimal values) and a standard AnchorStateRegistry. The only code change needed was adding the try/catch in `isGameProper()` to propagate SuperGame invalidation to DelegatedDisputeGames.
