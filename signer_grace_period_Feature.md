# Feature: Unsafe Block Signer Grace Period

**Author:** AI Tool + Developer  
**Issue:** https://github.com/ethereum-optimism/optimism/issues/19981  
**Started:** 2026-04-13  

## Description

When the unsafe block signer address is rotated on L1, verifier nodes experience a stale signer window (up to ~11 minutes with defaults) during which they reject valid blocks from the new signer. This feature adds a **grace period** so that when `RuntimeConfig.Load` detects a signer change, blocks from the **previous** signer continue to be accepted for a configurable duration (default: 20 minutes), or until the **new** signer is observed in-use on gossip.

This is a verifier-side behavior change only. No additional CLI config is required. The unsafe signer key rotation logic is not consensus code.

## Work Breakdown (Subfeatures by commit)

### Subfeature 1: Extend GossipRuntimeConfig and RuntimeConfig for dual-signer tracking

**Files:**
- `op-node/p2p/gossip.go` — add `PreviousP2PSequencerAddress()` and `ConfirmCurrentSigner()` to `GossipRuntimeConfig`
- `op-node/node/runcfg/runtime_config.go` — add previous signer tracking, grace period expiry, and confirmation logic
- `op-service/testutils/runtime_config.go` — update mock to satisfy new interface

### Subfeature 2: Update gossip block signature verification

**Files:**
- `op-node/p2p/gossip.go` — update `verifyBlockSignature` to try previous signer during grace period
- `op-node/p2p/gossip_test.go` — add tests for grace-period acceptance, expiry, and confirmation

---

## Diary

### Prompt 1: Initial context gathering
**Received:** Instructions to fix issue #19981 using a dual-signer grace period approach.  
**Actions:** Read AGENTS.md, go-dev.md, overprompt.md. Explored RuntimeConfig, gossip validation, and block auth code across 6 files. Mapped the full flow from L1 storage → RuntimeConfig.Load → P2PSequencerAddress → verifyBlockSignature.

### Prompt 2: Plan approval
**Received:** Developer approved the plan.  
**Actions:** Presented spec with interface changes, RuntimeConfig state tracking, verifyBlockSignature dual-check, and mock updates. Developer approved.

### Prompt 3: Test development
**Received:** Developer asked to begin with tests.  
**Actions:**
- Extended `GossipRuntimeConfig` interface with `PreviousP2PSequencerAddress()` and `ConfirmCurrentSigner()`
- Updated `MockRuntimeConfig` in testutils
- Added stub methods to `RuntimeConfig` (compile-time interface satisfaction)
- Added 5 new subtests to `TestVerifyBlockSignature` in gossip_test.go
- Created `runtime_config_test.go` with 6 tests covering grace period lifecycle
- Verified: 3 gossip tests fail (expected), RuntimeConfig tests won't compile (expected)

### Prompt 4: Implementation
**Received:** Developer approved to proceed with production code.  
**Actions:**
- Added `DefaultSignerGracePeriod = 20 * time.Minute` constant
- Added `prevP2PBlockSignerAddr` and `signerChangeTime` to `runtimeConfigData`
- Implemented `PreviousP2PSequencerAddress()` with time-based expiry
- Implemented `ConfirmCurrentSigner()` to clear previous signer
- Updated `Load()` to detect signer changes and start grace periods
- Updated `verifyBlockSignature()` to try current signer first, then previous during grace
- All 15 tests pass, full op-node/... builds clean, no lint errors
