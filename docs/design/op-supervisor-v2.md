# Supervisor v2 Design Document

Note: I am writing this as a draft and may contain confusing or incomplete information. Please ask questions and then correct / clarify the information on this page before proceeding with implementation.

## Goal
Refactor the supervisor so that it does not require significant modifications to the op-node. Pre-interop hard fork op-node API should be sufficient for integrating this Supervisor v2 and thereby enabling interop.

## Architecture
supervisor-v2 embeds an op-node (managed mode) for each chain in the dependency set (note: these op-nodes DO NOT use the interop hardfork; they should be pectra op-nodes, and no dependency-set information is sent to the op-node).

- Managed/embedded op-node: supervisor constructs and runs the op-node in-process using op-node libraries.
- RPC exposure:
  - Devstack/tests: dial the embedded op-node user RPC directly (typed client).
  - CLI: exposes a reverse-proxy under `/opnode/` on the supervisor HTTP server (default on) for single-port UX.
  - Important: the `/opnode/` reverse proxy is currently broken; until it is fixed, consumers should dial the embedded op-node user RPC directly. Devstack/sysgo paths already do this.

supervisor-v2 puts all of the safe blocks which are being synced by the op-nodes into the `local-safe` db. The cross-safe validation is then run on the latest `local-safe` block at the same block height (just like the normal op-supervisor). The `cross-safe` block is calculated. This block may either be a) valid; or b) invalid.

If valid, then the op-supervisor just continues. If invalid, then the supervisor-v2 will:
1. Stop the op-node for each of the chains effected
2. Roll back the execution layer to the block **before** the first invalid block
3. Add the payloadId which was **invalid** to the `payloadId denylist` (maybe blockhash denylist?). This payloadId denylist is a list of payloadId which are INVALID and therefore should trigger an error in derivation. This is as if that block contains an invalid transaction - same type of thing. It will trigger the 'steady derivation' logic and discard the batch if the op-node were to sync it.
4. Rollback the execution client's safe and unsafe heads to the L1 block before the invalid block detected by the cross-safe handling logic
5. Restart the op-node with a new `safe db` (so that we make sure we wipe the safe db as well)
6. When the op-node derives the block which was detected as INVALID, it will look up whether that payloadId is in the blocklist for that blockheight (this is new behavior). It will see that indeed the payloadId IS in the denylist and trigger the error handling logic for a malformed block (using steady derivation)

This means that the op-node will start syncing again but this time it will skip over the block which is invalid based on the cross-safety checks. This is all that is required to support interop and doesn't require significant changes to the op-node!


Clarifications
- All cross-safety and local-safety persistence lives solely inside supervisor-v2. The op-node stores none of this state.
- Minimal, backwards-compatible op-node change: add an optional denylist check in the derivation loop. If no entries are present or the feature is disabled, behavior is unchanged.
- Denylist key: engine/execution PayloadID deterministically computed from the payload fields (i.e., everything except the post-state). Denylist is per-chain-id, persisted, and does not expire unless explicitly pruned.
- Rollback target: the L2 block immediately before the first cross-invalid block. Order: add denylisted payload; stop op-node; roll back EL heads; clear op-node safe DB; restart op-node.
- Use L1 confirmation depth (default 40, configurable) for determining which local-safe blocks are eligible for cross-safety checks, to avoid reorging cross-safety itself.


Implementation plan:

Note: Please work with me to come up with implementation plans for each milestone. Add a checklist of things that we need to do for each of these milestones. Also ensure that at all times:
1. We don't break existing tests
2. We unit test all of our work
3. We integration test / e2e test all of our work

Please commit changes after each milestone (can do even more frequently but at minimum after milestones) and ensure that before committing all tests pass.

### 1. Testing Setup

Enable fast testing feedback by integrating op-supervisor-v2 with devstack and sysgo immediately. It should be possible to:

1. Run op-supervisor-v2 in the devstack with some basic tests
2. Run op-supervisor-v2 as a standalone system with sysgo

Both of these services would of course be hooked up with an execution client (geth).

Implementation plan
- [x] Create `op-supervisor-v2` process that manages an embedded op-node and a single L2 geth (EL) process/datadir.
- [x] Ingestion skeleton: poll op-node Rollup status for current heads and fetch corresponding L2 blocks/receipts (logging-only for now).
- [ ] Persist into supervisor-v2 DBs reusing existing schemas (`events`, `fromda` local-safe, `fromda` cross-safe).
- [x] Provide a minimal HTTP health/status endpoint with per-chain heads.
- [x] Bring up as a standalone sysgo scenario; add `scripts/sysgo-smoke.sh` to send a tx with cast and confirm receipt.
- [x] Add a devstack preset and tests without affecting existing suites.

Current status (M1):
- Managed mode implemented: supervisor embeds op-node and polls heads; CLI supports managed-only and reverse-proxies op-node RPC at `/opnode/` (default on).
- Devstack: dedicated preset (no external CL). Supervisor v2 hydrates a typed CL frontend to the embedded op-node; smoke test passes and asserts L2 advancement.
- Sysgo: `scripts/sysgo-smoke.sh` passes (sequencing and receipt confirmed).


### 2. Introduce op-node rollbacks triggered by op-supervisor-v2

Show in a test that it is possible to trigger the rollback logic in the op-supervisor-v2 using a devstack test

Implementation plan
- [x] Define rollback API within supervisor-v2: dev-only `POST /admin/rollback { to_block_number }`; stop embedded op-node; roll back EL via `debug_setHead` (fallback path); restart a fresh embedded op-node with empty safe DB; resume polling.
- [x] System test: sysgo-based test validates mechanics — head regresses immediately after rollback and re-advances back to at least the pre-rollback height (no denylist yet, so it returns to the same tip).

Current status (M2):
- Admin rollback endpoint implemented in `op-supervisor-v2` and managed op-node lifecycle supports replace-and-restart.
- Sysgo preset test `TestSupervisorV2Rollback` added; asserts rollback regression below pre-height and recovery back to at least the pre-height. The test now also validates that the replaced block at height H has a different hash, and the parent at H-1 remains unchanged (chain continuity up to H-1).
- Rollback implementation is abstracted via `rollbackEL(...)` (currently backed by `debug_setHead`). TODO: replace with Engine API `engine_forkchoiceUpdated` against the authenticated EL RPC for broader EL compatibility (e.g., reth), without changing finalized.
- Rollback API is now absolute-only (`to_block_number`). In multi-chain mode, the endpoint requires `?chainId=` to scope the operation.

### 3. Add denylist

Create a payloadId (or blockhash) denylist without introducing interop logic yet. Tests explicitly add denylist entries to exercise rollback and re-sync behavior.

We should show this working both in the devstack tests as well as in the sysgo system that we spin up

Implementation plan
- [x] Implement persisted per-chain denylist in supervisor-v2 keyed by `PayloadID` (deterministic header-hash of payload).
- [x] Minimal op-node change: optional denylist check via `SV2_DENYLIST_URL` to query supervisor before inserting a payload into EL; on deny, follow existing invalid/malformed path and discard.
- [x] Tests in sysgo preset to validate denylist hit → rollback → re-sync behavior (block at height H is replaced; parent at H-1 unchanged).

Notes
- Removed the seeded 1-in-N denylist policy. Entries are now managed explicitly by tests or future supervisor policies (no auto-seeding).

Current status (M3):
- `DenylistStore` implemented with in-memory map and optional JSON persistence; supervisor exposes `GET /denylist/v1/check?chainId=&id=`.
- Op-node integration added: when `SV2_DENYLIST_URL` is set, the op-node queries the supervisor denylist prior to `engine_newPayload*` acceptance.
- System test combines rollback and denylist: computes the pre-rollback payload header-hash at height H, asserts it’s denylisted, triggers rollback, then asserts: (a) head regressed then re-advanced; (b) block hash at H changed; (c) parent hash at H-1 is unchanged.

### 4. Create a new hardfork (interop2) which deploys the pre-deploys

Because we are NOT using the interop hardfork (it introduces too much complexity into the op-node), we will still need to deploy the pre-deploys. For this we will introduce a new hardfork which is interop2 that deploys the same predeploys as the normal interop system. This can be done with a `if interop OR interop2` in the pre-deploy setup bit.

Implementation plan
- [x] Add `interop2_time` to `op-node` rollup config and helpers: `IsInterop2(...)`, `IsInterop2ActivationBlock(...)`.
- [x] At `interop2_time`, inject the same predeploy upgrade txs as interop.
- [x] At `interop2_time`, also activate the cross-L2 inbox txs (unconditionally, no dep-set requirement).
- [x] Sequencer: treat interop2 activation as no-txpool (same as interop).
- [x] Add CLI override `--override.interop2` to set `interop2_time` at runtime for tests/dev.
- [x] Use interop2 by default in SV2 sysgo presets via a small offset after genesis.

Current status (M4):
- `interop2_time` supported end-to-end with activation helpers.
- Predeploys + cross-L2 inbox txs injected at `interop2_time`.
- Sequencer no-txpool behavior extended to interop2 activation.
- CLI override `--override.interop2` wired.
- SV2: rollback/denylist test still green; added sysgo test scaffold to check predeploy code exists at activation (pre/post code assertion under iteration).

### 5. Add a second op-node and second execution client

We've got the core logic working for one chain, to make this interesting we want to integrate the cross-safe package and that works best with two chains.

Add this new two chain setup to the devstack and create a simple test making it work. Also ensure it's added to our sysgo setup.

Implementation plan
- [ ] New devstack preset (e.g., `interop2_two_chain`) with two minimal L2s, each with its own op-node and L2 geth managed by supervisor-v2.
- [x] Supervisor-v2 manages multiple chains in a single process via per-chain handles (one embedded op-node + poller per chain) and exposes chain-scoped HTTP.
- [x] Sysgo option to start SV2 for all L2 ELs and register each chain (`WithSupervisorV2OnAllChains`).
- [x] Minimal two-chain system option (no CL) to avoid interop coupling; used by tests.
- [x] Basic tests: two chains independently advance; rollback on chain A does not affect chain B.

Current status (M5):
- Multi-chain supervisor scaffolding implemented:
  - `chains` map with per-chain `chainHandle` (embedded op-node, restart config, poll cancel, started timestamp).
  - Chain-scoped HTTP: `/status?chainId=`, `POST /admin/rollback?chainId=` (absolute-only body), and `/opnode/{chainId}/` reverse proxy.
- Sysgo integration:
  - `WithSupervisorV2OnAllChains` starts one supervisor instance and calls `AddChain(...)` for each L2 EL.
  - `DefaultTwoMinimalSystemNoCL` brings up L1 and two L2 ELs without CL/process overhead for fast tests.
- Tests:
  - `TestSupervisorV2TwoChainAdvance`: both chains reach ≥ N blocks (no rollback).
  - `TestSupervisorV2TwoChainRollbackIsolation`: rolling back chain A regresses then re-advances; chain B is unaffected.
  - Single-chain rollback test remains and continues to validate absolute rollback on a single-chain setup (no `chainId` required).
- Usage:
  - Both two-chain tests are wired to the new preset `WithSV2TwoChainMinimal(6)` to exercise the preset end-to-end.
- Devstack two-chain preset added: `WithSV2TwoChainMinimal(offset)` builds a minimal two-chain setup without CLs, wires SV2 across both via `WithSupervisorV2OnAllChains`, and gates `L2NetworkCount(2)`. Tests may depend on this or the sysgo option interchangeably.

### 6. Integrate cross safe

This is where we integrate cross safe by populating the cross-db with all of our block data / event data. We will want to test this similarly to how interop is tested currently - creating valid and invalid executing messages which are validated or trigger reorgs.

Implementation plan
- [ ] Populate `events` DB from L2 geth logs/receipts and `fromda` local-safe DB from op-node local-safe mapping; compute cross-safe as in existing supervisor.
- [ ] Apply L1 confirmation depth gating for cross-safe inputs.
- [ ] Reuse existing cross-safety rulesets and tests; add cases: (a) valid executing message passes cross-safety, (b) invalid dependency fails and triggers reorg.
- [ ] On crash/restart: reload denylist, recompute cross-safe from last known-good height, reconcile with current L2 heads.

Notes for M6 planning:
- Absolute rollback and denylist integration are in place; multi-chain lifecycle is isolated per chain via `chainHandle` locks.
- Next step is DB ingestion/persistence inside SV2 (events/local-safe/cross-safe) without changing op-node storage.

Once we've done this we are done!




Please ask clarifying questions!
