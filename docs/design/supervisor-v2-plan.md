## Supervisor v2: finalized-only plan and milestones

### Fresh prompt

Use this prompt when spinning up an AI to work on SV2:

"""
We are building Supervisor v2 (SV2), a minimal finalized-only supervisor that embeds one op-node per chain.

Read these docs first:
- docs/design/supervisor-v2-plan.md (this plan with milestones and checklists)
- docs/design/op-supervisor-v2.md (overall SV2 design)
- docs/design/supervisor-v1-cross-safe-walkthrough.md (cross-safe background and DBs)

Key constraints and context:
- Cross-finalized only initially (no safe overlay). L1 scope is configurable: unsafe | safe | finalized (default: finalized). If safe, use l1_conf_depth.
- Ingest local-safe {L1 origin, L2 ref} via in-process op-node events (PromoteLocalSafeEvent). Build cross-finalized = min over chains of eligible local-safe (per L1 scope).
- Run validity checks at cross-finalized; for violations: add deterministic payloadID to denylist and rollback EL/op-node to H-1. Denylist is persisted forever (no TTL/GC).
- Status/metrics: Prometheus-compatible; simple JSON status. v1 RPC compatibility (including CheckAccessList) comes after runner is green.
- Do not add legacy interop activation or heavy rewinder/syncnode logic. Keep code minimal; tests must accompany changes.
"""

### Goals

- Minimal, robust SV2 that:
  - Embeds one op-node per chain
  - Computes cross-finalized strictly on L1 finality (no reorg handling logic)
  - Runs block-validity checks at cross-finalized and enforces via denylist + EL rollback
  - Exposes a small status/admin surface

Non-goals (initially):
- Safe overlay, cross-safe persistence, action queues, sidecar services


### Phase 0 – Foundations (infra and API)

- Wire per-chain op-node embedding and single-port proxy (existing)
- Add minimal internal API in SV2 for:
  - Denylist add/check
  - EL rollback (to_block_number)
  - Heads read (per chain)

Deliverables:
- Clean denylist + rollback tested single-chain (already covered by smoke tests)

Implementation checklist (TODOs / caveats):
- [ ] Add env/config for l1_scope and l1_conf_depth (default finalized)
- [ ] Ensure denylist endpoints exist (check/add) and persist forever (no GC)
- [ ] Expose heads/status stubs to be filled later
- Caveat: reuse existing rollback path; avoid changes to op-node except in-process wiring


### Phase 1 – Finalized-only runner (no reorg handling)

- Persist local-safe L2→L1 origin mapping using rollup RPC:
  - On SafeL2 progression per chain, fetch `L2BlockRef` (includes L1 origin) by number/hash
  - Write (L2 ref → L1 origin) into v1 `local_safe.db` (fromda)
- Read L1 finalized head periodically
- Compute cross-finalized height:
  - For each chain, eligible = latest local-safe whose L1 origin ≤ finalized L1
  - Cross-finalized = min over chains of eligible.L2 number
- Build snapshot at cross-finalized and run checkers
- On violation: denylist payloadID and EL rollback to H-1; let op-node re-derive

Deliverables:
- Runner loop with hard gating on L1 finalized only
- Tests:
  - Single-chain finalized-gated rollback
  - Two-chain A-only finalized-gated rollback

Implementation checklist (TODOs / caveats):
- [ ] Safe writer: for (lastSafe, Safe] persist (L2→L1 origin) via rollup RPC into `local_safe.db`
- [ ] L1 head reader for unsafe/safe/finalized; honor configured l1_scope
- [ ] Compute cross-finalized = min eligible across chains; monotonic
- [ ] Build snapshot at cross-finalized and call checker(s)
- [ ] On violation: denylist payloadID, rollback to H-1, wait readiness
- Caveat: on restart, backfill latest local-safe once before resuming


### Phase 2 – Checker interface and built-ins

- Define `BlockValidityChecker`:
  - `Evaluate(ctx, snapshot, signals) -> []Proposal`
  - Stage is always Finalized in this phase
- Provide minimal `SignalSet` (L1/L2 header reads, optional external sources)
- Ship 1–2 basic checks (e.g., placeholder invariants, deny specific payload ID for tests)

Deliverables:
- Checker interface + registration
- Tests:
  - Unit tests for checker Evaluate
  - Integration: proposal → denylist+rollback → replacement at H

Implementation checklist (TODOs / caveats):
- [ ] Define BlockValidityChecker and minimal SignalSet
- [ ] Adapter to reuse v1 cross-safe calculator at finalized scope (primary checker)
- [ ] Wire proposals to denylist+rollback executor
- Caveat: run only at configured L1 scope (default finalized)


### Phase 3 – Two-chain stability and observability

- Make cross-finalized computation resilient (monotonic, per-chain readiness)
- Health/status endpoints:
  - Current local-safe per chain
  - Current cross-finalized
  - Last action (denylist/rollback), reason
- Metrics: loop latency, proposals, executed actions, per-chain cooldowns

Deliverables:
- Two-chain smoke suite green and stable
- Basic metrics and status JSON
- Tests:
  - Flake-hunting soak: two-chain finalized progression for N minutes
  - Status endpoint validation

Implementation checklist (TODOs / caveats):
- [ ] Prometheus metrics: loop latency, proposals, executed actions, cooldowns
- [ ] Status JSON: per-chain heads (unsafe/safe/finalized), eligible local-safe, cross-finalized, L1 heads, last action summary, denylist count
- [ ] Cooldowns for rollbacks per chain (prevent flap)
- Caveat: surface bottleneck chain for cross-finalized


### Phase 4 – External inputs (optional)

- Add a minimal “external check” adapter (e.g., Solana attestation)
- Keep pull-based: periodic poll → proposal → finalized runner executes

Deliverables:
- Example external checker, feature-flagged
- Tests:
  - Mock external source → proposal → rollback flow

Implementation checklist (TODOs / caveats):
- [ ] Minimal HTTP client poller for external signal
- [ ] Map to Proposal; idempotent enqueue/execute
- Caveat: keep disabled by default via feature flag


### Phase 5 – Hardening and polish

- Cooldowns and idempotency for rollbacks (per chain)
- Durable denylist storage (small table: chain_id, payload_id, reason, created_at)
- Audit log of actions

Deliverables:
- Denylist persistence (keep entries forever; no TTL/GC) and short audit trail
- Tests:
  - Denylist persistence/idempotency
  - Cooldown behavior per chain
  - Audit log entries for actions

Implementation checklist (TODOs / caveats):
- [ ] Persist denylist (bolt/sqlite) in SV2 datadir; append-only audit log
- [ ] Idempotent operations on payloadID; expose list/recent endpoints
- Caveat: no GC; consider future pruning tools separately


### RPC compatibility (v1 surface on SV2)

- Implement v1-compatible RPC in SV2 (CrossSafe/LocalSafe/Unsafe/Finalized/SyncStatus/SuperRoot/CheckAccessList)
- Minimal ChainsDB ingestion (logs + local-safe) to back CheckAccessList

Sequencing:
- Defer CheckAccessList parity until after the finalized runner is green and stable

Deliverables:
- RPC handlers mapped to SV2 state
- Tests:
  - Golden parity tests per endpoint vs v1 on the same datadir (except CheckAccessList initially)
  - Add CheckAccessList parity tests post-runner

Implementation checklist (TODOs / caveats):
- [ ] Implement CrossSafe/LocalSafe/LocalUnsafe/Finalized/SyncStatus/SuperRoot on SV2
- [ ] Embed ChainsDB + minimal log ingestor to back CheckAccessList (deferred)
- [ ] Map SV2 cross-finalized as RPC "safe" in this mode; document
- Caveat: defer CheckAccessList until runner is green


### Later (out of initial scope)

- Safe overlay (no rollback; pause on safe reorg and reset to finalized)
- Action queue for multi-service inputs (idempotent, coalescing)
- Sidecar subscription endpoints (if needed)


### Implementation notes

- Cross-finalized is strictly derived from L1 finalized:
  - No confirmation-depth policy; only finalized qualifies
  - Cross-finalized advances monotonically and only when all chains have local-safe entries derived from finalized L1
- Use in-process event hook to avoid polling complexity; on restart, backfill once from current node heads
- Keep EL rollback sequencing the same as our tests (stop node → EL rollback → restart → wait ready)
- Denylist entries use the op-node deterministic payload ID; no extra context required


### Configuration

- l1_scope: unsafe | safe | finalized (default: finalized)
  - unsafe: eligible if L1 origin ≤ latest L1
  - safe: eligible if L1 origin has ≥ l1_conf_depth confirmations (or ≤ L1 “safe” head if available)
  - finalized: eligible only if L1 origin ≤ finalized L1
- l1_conf_depth (optional): only used if l1_scope = safe and no explicit L1 safe head is exposed

- Operational notes:
  - The process reads `SV2_L1_SCOPE` env var to set the L1 scope label at startup (`unsafe|safe|finalized`).
  - The `/status` endpoint includes `cross_finalized` as the current minimum finalized height across managed chains.

- Tests:
  - Mode=finalized: finalized-gated rollback (single/two-chain)
  - Mode=safe: with l1_conf_depth=N, assert eligibility respects depth
  - Mode=unsafe: fast progression; still enforce checks/denylist


### Status and metrics

- Status (initial): per-chain L2 heads (unsafe/safe/finalized), eligible local-safe per configured L1 scope, cross-finalized (`cross_finalized`), L1 heads, last action summary, denylist count
- Omit "next target H" for now (optional later)
- Metrics: Prometheus-compatible counters/gauges for loop latency, proposals, executed actions, per-chain cooldowns


### Testing strategy (summary)

- Unit: checkers, cross-finalized computation, denylist store
- Integration: single- and two-chain finalized-gated rollback; RPC endpoint parity; external checker mock
- Soak/CI: run two-chain suite with time budget; assert no regressions and steady cross-finalized progress


### Risks and mitigations

- Finalization latency: slower reaction vs safe-gated; acceptable for correctness-first
- Replacement timing: after rollback, wait until replacement at H is visible before further checks
- Multi-chain skew: cross-finalized bottlenecked by slowest chain – surface this in status


### Milestone checklist (quick)

- [ ] Phase 1 runner + single/two-chain tests
- [ ] Phase 2 checker interface + basic checks
- [ ] Phase 3 status/metrics + stability
- [ ] Phase 4 external check (optional)
- [ ] Phase 5 denylist persistence + audits
- [ ] Phase 6 backwards-compatibility validation


### Open questions

1) Checker scope: any must-have validity rules you want in v1 besides a payloadID deny checker?
2) Denylist persistence: file-backed (bolt/sqlite) in SV2 repo acceptable?
3) EL rollback contract: OK to keep existing single-step rollback to H-1, or add N-block rollback?
4) Status surface: which fields do you want first (for dashboards/alerts)?
5) Test infra: run finalized-only tests in CI for both single-chain and two-chain?



### Phase 6 – Backwards-compatibility validation (OP Mainnet, Unichain, delayed interop devnet)

- Objective: demonstrate SV2 works against existing networks and pre-interop configurations.

- Scope:
  - Run SV2 in read-only enforcement mode against OP Mainnet and Unichain:
    - Use public RPCs or internal mirrors for L1/L2 reads.
    - Compute cross-finalized and run checkers; do not issue rollbacks (dry-run) in production.
    - Verify denylist remains empty and no false positives are proposed.
  - Stand up an interop devnet with Interop hardfork activation delayed until block N:
    - Ensure SV2 operates with pre-interop op-node config until activation.
    - After activation, confirm continuous operation with the same SV2 binaries/configs.

- Deliverables:
  - Scripts/configs to point SV2 at OP Mainnet and Unichain (read-only mode), with metrics and status pages.
  - Devnet composition for delayed interop activation and a short runbook to validate behavior before/after activation.
  - Report summarizing cross-finalized progression, checker decisions (expected to be no-ops), and any observed discrepancies.

- Implementation checklist (TODOs / caveats):
  - [ ] Add a "dry-run"/read-only flag to disable rollback execution while still computing proposals.
  - [ ] Parameterize network configs (OP Mainnet, Unichain): L1/L2 RPCs, JWT if needed, rollup config.
  - [ ] Add dashboards/metrics panels for cross-finalized, per-chain heads, and proposal counts.
  - [ ] Compose an interop devnet with delayed activation height; document how to set N.
  - Caveats: never execute rollbacks against production networks; limit rate and scope of external RPC polling.

