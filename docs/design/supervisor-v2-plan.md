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
- [x] Add env/config for l1_scope and l1_conf_depth (default finalized) — `SV2_L1_SCOPE`, depth plumbed in adapter
- [x] Ensure denylist endpoints exist (check/add) and persist forever (no GC)
- [x] Expose heads/status (`/status`) inc. `cross_finalized` and `l1_scope_label`
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
- [x] Safe writer: persist (L2→L1 origin) via rollup RPC into `local_safe.db` during tick ingest
- [x] L1 head reader for unsafe/safe/finalized; honor configured `SV2_L1_SCOPE`
- [x] Compute cross-finalized = min eligible across chains; monotonic (`/status.cross_finalized`)
- [x] Build snapshot and call checker(s) (height/env-deny test checkers wired)
- [x] On violation: denylist payloadID, rollback to H-1, wait readiness (auto via adapter)
- Caveat: on restart, backfill latest local-safe once before resuming (implemented)

Progress: Implemented. Unit tests green; single-chain and two-chain smokes passing.


### Phase 2 – Checker interface and built-ins

- Define `BlockValidityChecker` interface
- Provide minimal `SignalSet`/snapshot to resolve payload IDs at height
- Built-ins: env deny (test), height-based (test);
- Execute proposals (denylist + rollback) in finalized runner when enabled

Deliverables:
- Checker interface + registration
- Tests: unit (runner/checker), integration (proposal → denylist+rollback → replacement)

Implementation checklist (TODOs / caveats):
- [x] Define `BlockValidityChecker` and snapshot resolver
- [x] Wire proposals to denylist+rollback executor behind `SV2_ENABLE_CHECKERS`
- [x] Height-based checker for tests; env deny for tests
- Caveat: run at configured L1 scope (default finalized/safe)

Progress: Implemented and exercised by smoke tests (auto-rollback via checker path).


### Phase 3 – Two-chain stability and observability

- Make cross-finalized computation resilient (monotonic, per-chain readiness)
- Health/status endpoints
- Metrics (loop latency, proposals, executed actions)

Deliverables:
- Two-chain smoke suite green and stable
- Basic metrics and status JSON

Implementation checklist (TODOs / caveats):
- [x] Status JSON includes per-chain heads and `cross_finalized`, `l1_scope_label`
- [ ] Prometheus metrics: loop latency, proposals, executed actions, cooldowns
- [ ] Cooldowns for rollbacks per chain (prevent flap)
- Caveat: surface bottleneck chain for cross-finalized

Progress: Two-chain smokes green; metrics/cooldowns pending.


### Phase 4 – External inputs (optional)

- Add a minimal external checker adapter (e.g., Solana attestation)

Deliverables:
- Example external checker, feature-flagged

Implementation checklist (TODOs / caveats):
- [ ] Minimal HTTP client poller for external signal → Proposal
- [ ] Idempotent enqueue/execute
- Caveat: feature-flagged off by default


### Phase 5 – Hardening and polish

- Cooldowns and idempotency for rollbacks (per chain)
- Durable denylist storage and audit log

Implementation checklist (TODOs / caveats):
- [ ] Persist denylist (bolt/sqlite) and append-only audit log in SV2 datadir
- [ ] Idempotent operations on payloadID; expose list/recent endpoints
- Caveat: no GC


### RPC compatibility (v1 surface on SV2)

- Implement v1-compatible RPC in SV2 (CrossSafe/LocalSafe/Unsafe/Finalized/SyncStatus/SuperRoot/CheckAccessList)

Implementation checklist (TODOs / caveats):
- [ ] Implement read-only RPCs mapped to SV2 state
- [ ] Defer CheckAccessList parity until after runner is stable


### Later (out of initial scope)

- Safe overlay (no rollback; pause on safe reorg and reset to finalized)
- Action queue; sidecar subscriptions (if needed)


### Status and metrics

- Status: per-chain L2 heads (unsafe/safe/finalized), `local_safe`, `cross_safe` head (if present), `cross_finalized`, `l1_scope_label`
- Metrics: Prometheus counters/gauges (pending)


### Testing status (as of now)

- Unit: denylist store; finalized runner; executor
- SV2 smokes:
  - Single-chain safe progression via batcher; cross-safe single-chain progression
  - Two-chain: valid exec stable; invalid exec auto-reorg; exec reorgs on remote init rollback (B auto-rollback)
  - After-safe rollback variants and serialized progression (stability)


### Risks and mitigations

- Finalization latency vs safe responsiveness
- Replacement timing after rollback → tests wait for replacement before proceeding
- Multi-chain skew → surface bottleneck in status


### Milestone checklist (quick)

- [x] Phase 1 runner + single/two-chain tests
- [x] Phase 2 checker interface + basic checks
- [ ] Phase 3 status/metrics + stability (metrics/cooldowns remaining)
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

