### op-node: Layered derivation refactor (unsafe → local-safe → cross-safe → finalized)

Owner: Core Protocol
Status: Draft for discussion
Audience: op-node, op-supervisor, op-conductor maintainers

## Goal

- **Factor op-node derivation into isolated layers** that behave like stacked consensus protocols with increasing eventual consistency.
- Preserve current behavior by default; enable a minimal and incremental path to expose clear boundaries and swap/augment layers.
- Align public semantics so that external "safe" becomes the cross-chain validated head, while retaining the current local view as "local-safe".

## Terms

- **unsafe**: Best-effort L2 head from p2p/alt-sync/gossip (reorgable).
- **local-safe**: L2 head fully derived from locally confirmed L1 data (subject to L1 reorg within configured conf-depth).
- **cross-safe (safe)**: L2 head proven consistent across the superchain (via supervisor/interop). This will be presented as "safe" to external consumers.
- **finalized**: L2 head that is final per finality rules (L1 finality or AltDA scheme), i.e. irreorgable.

## Current architecture snapshot (relevant pieces)

- `driver.NewDriver` wires: `derive.NewDerivationPipeline`, `engine.NewEngineController`, `finality.NewFinalizer`, `status.NewStatusTracker`, `clsync`, scheduler, optional `sequencer`.
- `status.StatusTracker` already models `LocalSafeL2`, `CrossSafeL2`, and maps `SafeL2` to cross-safe on `engine.CrossSafeUpdateEvent`.
- `finality.Finalizer` finalizes based on L1 finalized signals and safe-derivation provenance; interop time gates behavior.
- `node.OpNode` subscribes to L1 unsafe/safe/finalized, initializes interop subsystem when configured, and exposes RPC.

Conclusion: most primitives exist; the refactor focuses on isolating responsibilities, naming, and public semantics.

## Design: three isolated derivation layers (+ finality)

Each layer is a small deriver with a single-responsibility interface and explicit inputs/outputs. Layers communicate only via events and typed head updates; no shared mutable state.

- Layer 0 – UnsafeDeriver
  - Inputs: p2p/alt-sync payloads, local engine state, scheduler ticks.
  - Output events: `engine.CrossUnsafeUpdateEvent{CrossUnsafe, LocalUnsafe}` (CrossUnsafe mirrors LocalUnsafe unless interop overrides).
  - Guarantees: monotonic local-unsafe head within the local engine forkchoice.

- Layer 1 – LocalSafeDeriver
  - Inputs: L1 fetcher with conf-depth (`confdepth.VerifierConfDepth`), `derive.DerivationPipeline`.
  - Output events: `engine.LocalSafeUpdateEvent{Ref}` and `engine.PendingSafeUpdateEvent{}` during in-flight derivation.
  - Guarantees: head is reproducible from locally confirmed L1 data.

- Layer 2 – CrossSafeDeriver (interop)
  - Inputs: local-safe head, supervisor/cross-chain backend.
  - Output events: `engine.CrossSafeUpdateEvent{CrossSafe, LocalSafe}`.
  - Guarantees: cross-chain validated safe head. Public "safe" maps to this value.

- Layer 3 – Finalizer
  - Inputs: L1 finalized signals or AltDA windows; provenance from LocalSafeDeriver.
  - Output events: `engine.PromoteFinalizedEvent{Ref}`.

Event routing remains via `event.System`; scheduling via `StepSchedulingDeriver` is retained.

### Public semantics change

- Externally visible `safe` becomes the cross-safe head. The existing `LocalSafeL2` remains available for advanced/diagnostic use.
- Backward compatibility: for nodes without interop enabled, `cross-safe == local-safe`, so external `safe` is unchanged.

## Minimal change plan (incremental)

1) Interfaces and naming (no behavior change)
   - Introduce small, explicit interfaces in `rollup/driver`:
     - `UnsafeDeriverIface`, `LocalSafeDeriverIface`, `CrossSafeDeriverIface` with `AttachEmitter`, `OnEvent`, and `Start/Stop` when applicable.
   - Add comments and type aliases mapping existing components to the above roles.

2) Event boundary hardening
   - Ensure `LocalSafeUpdateEvent` is emitted only by LocalSafeDeriver; `CrossSafeUpdateEvent` only by CrossSafeDeriver (interop backend).
   - Confirm `status.StatusTracker` continues to set `SafeL2 = CrossSafeL2` and keep `LocalSafeL2` separately.

3) Public API semantics gate
   - In `node/server.go` API surfaces and in JSON-RPC responses: treat `safe` as cross-safe. Keep `local_safe` fields (already present in `eth.SyncStatus`).
   - For non-interop networks, emit `CrossSafeUpdateEvent{CrossSafe: LocalSafe, LocalSafe: LocalSafe}` so external consumers observe identical values.

4) Derivation pipeline isolation (scoped, mechanical)
   - Wrap `derive.NewDerivationPipeline` in a `LocalSafeDeriver` shell that:
     - Consumes L1 origin from `confdepth`.
     - Emits `PendingSafeUpdateEvent` while producing attributes.
     - Emits `LocalSafeUpdateEvent` when attributes are fully processed into the engine’s safe head.
   - No cross-layer calls; communication is via events.

5) Finalizer alignment
   - Keep current `finality.Finalizer` behavior. It already skips provenance recording after interop activation and relies on supervisor for finalization.
   - Validate that `TryFinalizeEvent` is only triggered after LocalSafeDeriver idle events.

6) Metrics and logging
   - Add per-layer metrics: queue lengths, head numbers, lag to wallclock.
   - Do not change existing metric names; add new ones with `layer_` prefixes.

## Files touched (expected small, localized edits)

- `op-node/rollup/driver/driver.go`: register explicit layer derivers; no logic change.
- `op-node/rollup/status/status.go`: already supports `LocalSafeL2`/`CrossSafeL2`; keep mapping of `SafeL2` to `CrossSafeL2`.
- `op-node/rollup/finality/finalizer.go`: no behavioral change; minor guard comments around interop.
- `op-node/node/server.go`: ensure RPC continues reporting `safe` as cross-safe; expose `local_safe` where applicable.
- `op-node/rollup/interop/*`: confirm `CrossSafeUpdateEvent` emission semantics.

## Rollout

- Feature flag: existing interop/superchain config enables CrossSafeDeriver. Otherwise, cross-safe mirrors local-safe.
- No config migrations; external clients continue reading `safe`. Advanced clients can optionally read `local_safe`.

## Risks

- Divergence between local-safe and cross-safe during partial interop deployments. Mitigation: metrics, alerts on sustained divergence, and RPC field availability for troubleshooting.
- Edge cases around engine resets and event ordering. Mitigation: existing reset events remain the single recovery path across layers.

## Test plan (systematic, one-to-one)

- Unit tests per layer
  - UnsafeDeriver: insert/gossip sequences, reorg handling, backoff scheduling.
  - LocalSafeDeriver: L1 origin conf-depth windows, derivation idle signaling, safe promotion.
  - CrossSafeDeriver: supervisor stream driving `CrossSafeUpdateEvent`, mirror behavior without interop.
  - Finalizer: provenance windows, `TryFinalizeEvent` scheduling and promotion.

- Integration tests
  - No-interop network: `safe == local_safe`, finalized progresses with L1 finality.
  - Interop-enabled network: `safe == cross_safe`, `local_safe` lags `safe` under cross-validation; finalized driven by supervisor.

- RPC compatibility tests
  - Existing clients reading `safe` observe unchanged behavior without interop; with interop, observe cross-safe.
  - New clients can read `local_safe` for advanced use.

- Fault injection
  - Drop/lag supervisor; ensure cross-safe halts while local-safe advances; recovery resumes.
  - L1 reorgs through conf-depth; validate local-safe reorg, cross-safe stability, and finalizer safety.

## Appendix: migration notes

- Naming: docs and comments refer to the layers; code changes keep edits minimal and avoid large renames. Public docs should state that "safe" means cross-safe on interop networks.


