## Supervisor v1 cross-safe: a detailed walkthrough

### Purpose

- Cross-safe determines which L2 blocks are safe across chains, given L1 scope and cross-chain message dependencies.
- It persists two derivation indexes per chain:
  - local-safe: “optimistic” derived-from mapping for each L2 block from L1
  - cross-safe: subset of local-safe that passes cross-chain validity checks

This doc explains the core code paths, DB layout, and how to use the cross-safe package standalone (without the rest of Supervisor v1 orchestration).


### Key interfaces and update flow

- The cross-safe update entrypoint and dependencies:

```go
// CrossSafeDeps defines what the cross-safe algorithm needs from storage.
type CrossSafeDeps interface {
    reads.Acquirer
    CrossSafe(chainID eth.ChainID) (pair types.DerivedBlockSealPair, err error)
    SafeFrontierCheckDeps
    SafeStartDeps
    CandidateCrossSafe(chain eth.ChainID) (candidate types.DerivedBlockRefPair, err error)
    NextSource(chain eth.ChainID, source eth.BlockID) (after eth.BlockRef, err error)
    PreviousCrossDerived(chain eth.ChainID, derived eth.BlockID) (prevDerived types.BlockSeal, err error)
    OpenBlock(chainID eth.ChainID, blockNum uint64) (ref eth.BlockRef, logCount uint32, execMsgs map[uint32]*types.ExecutingMessage, err error)
    UpdateCrossSafe(chain eth.ChainID, l1View eth.BlockRef, lastCrossDerived eth.BlockRef) error
    InvalidateLocalSafe(chainID eth.ChainID, candidate types.DerivedBlockRefPair) error
}

// CrossSafeUpdate drives one step of cross-safe progress, or adjusts L1 scope if out-of-scope.
func CrossSafeUpdate(logger log.Logger, chainID eth.ChainID, d CrossSafeDeps, linker depset.LinkChecker) error
```

- Hazard checks used during update:

```go
// Builds a hazard set for the candidate block, checking that all intra/inter-chain deps exist
// and are within L1 scope (inL1Source), to rule out invalid cross-chain cycles.
func CrossSafeHazards(d SafeStartDeps, linker depset.LinkChecker, logger log.Logger, chainID eth.ChainID, inL1Source eth.BlockID, candidate types.BlockSeal) (*HazardSet, error)
```

- Update flow (scopedCrossSafeUpdate):
  1. Select candidate via `CandidateCrossSafe` (next local-safe derived eligible under current L1 scope)
  2. Compute hazard set and run checks: frontier membership and cycle checks
  3. If all reads are consistent and checks pass, `UpdateCrossSafe` promotes the candidate into the cross-safe DB
  4. If out-of-scope, advance L1 scope (`NextSource`) and repeat previous L2 block to keep continuity
  5. If the candidate conflicts with prior cross-safe data, invalidate the local-safe block (`InvalidateLocalSafe`)


### Event-driven wiring (reference)

- In Supervisor v1, the backend registers a `CrossSafeWorker` per chain which reacts to DB events and calls `CrossSafeUpdate`. You can copy this pattern but you do not need the full event system to use cross-safe storage.

```go
type CrossSafeWorker struct { /* logger, chainID, deps, linker */ }

func (c *CrossSafeWorker) OnEvent(ctx context.Context, ev event.Event) bool {
    switch ev.(type) {
    case superevents.UpdateCrossSafeRequestEvent:
        _ = CrossSafeUpdate(c.logger, c.chainID, c.d, c.linker)
    default:
        return false
    }
    return true
}
```


### Databases: what gets stored and where

- ChainsDB manages per-chain DBs and exposes the methods cross-safe relies on.
  - `local_safe.db` (DerivationStorage): local derived-from mapping
  - `cross_safe.db` (DerivationStorage): cross-validated derived-from mapping
  - `log.db` (LogStorage): unsafe events and per-block executing messages

```go
// Database file names
const (
  DBLocalSafe Database = "local_safe"
  DBCrossSafe Database = "cross_safe"
)
var Databases = map[Database]string{
  DBLocalSafe: "local_safe.db",
  DBCrossSafe: "cross_safe.db",
}
```

- ChainsDB structure (subset):

```go
type ChainsDB struct {
  logDBs     locks.RWMap[eth.ChainID, LogStorage]       // unsafe events
  localDBs   locks.RWMap[eth.ChainID, DerivationStorage]// local-safe index
  crossDBs   locks.RWMap[eth.ChainID, DerivationStorage]// cross-safe index
  finalizedL1 locks.RWValue[eth.L1BlockRef]             // L1 finality
  readRegistry *reads.Registry                          // read-set invalidation for reorg-safety
}
```

- Persisting cross-safe progress (happy path):

```go
// local DB already contains the new block & any replacement data; sync cross DB to it using the same revision
revision, _ := localDB.SourceToRevision(l1View.ID())
_ = crossDB.AddDerived(l1View, lastCrossDerived, revision)
// emit CrossSafeUpdateEvent and record metrics
```

- Invalidation path:

```go
// If a local-safe block cannot be promoted to cross-safe, invalidate it:
// - mark invalidated in local-safe DB (RewindAndInvalidate)
// - reset cross-unsafe if needed
// - rewind logs DB to drop events from the invalidated block onwards
// - emit InvalidateLocalSafeEvent to trigger replacement indexing
```


### The fromda derivation DB: keys and revisions

- Both local-safe and cross-safe use a compact append-only index (`fromda.DB`) with entries keyed by:
  - source (L1 block number)
  - revision (monotonic per invalidation/replacement epoch)
  - derived (L2 block number; monotonic within a revision)

Properties:
- Multiple L2 blocks can map to the same source (empty L1 blocks repeat last L2)
- A given L2 number may reoccur later (replacement) under a higher revision if it was invalidated
- Lookups support both directions (source→last derived, derived→source), first/last occurrence, previous/next navigation

```go
// Examples of core APIs
DerivedToRevision(derived eth.BlockID) (types.Revision, error)
SourceToRevision(source eth.BlockID) (types.Revision, error)
SourceToLastDerived(source eth.BlockID) (types.BlockSeal, error)
PreviousDerived(derived eth.BlockID, revision types.Revision) (types.BlockSeal, error)
Candidate(maxSource eth.BlockID, afterDerived eth.BlockID, revision types.Revision) (types.DerivedBlockRefPair, error)
AddDerived(source eth.BlockRef, derived eth.BlockRef, revision types.Revision) error
ReplaceInvalidatedBlock(inv reads.Invalidator, replacementDerived eth.BlockRef, invalidated common.Hash) (types.DerivedBlockRefPair, error)
```


### Using cross-safe without the full Supervisor v1

Goal: ingest local-safe and logs, then compute/promote cross-safe using the same algorithm, without spinning up the full event system.

Minimal steps:
1) Open data dir and per-chain DBs
- Create `ChainsDB` and attach `local_safe.db`, `cross_safe.db`, `log.db` per chain (use `db.OpenLocalDerivationDB`, `db.OpenCrossDerivationDB`, `db.OpenLogDB`).

2) Feed local-safe and logs
- For each L2 block, call `ChainsDB.UpdateLocalSafe(chainID, l1Ref, l2Ref, nodeID)`
- For each unsafe block/log, call `ChainsDB.AddLog(...)` then `ChainsDB.SealBlock(...)`

3) Run cross-safe updates in a loop
- Implement `CrossSafeDeps` by delegating to `ChainsDB` (or reuse a small adapter, see SV2’s `crosssafeAdapter` for reference)
- Call `CrossSafeUpdate(logger, chainID, deps, linker)` on a timer or after each `LocalSafeUpdateEvent`
- On `types.ErrOutOfScope`, call `NextSource` and continue; on `types.ErrConflict`, call `InvalidateLocalSafe`

4) Query heads and provenance
- `ChainsDB.CrossSafe(chainID)` → last cross-safe (source/derived pair)
- `ChainsDB.LocalSafe(chainID)` → last local-safe (source/derived pair)
- `ChainsDB.CrossDerivedToSource(chainID, derived)` → the L1 source a cross-safe/eligible local block was derived from

Notes:
- You do not need Supervisor v1’s `syncnode`, `processors`, or `frontend` to use the DBs; a simple poll/ingest loop suffices.
- The hazard checks require a `depset.LinkChecker` across chains and per-block executing messages via `OpenBlock` (from the logs DB), so make sure your ingestion populates executing messages.


### How local-safe mappings are learned from op-node

Flow: op-node derives a new L2 block from an L1 origin → supervisor receives a `LocalDerivedEvent` → `ChainsDB.UpdateLocalSafe` records the mapping (L1 source → L2 derived) in `local_safe.db`.

- Event shape:
```94:102:op-supervisor/supervisor/backend/superevents/events.go
type LocalDerivedEvent struct {
  ChainID eth.ChainID
  Derived types.DerivedBlockRefPair
  NodeID  string
}
```

- Emitted by the managed node when the op-node reports a derivation update:
```351:359:op-supervisor/supervisor/backend/syncnode/node.go
func (m *ManagedNode) onDerivationUpdate(pair types.DerivedBlockRefPair) {
  m.emitter.Emit(m.ctx, superevents.LocalDerivedEvent{
    ChainID: m.chainID,
    Derived: pair,
    NodeID:  m.Node.String(),
  })
  m.lastNodeLocalSafe = pair.Derived.ID()
}
```

- Consumed by the DB to persist local-safe and emit `LocalSafeUpdateEvent`:
```211:221:op-supervisor/supervisor/backend/db/db.go
case superevents.LocalDerivedEvent:
  if !db.isInitialized(x.ChainID) { /* skip until anchor */ return false }
  db.UpdateLocalSafe(x.ChainID, x.Derived.Source, x.Derived.Derived, x.NodeID)
```

- The write itself:
```100:135:op-supervisor/supervisor/backend/db/update.go
func (db *ChainsDB) initializedUpdateLocalSafe(chain eth.ChainID, source eth.BlockRef, lastDerived eth.BlockRef, nodeId string) {
  if err := localDB.AddDerived(source, lastDerived, types.RevisionAny); err != nil { /* ... */ }
  db.emitter.Emit(db.rootCtx, superevents.LocalSafeUpdateEvent{
    ChainID: chain,
    NewLocalSafe: types.DerivedBlockSealPair{
      Source:  types.BlockSealFromRef(source),
      Derived: types.BlockSealFromRef(lastDerived),
    },
  })
}
```


### SV2 in-process event hook (future: easy population of cross DBs)

Since SV2 embeds an op-node per chain, you can register a tiny in-process event handler to capture L1→L2 derivations directly from the engine and feed `local_safe.db` (and then cross-safe) without any extra RPC.

- The engine emits local-safe promotion with both L2 and L1 refs (pre-interop too):
```116:123:op-node/rollup/engine/events.go
type PromoteLocalSafeEvent struct {
  Ref    eth.L2BlockRef
  Source eth.L1BlockRef
}
```

- Emission is unconditional (no interop gate):
```436:451:op-node/rollup/engine/events.go
case PromotePendingSafeEvent:
  if x.Concluding && x.Ref.Number > d.ec.LocalSafeL2Head().Number {
    d.emitter.Emit(ctx, PromoteLocalSafeEvent{Ref: x.Ref, Source: x.Source})
  }
```

- Minimal plan in SV2:
  - Create a small `event.Deriver` impl inside SV2 that subscribes to op-node’s emitter and listens for `PromoteLocalSafeEvent` and `LocalSafeUpdateEvent`.
  - On each event, call `ChainsDB.UpdateLocalSafe(chainID, l1Ref, l2Ref, nodeID)` to append to `local_safe.db`.
  - Run the cross-safe step (`CrossSafeUpdate`) after local-safe updates to populate `cross_safe.db`.

This avoids extra network surfaces and keeps the op-node diff minimal; SV2 just wires an internal subscriber.


### Verifying behavior by reading the code

- CrossSafeDeps and update entry:
```12:40:op-supervisor/supervisor/backend/cross/safe_update.go
type CrossSafeDeps interface {
    reads.Acquirer
    CrossSafe(chainID eth.ChainID) (pair types.DerivedBlockSealPair, err error)
    SafeFrontierCheckDeps
    SafeStartDeps
    CandidateCrossSafe(chain eth.ChainID) (candidate types.DerivedBlockRefPair, err error)
    NextSource(chain eth.ChainID, source eth.BlockID) (after eth.BlockRef, err error)
    PreviousCrossDerived(chain eth.ChainID, derived eth.BlockID) (prevDerived types.BlockSeal, err error)
    OpenBlock(chainID eth.ChainID, blockNum uint64) (ref eth.BlockRef, logCount uint32, execMsgs map[uint32]*types.ExecutingMessage, err error)
    UpdateCrossSafe(chain eth.ChainID, l1View eth.BlockRef, lastCrossDerived eth.BlockRef) error
    InvalidateLocalSafe(chainID eth.ChainID, candidate types.DerivedBlockRefPair) error
}
```

```105:138:op-supervisor/supervisor/backend/cross/safe_update.go
func scopedCrossSafeUpdate(h reads.Handle, logger log.Logger, chainID eth.ChainID, d CrossSafeDeps, linker depset.LinkChecker) (update types.DerivedBlockRefPair, err error) {
    candidate, err := d.CandidateCrossSafe(chainID)
    // ... depend on L1/L2, build hazards, run checks ...
    if !h.IsValid() { return }
    if err := d.UpdateCrossSafe(chainID, candidate.Source, candidate.Derived); err != nil { /* ... */ }
    return candidate, nil
}
```

- Hazard scope guard (L1 scope for cross-safe):
```20:43:op-supervisor/supervisor/backend/cross/safe_start.go
func CrossSafeHazards(d SafeStartDeps, linker depset.LinkChecker, logger log.Logger, chainID eth.ChainID, inL1Source eth.BlockID, candidate types.BlockSeal) (*HazardSet, error) {
    safeDeps := &SafeHazardDeps{SafeStartDeps: d, inL1Source: inL1Source}
    return NewHazardSet(safeDeps, linker, logger, chainID, candidate)
}
```

- DB file layout:
```7:16:op-supervisor/supervisor/backend/db/sync/sync.go
var Databases = map[Database]string{
  DBLocalSafe: "local_safe.db",
  DBCrossSafe: "cross_safe.db",
}
```

- ChainsDB cross-safe update (persist + emit):
```162:209:op-supervisor/supervisor/backend/db/update.go
func (db *ChainsDB) initializedUpdateCrossSafe(chain eth.ChainID, l1View eth.BlockRef, lastCrossDerived eth.BlockRef) error {
    revision, _ := localDB.SourceToRevision(l1View.ID())
    _ = crossDB.AddDerived(l1View, lastCrossDerived, revision)
    db.emitter.Emit(db.rootCtx, superevents.CrossSafeUpdateEvent{ /* ... */ })
    // sync cross-unsafe <= cross-safe if needed
}
```

- Invalidating a local-safe block:
```245:281:op-supervisor/supervisor/backend/db/update.go
func (db *ChainsDB) InvalidateLocalSafe(chainID eth.ChainID, candidate types.DerivedBlockRefPair) error {
    _ = localSafeDB.RewindAndInvalidate(db.readRegistry, candidate)
    _ = db.ResetCrossUnsafeIfNewerThan(chainID, candidate.Derived.Number)
    _ = eventsDB.Rewind(db.readRegistry, candidate.Derived.ParentID())
    db.emitter.Emit(db.rootCtx, superevents.InvalidateLocalSafeEvent{ ChainID: chainID, Candidate: candidate })
}
```

- fromda DB model (append-only, revisions):
```28:45:op-supervisor/supervisor/backend/db/fromda/db.go
// Each entry increments in L1 (source) and/or L2 (derived) dimensions, with a revision that bumps on invalidation
// Key-space order: source number, then revision number, then derived number (within revision)
```


### Minimal standalone usage sketch (no full Supervisor v1)

```go
// create ChainsDB + open per-chain DBs
chains := []eth.ChainID{chainA, chainB}
cdb := db.NewChainsDB(logger, depSet, metrics.NoopMetrics)
for _, id := range chains {
  logDB, _ := db.OpenLogDB(logger, id, dataDir, chainMetrics)
  localDB, _ := db.OpenLocalDerivationDB(logger, id, dataDir, chainMetrics)
  crossDB, _ := db.OpenCrossDerivationDB(logger, id, dataDir, chainMetrics)
  cdb.AddLogDB(id, logDB); cdb.AddLocalDerivationDB(id, localDB); cdb.AddCrossDerivationDB(id, crossDB); cdb.AddCrossUnsafeTracker(id)
}

// ingest
cdb.UpdateLocalSafe(chainA, l1Ref, l2Ref, "node-0")
cdb.AddLog(chainA, logHash, parentID, idx, execMsg)
cdb.SealBlock(chainA, unsafeRef)

// run cross-safe step
deps := yourDepsImpl{cdb}
_ = cross.CrossSafeUpdate(logger, chainA, deps, depset.LinkerFromConfig(depSet))

// query
cs, _ := cdb.CrossSafe(chainA)
```


### Operational notes

- Reads use a `reads.Registry` to ensure consistency across reorgs: if inputs are invalidated mid-read, updates abort and retry.
- When cross-safe advances, cross-unsafe is kept <= cross-safe for monotonicity.
- Local-safe invalidation rewinds log indexing past the invalidated block and triggers replacement ingestion.


### Questions / confirmations

- Do you want a minimal adapter (like SV2’s `crosssafeAdapter`) added under `op-supervisor` to make `CrossSafeDeps` wiring turnkey outside the backend?
- Should I include a tiny CLI that opens a data dir and runs one step of cross-safe update for a given chain to help manual testing?


