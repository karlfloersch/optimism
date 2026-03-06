# Strong Consistency Plan V2 for Supernode Interop

## Status

This is a working implementation plan, not a rigid spec.

The intent of V2 is to simplify the design around the actual interop primitives we already have in the codebase:

- accepted verified snapshots
- frozen frontier snapshots
- logs DB rewind
- denylist entries that can self-validate
- a pluggable consistency checker

If implementation reveals a cleaner shape, we should keep the invariants and adjust the helper boundaries.

## Summary

The core change in V2 is to stop treating this primarily as a "canonical L1 oracle" problem.

Instead, interop should operate on per-timestamp snapshots of:

- the L2 block being considered on each chain
- the L1 block at which that L2 block became safe on that chain

From there:

1. Before processing the next timestamp, re-collect the last accepted verified snapshot and compare it to the stored one.
2. If that accepted snapshot changed, rewind interop-local state backward and retry.
3. Freeze the next timestamp's frontier once.
4. If the frontier is inconsistent, do not invalidate or rewind; just wait and try again later.
5. If the frontier is consistent, load logs, verify, and commit the new snapshot.
6. Keep the denylist lookup key the same, but add metadata so deny entries can invalidate themselves when the accepted cross-safe world changes.

This keeps the system centered on the actual interop state transitions instead of pushing most of the logic into direct L1 canonicality checks.

## Current State

Today on local `develop`:

- interop progresses one timestamp at a time
- `VerifiedResult` stores:
  - `Timestamp`
  - `L1Inclusion`
  - `L2Heads`
- `logsDB` can be rewound, but only through the existing invalidation/reset path
- denylist entries are keyed only by L2 block height and payload hash
- there is no explicit repair path for previously accepted interop state after L1/L2 reorgs

So the gaps are:

- accepted verified state can go stale
- `logsDB` can retain stale blocks/logs from an old world
- denylist entries can outlive the cross-safe world that justified them

## Definitions

### Accepted Snapshot

The latest verified interop state that we trust.

V2 should treat this as more than just `L2Heads`. The accepted snapshot should include, per chain:

- `L2Head`
- `L1Head`

And optionally the derived rollup-wide `L1Inclusion` if we still want to store it explicitly.

Suggested shape:

```go
type VerifiedResult struct {
    Timestamp   uint64
    L2Heads     map[eth.ChainID]eth.BlockID
    L1Heads     map[eth.ChainID]eth.BlockID
    L1Inclusion eth.BlockID
}
```

`L1Inclusion` is the accepted round anchor. `L1Heads` stores the per-chain L1 blocks at which the corresponding `L2Heads` were safe.

### Candidate Frontier

The frozen per-chain snapshot for the next timestamp we want to process.

For each chain:

- `L2HeadAtTimestamp`
- `L1HeadForThatL2`

This snapshot is collected once per round and reused for all later steps in that round.

### Deny Entry Metadata

The denylist key can remain:

- block height
- payload hash

But each deny entry should carry the full invalid `Result` snapshot for the round that justified the denial.

That means the deny entry value can directly reuse the same primitive interop already produces during verification, rather than inventing a separate anchor format.

Example shape:

```go
type DenyEntry struct {
    PayloadHash common.Hash
    Result      Result
}
```

This is intentionally simple.

It allows the supernode to validate a stored deny entry against the current accepted world without inventing a separate anchor metadata format.

### Consistency Checker

Interop should use a pluggable checker abstraction.

V2 keeps the snapshot-driven control flow, but the actual consistency predicate is delegated to a checker:

```go
type ConsistencyChecker interface {
    AcceptedResultConsistent(ctx context.Context, stored VerifiedResult, current VerifiedResult) (bool, error)
    FrontierConsistent(ctx context.Context, accepted VerifiedResult, frontier VerifiedResult) (bool, error)
}
```

Recommended implementations:

- `ByNumberChecker`
  - first implementation
  - uses `L1BlockRefByNumber`
  - sufficient for the first patch
- `AncestryChecker`
  - later implementation
  - uses parent-walk / cache
  - stronger than the by-number version

The control flow should depend on the interface, not on the details of the first checker.

## Core Invariants

### 1. Accepted State Must Still Match Reality

Before processing `nextTS`, the latest accepted verified snapshot must still match the chain state for that same timestamp.

If it does not match anymore, interop has stale accepted state and must move backward.

### 2. Frontier Inconsistency Means Wait, Not Immediate Rewind

If the next frontier is inconsistent, we do not treat that as proof that the already-accepted snapshot is wrong.

Instead:

- do not commit
- do not invalidate
- do not advance
- return and retry later

### 3. One Round Uses One Frozen Frontier

Interop should not re-query chain state later in the same round and accidentally mix snapshots.

The same frozen frontier object should be used for:

- consistency checks
- `loadLogs`
- verification
- L1 inclusion calculation

### 4. Deny Entries Must Self-Invalidate

A deny entry should only apply while the accepted cross-safe world that justified it still applies.

That means we should be able to avoid explicit denylist rewind if deny entries are validated against the current accepted world before being enforced.

## Proposed Algorithm

### Step 0: Read the Current Accepted Boundary

Read the latest verified timestamp from `verifiedDB`.

If nothing is verified yet:

- skip accepted-state repair
- start from activation timestamp as usual

### Step 1: Re-collect the Accepted Snapshot

If `lastTS` exists:

- query every chain for the L2 block at `lastTS`
- query every chain for the L1 block at which that L2 block became safe
- derive `L1Inclusion` from those per-chain L1 heads
- build a fresh `VerifiedResult`

Compare it to the stored accepted result at `lastTS` using the consistency checker.

If they match:

- proceed

If they do not match:

- accepted interop state is stale
- rewind interop-local state backward
- return and retry on the next loop

### Step 2: Rewind Strategy

V2 should scan backward to the newest consistent accepted timestamp, then rewind once.

So if the accepted result at `lastTS` no longer matches:

- check `lastTS`
- if inconsistent, check `lastTS - 1`
- continue until:
  - the newest consistent accepted timestamp is found, or
  - no accepted result remains

Then:

- rewind `verifiedDB` once to that boundary
- rewind `logsDB` once to match that boundary
- reset any in-memory accepted-state trackers
- return

If no accepted result remains:

- clear interop-local accepted state
- restart from activation behavior on the next loop

### Step 3: Freeze the Candidate Frontier

For `nextTS = latestAcceptedTS + 1`:

- collect `L2HeadAtTimestamp` for every chain once
- collect `L1Head` for each of those L2 heads once
- derive `L1Inclusion`
- build a frozen `VerifiedResult` candidate for `nextTS`

No later step in the round should refetch these values.

### Step 4: Frontier Consistency Gate

Run the frontier consistency check against the frozen frontier.

This check should be expressed in terms of the frozen per-chain `(L2Head, L1Head)` tuples, using the pluggable consistency checker.

If the frontier is inconsistent:

- return no progress
- do not invalidate blocks
- do not rewind accepted state
- retry later

If the frontier is consistent:

- proceed

Open question:

- the exact frontier consistency predicate still needs to be nailed down
- first implementation should use the by-number checker
- the check should be explicit and testable, not implicit in later log loading

### Step 5: Load Logs From the Frozen Frontier

`loadLogs` should use the already-frozen frontier L2 heads instead of refetching `LocalSafeBlockAtTimestamp`.

This avoids intra-round TOCTOU and makes the round reproducible in tests.

### Step 6: Verify and Commit

If the frontier passes:

- load logs
- verify messages / cycles
- if valid, commit the new accepted snapshot

That commit should include enough data to re-check the snapshot later:

- `L2Heads`
- per-chain `L1Heads`
- optionally derived `L1Inclusion`

### Step 7: Denylist Lookup Semantics

Keep the primary denylist lookup unchanged:

- lookup by block number and payload hash

But change the meaning of a hit.

The raw denylist lookup should stay simple.

On a hit, `IsDenied()` should call an internal deny-entry validator on the supernode side.

Suggested shape:

```go
func (i *Interop) ValidateDenyEntry(entry DenyEntry) (bool, error)
```

or equivalent wiring through the chain container / superauthority layer.

The important design point is:

- raw storage lookup stays cheap
- deny-entry validity is decided by a separate validator with access to interop state

#### Fast Path

If the denylist lookup misses and the payload block number is ahead of the latest verified L2 head for that chain:

- return `false` immediately

That should cover the common case.

#### Hit Path

If the denylist lookup hits:

- load the stored deny entry
- call the internal deny-entry validator
- the validator regenerates the candidate `Result` for the stored result timestamp
- the validator runs the consistency checker
- the validator compares the regenerated `Result` to the stored invalid `Result`
- only return `true` if they still match and the payload is still in `InvalidHeads`

This uses the same result primitive interop already knows how to produce, and avoids inventing a second metadata scheme.

It should not depend on re-querying the live next frontier at some unrelated timestamp or on bolting the full validation flow directly into the raw denylist storage layer.

Reason:

- frontier liveness and deny validity are different questions
- deny validity should not flap just because some chain is temporarily lagging

#### Misses At or Behind Accepted State

This is a lower-priority design point.

The first version can keep misses cheap and focus on making hits safe.

If we later find a need for stricter handling in the accepted region, we can add it.

## Why This Is Better Than The Prior Plan

### Denylist Rewind Becomes Optional

Instead of physically rewinding denylist entries, we make them self-invalidating.

That is simpler operationally and reduces coordination burden.

### Repair Is Driven By Accepted Snapshot Drift

We no longer need to phrase the repair logic first as "compare stored L1 hash against canonical L1."

The direct question becomes:

- does the accepted snapshot we stored still match what the chains say at that timestamp?

That is easier to reason about and closer to the actual interop state.

### Frontier Logic Stays Synchronous

The frontier gate remains a clean `wait` mechanism:

- if the next timestamp is not coherent yet, do nothing

This avoids overusing rewind as a response to temporary lack of synchrony.

## Expected Data Model Changes

### Verified State

Extend the verified state from:

- `Timestamp`
- `L1Inclusion`
- `L2Heads`

to also include:

- per-chain `L1Heads`

### Denylist

Change denylist entries from raw hash lists to structured records that store the full invalid `Result`.

That gives us:

- a direct record of the exact invalid frontier that created the denial
- simple deny-hit validation through a dedicated validator that regenerates and compares results
- no need for a separate deny-anchor metadata format in the first version

## Testing Plan

### Keystone Integration Suite

The top-level tests should be integration tests with heterogeneous chains:

- different L2 block numbers
- different block times
- same timestamp can map to different per-chain L2 heads

#### 1. Accepted Snapshot Drift Rewinds Interop State

Story:

- interop has committed accepted snapshots through `T`
- later, the chains report a different snapshot for `T`

Assert:

- interop rewinds one verified timestamp
- corresponding `logsDB` state is rewound
- no new commit occurs in that round

#### 2. Inconsistent Frontier Waits

Story:

- accepted snapshot is still stable
- next timestamp frontier is inconsistent across chains

Assert:

- no commit
- no invalidation
- no rewind
- later retry succeeds once frontier becomes coherent

#### 3. Deny Entry Self-Invalidates

Story:

- a deny entry exists for a payload
- the accepted cross-safe world changes so the entry metadata no longer matches

Assert:

- `IsDenied()` returns `false`
- no explicit denylist rewind was required

#### 4. Fast Miss Above Verified Prefix

Story:

- payload is ahead of the verified prefix
- deny lookup misses

Assert:

- `IsDenied()` returns `false` without expensive revalidation

### Pure Tests

Add pure tests for:

- snapshot equality / mismatch decisions
- frontier consistency predicate
- deny-entry result validation

These should be easy to table-test with synthetic snapshots.

## Phase Outline

### Phase 1: Snapshot Model

- extend `VerifiedResult` to carry `L1Heads`
- add helpers to collect accepted and frontier `VerifiedResult` values
- add the checker abstraction
- add pure comparison helpers

### Phase 2: Accepted-State Repair

- re-check the last accepted result before processing the next timestamp
- scan backward to the newest consistent accepted timestamp
- rewind `verifiedDB` and `logsDB` once

### Phase 3: Frozen Frontier

- freeze the next frontier once
- run the frontier gate
- make `loadLogs` and verification consume the frozen snapshot

### Phase 4: Self-Validating Deny Entries

- extend deny entries to store full invalid `Result` values
- add a deny-entry validator on the supernode side
- make `IsDenied()` call that validator on hits before returning `true`
- add the fast miss-above-verified-prefix path

## Open Questions

1. Should V2 rewind one timestamp at a time, or scan backward to the newest matching timestamp and rewind once?
   Resolved: scan backward to the newest consistent accepted timestamp, then rewind once.

2. What is the exact frontier consistency predicate for the by-number checker?

3. Do we want deny entries to store the full invalid `Result` directly in the denylist, or split that into a separate result store later if storage duplication becomes annoying?

4. For deny hits ahead of the verified prefix, is result regeneration plus comparison enough, or do we want an additional fast namespace check later?

5. Do we want to keep storing `L1Inclusion` explicitly, or derive it from per-chain `L1Heads`?

## Recommendation

Proceed with V2 as the new primary design direction.

The strongest parts of this plan are:

- accepted snapshot re-check before progress
- frozen frontier gate
- self-validating deny entries

The two things that need the most care during review are:

- the precise frontier consistency predicate
- the deny-entry validation path and storage shape
- the read/write boundary between `IsDenied()` and the internal deny-entry validator
