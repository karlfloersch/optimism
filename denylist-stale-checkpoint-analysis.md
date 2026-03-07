# Denylist Stale-Checkpoint Analysis

## Question

Can a stale accepted checkpoint cause the supernode to apply a denylist entry that is no longer valid after an L1/L2 reorg, and if so, is that only a temporary liveness issue or can it permanently diverge the node?

## Current Design

Today the relevant pieces are:

- `verifiedDB` stores accepted interop snapshots (`VerifiedResult`).
- `validated` tracks the highest accepted timestamp we currently trust.
- denylist entries persist, but on a deny hit we do not trust raw storage alone.
- instead, `ValidateDeniedEntry` checks the deny entry against the currently accepted checkpoint.

That means the denylist is logically guarded by the accepted checkpoint, not physically rewound.

## Failure Mode

The dangerous case is:

1. Accepted checkpoint `C` is still stored and marked validated.
2. A deny entry `D` was created under `C`.
3. An L1 reorg happens.
4. The chain starts moving to the new world, but interop has not run repair yet.
5. `C` is now stale, but still looks accepted locally.
6. A payload `X` arrives that was invalid under `C` but is valid in the new world.
7. `IsDenied(height(X), hash(X))` hits `D`.
8. `ValidateDeniedEntry` validates `D` against stale checkpoint `C`.
9. `IsDenied` returns `true`.
10. op-node rejects `X` and requests a deposits-only replacement block.
11. The replacement block is built and force-reset into the chain.

## Why This Can Be Permanent

This is not just a temporary false positive.

If the replacement block is installed:

- the original valid block `X` may never be reintroduced,
- later interop repair may disable the stale deny entry,
- but that does not automatically restore `X`,
- and another node that accepted `X` may continue on a different branch.

So a stale-checkpoint false deny can permanently push this node onto a different local-safe / cross-safe history.

## Comparison With a False Negative

The opposite mistake is:

- a deny miss when a block should have been denied.

That is usually recoverable:

- the block gets inserted,
- interop later evaluates it,
- if it is actually invalid, the existing invalidation / reset machinery can remove it.

So the two error modes are not symmetric:

- false positive deny: can permanently force a wrong replacement path
- false negative deny: usually recoverable later by interop

Given that asymmetry, the safer bias is:

- never return `true` from `IsDenied` unless we are confident the checkpoint behind that deny entry is still live-valid
- on uncertainty, prefer `false`

## Recommended Rule

Use this policy:

1. If there is no accepted validated checkpoint and the denylist lookup is a miss:
   return `false` immediately.

2. Otherwise, before trusting either a deny hit or a deny miss, revalidate the currently accepted checkpoint live:
   - re-collect a fresh snapshot for the validated timestamp
   - compare it to the stored accepted snapshot using the consistency checker

3. If that live revalidation fails or the checkpoint is stale:
   - do not trust the deny result
   - return `false`
   - trigger or allow accepted-state repair to run

4. Only if the accepted checkpoint is still live-valid:
   - evaluate deny entries
   - on a deny hit, validate the entry against that checkpoint
   - on a deny miss, return `false`

This is deliberately fail-open on deny uncertainty.

## Why Recheck On Miss Too

Rechecking only on deny hits still leaves an awkward split:

- deny hit: "I need to prove the checkpoint is fresh"
- deny miss: "I assume the checkpoint is fresh"

If the accepted checkpoint is stale, both answers are suspect. Rechecking on both paths is simpler and easier to reason about.

The one good fast path is:

- no accepted validated checkpoint exists
- denylist lookup is a miss

In that case there is no accepted world to protect yet, so we can safely return `false` without extra work.

## Tradeoff

This makes `IsDenied` more expensive because it may trigger live snapshot validation even on misses after cross-validation has started.

That cost is acceptable if the priority is eventual consistency and avoiding false positive deny decisions that can permanently alter the local chain.

## Recommendation

The robust version is:

- keep deny entries persistent
- do not physically rewind the denylist by default
- but never trust a deny result unless the current accepted checkpoint has been revalidated live
- recheck on both hits and misses once a validated checkpoint exists
- keep the single fast path:
- no validated checkpoint
- deny miss
- immediate `false`

This is the smallest design change that closes the stale-checkpoint false-deny hole without redesigning the whole interop / denylist model.

## Chosen Rule

The implemented rule is:

1. `IsDenied` looks up the raw denylist entry versions.
2. If there is no deny entry and there is no validated checkpoint:
   return `false` immediately.
3. Otherwise, live-revalidate the currently accepted checkpoint.
4. If that checkpoint is stale or the revalidation fails:
   return `false`.
5. Only if the checkpoint is still live-valid do we trust deny entries.

This means we now recheck the accepted checkpoint on both deny hits and deny misses once cross-validation has begun.
