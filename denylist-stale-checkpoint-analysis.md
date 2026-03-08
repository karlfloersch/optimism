# Denylist Repair Analysis

## Problem

Anchoring denylist lookups to the currently accepted checkpoint was hard to reason about under reorgs.

The failure mode was:

1. A deny entry is created for an invalid frontier.
2. L1/L2 reorgs before interop repairs the accepted checkpoint.
3. The stale checkpoint still makes the deny entry look valid.
4. op-node rejects a block that is now actually valid and may install a replacement block instead.

That replacement can persist even after interop later realizes the checkpoint was stale.

## Pivot

The simpler eventual-consistency design is:

- keep denylist lookup dumb on the hot path
- prune deny entries explicitly when accepted-state repair rewinds the interop prefix
- reset any chain whose denylist lost entries, so any stale replacement blocks are rolled back too

This makes the denylist part of the repaired suffix, instead of trying to prove old deny entries live at lookup time.

## Repair Rule

When accepted-state repair finds the new kept timestamp `keepTS`:

1. Rewind `verifiedDB` to `keepTS`.
2. Rewind each `logsDB` to the kept per-chain `L2Head`.
3. Prune each chain's denylist entries whose `Result.Timestamp > keepTS`.
4. For each chain that had entries pruned, rewind the chain engine to `keepTS`.

If no accepted prefix remains:

1. Rewind `verifiedDB` to empty.
2. Clear all `logsDB`s.
3. Prune all deny entries created at or after interop activation.
4. Rewind affected chains to just before activation.

## Why This Is Easier To Reason About

- stale deny entries from the discarded suffix are removed directly
- if one of those entries already caused a replacement block, the chain reset undoes it
- deny lookups no longer depend on checkpoint freshness or live validator RPCs
- eventual consistency is driven by one repair boundary: the kept interop timestamp

## Implementation Notes

- deny entries still store the invalid `Result`, but now only so repair can recover the decision timestamp
- denylist lookup is back to raw `(height, payloadHash)` membership
- accepted-state repair uses `PruneDenyListAfter(keepTS)` on each chain
- repair-triggered chain rewinds use `RewindEngine(..., eth.BlockRef{})`; the zero block acts as a sentinel so interop ignores the callback because it already applied the local rewind itself

## Testing Focus

The keystone behavior to preserve is:

1. accept through timestamp `T`
2. create deny decisions on the discarded suffix
3. reorg so the accepted world rewinds to `keepTS < T`
4. prune deny entries with decision timestamp `> keepTS`
5. reset affected chains
6. rebuild from the kept prefix and converge again

The most important unit checks are:

- pruning removes only entries after `keepTS`
- accepted-state repair rewinds logs, verified state, and affected chains together
- repair-reset callbacks do not double-apply interop rewinds
