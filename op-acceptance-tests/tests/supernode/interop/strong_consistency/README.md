# Strong Consistency

This directory contains the acceptance coverage for the supernode's strong-consistency behavior.

## Goal

The supernode should only expose cross-validated state that is consistent with the current canonical L1-derived world of the managed L2s.

In practice, that means:

- if an already-accepted interop timestamp becomes stale after an L1/L2 reorg, the supernode must rewind to the newest still-valid accepted prefix
- if the next candidate timestamp is inconsistent across chains, the supernode must wait rather than advancing
- stale denylist decisions from discarded state must not survive and poison later derivation

## How It Works

The implementation uses three main mechanisms.

### 1. Accepted Snapshot Repair

Each accepted interop timestamp is treated as a snapshot:

- `Timestamp`
- per-chain `L2Heads`
- per-chain `L1Heads`
- `L1Inclusion`

Before processing the next timestamp, interop re-collects the latest accepted snapshot and compares it to what is stored in `verifiedDB`.

- If it still matches, interop continues.
- If it does not match, interop scans backward to find the newest accepted timestamp that is still valid.
- It then rewinds once to that kept prefix.

That rewind updates:

- `verifiedDB`
- per-chain `logsDB`
- the validated boundary used by read-side queries

### 2. Frozen Frontier Gate

For the next unverified timestamp, interop freezes the candidate frontier once and uses that same frontier for the whole round.

It then checks:

- the frozen snapshot is internally coherent
- the per-chain L1 heads are canonical under the current checker
- the candidate extends the accepted prefix monotonically

If the frontier is inconsistent, interop returns `wait` and retries later. It does not commit and does not repair accepted state just because the future is temporarily messy.

### 3. Denylist Cleanup On Repair Or Frontier Drift

Interop invalidation still uses the denylist to block bad payloads, but stale deny decisions are cleaned up in two places:

- on accepted-state repair, deny entries after the kept timestamp are pruned, and chains affected by those pruned entries are rewound
- when retrying the same frontier timestamp, deny entries whose stored frontier snapshot no longer matches the current frontier are pruned before re-evaluation

This prevents stale deny decisions from surviving a reorged world.

## Serialization

Interop mutations are effectively single-writer.

Chain resets are queued, and the main interop loop applies them serially with:

- accepted-state repair
- denylist reconciliation
- verified-result commits

That keeps `verifiedDB`, `logsDB`, and the validated boundary aligned.

## Snapshot Coherence

Snapshot collection now checks that:

- the L2 head returned by `OptimisticAt(timestamp)`
- matches the frozen `LocalSafeBlockAtTimestamp(timestamp)` head

If those diverge during collection, interop treats the snapshot as moved and waits rather than proving a mixed world.

## What The Acceptance Test Proves

`TestSupernodeStrongConsistency_L1Reorg_RepairsAndRecovers` covers the main recovery story:

1. a timestamp is accepted
2. an L1 reorg invalidates that accepted world
3. the accepted timestamp disappears while interop repairs
4. the timestamp is revalidated against a different accepted L1 dependency
5. once interop resumes, forward progress continues normally

This is the keystone test for the strong-consistency path.
