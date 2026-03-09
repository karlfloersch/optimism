# Strong Consistency

This directory contains the acceptance coverage for the supernode's strong-consistency behavior.

## Goal

The supernode should only expose cross-validated state that is consistent with the current canonical L1-derived world of the managed L2s.

In practice, that means:

- if an already-accepted interop timestamp becomes stale after an L1/L2 reorg, the supernode must rewind to the newest still-valid accepted prefix
- if the next candidate timestamp is inconsistent across chains, the supernode must wait rather than advancing
- stale denylist decisions from discarded state must not survive and poison later derivation

## Intuition

The supernode is not trying to make the managed op-nodes perfectly synchronized at every instant.

Instead, it maintains its own validated view **over** the op-nodes:

- it reads a snapshot of what each chain currently thinks is safe
- it decides whether that snapshot is a world it trusts
- it only exposes / commits snapshots that survive those checks
- on every loop, it re-checks the latest accepted snapshot against the current underlying chain state

That gives the system an eventually consistent shape:

- if the op-nodes reorg while interop is running, the next loop sees the drift
- if the drift changed already-accepted state, interop rewinds its accepted prefix
- if the drift only changed the next candidate frontier, interop waits and retries
- if a stale deny decision previously caused a chain rewrite, repairing or pruning that deny decision also unwinds that write

So the core idea is:

- the supernode keeps a validated snapshot view over the chains
- the chains may move underneath it
- but every time interop re-enters the loop, it re-proves the latest accepted view before building on top of it

## Mental Model

```text
underlying op-nodes
    |
    |  current local-safe / optimistic state
    v
collect snapshot
    |
    +--> does latest accepted snapshot still match?
    |       |
    |       +--> no: rewind accepted prefix, logs, and stale deny decisions
    |       |
    |       +--> yes: continue
    |
    +--> is next frontier coherent and canonical?
            |
            +--> no: wait
            |
            +--> yes: verify messages / cycles
                        |
                        +--> valid: commit new accepted snapshot
                        |
                        +--> invalid: deny bad payloads, possibly rewind affected chain
```

The important asymmetry is:

- **reads** are from the supernode's validated snapshot view
- **writes** to chains only happen when invalidation causes a rewind/replacement

Because that write path is narrow, the supernode can also unwind it when the accepted world changes.

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
- denylist state for discarded timestamps

On startup, interop also scrubs persisted state back to the accepted prefix before entering the main loop. This prevents a crash from leaving `logsDB` or stale deny decisions ahead of `verifiedDB`.

### 2. Frozen Frontier Gate

For the next unverified timestamp, interop freezes the candidate frontier once and uses that same frontier for the whole round.

It then checks:

- the frozen snapshot is internally coherent
- the per-chain L1 heads are canonical under the current checker
- the candidate extends the accepted prefix monotonically

If the frontier is inconsistent, interop returns `wait` and retries later. It does not commit and does not repair accepted state just because the future is temporarily messy.

### 3. Denylist Cleanup On Repair Or Frontier Drift

Interop invalidation still uses the denylist to block bad payloads.

Each deny entry is stored under `(height, payloadHash)`, and stale deny decisions are removed when interop proves they belong to a world that is no longer valid.

That cleanup happens in two places:

- on accepted-state repair, deny entries after the kept timestamp are pruned, and chains affected by those pruned entries are rewound to the kept prefix
- when retrying the same frontier timestamp, deny entries whose stored frontier L1 world no longer matches the current frontier are pruned before re-evaluation

This keeps stale deny decisions from surviving a reorged world.

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
