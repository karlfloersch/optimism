## Safe-Blocks Poller Main Loop (Design)

This component advances the local safe/finalized labels from an external L2 RPC.

Inputs:
- Endpoint: `--safe-blocks-rpc` (also `OP_NODE_SAFE_BLOCKS_RPC`)
- Poll interval: `--safe-blocks-rpc-poll-interval`

Responsibilities:
- Read external safe/finalized heads periodically
- Apply finalized first, then safe
- Advance safe head strictly along parent links; on mismatch walk backwards to find a connecting ancestor

Main loop:
1. Every `Interval`:
   - Fetch `finalizedHeadNum` using `eth_getBlockByNumber("finalized", false)`
   - Fetch `safeHeadNum` using `eth_getBlockByNumber("safe", false)`
   - If present, apply finalized by number
   - If present, advance safe to `safeHeadNum`

Advance safe to N:
- Let `local` be the current local safe head
- While `local.Number < N`:
  - Fetch `b = eth_getBlockByNumber(local.Number+1)`
  - If `b.ParentHash == local.Hash`: apply `b` as safe
  - Else (mismatch): walk backwards from `b.Number-1` until a block `pb` with `pb.ParentHash == local.Hash` is found
    - If found: set `b = pb` and apply as safe (repeat)
    - If none found before reaching `local.Number`: stop (wait for next tick)

Applying labels:
- Finalized: `SetFinalizedHead(ext)` then `TryUpdateEngine`
- Safe: set both local-safe and cross-safe to `ext`, then `TryUpdateEngine`

Notes:
- The loop intentionally stops on data gaps or mismatches and waits for the next tick.
- Unsafe behavior is unchanged; only safe/finalized labels are adjusted.

