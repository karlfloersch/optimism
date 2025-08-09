### op-supervisor: Cross-safe layer and op-node interop

Owner: Core Protocol
Status: Draft for discussion
Audience: op-supervisor, op-node maintainers



---

Notes:

Design goals:
- Goal: All existing tests pass
  - Implementation:
    - Remain backwards compatible with existing op-node by making the 'unverified', 'valid', 'invalid' checks optional
- Max code reuse with the supervisor
  - Implementation:
    - Keep existing supervisor package unchanged. Instead, create a new `supervisor-2` package which can be run by the sysgo engine used in devstack. `supervisor-2` will pull in code from supervisor and op-node -- will need to watch out for circular dependencies.



Misc:

### Disable p2p for optimistic deriver?
Inside node.go/init p2p is initialized. This is really not required for the optimistic op-node and ideally can be removed entirely. No need for unsafe block handling.

Source:
```
	if err := n.initRuntimeConfig(ctx, cfg); err != nil { // depends on L2, to signal initial runtime values to
		return fmt.Errorf("failed to init the runtime config: %w", err)
	}
	if err := n.initP2PSigner(ctx, cfg); err != nil {
		return fmt.Errorf("failed to init the P2P signer: %w", err)
	}
	if err := n.initP2P(cfg); err != nil {
		return fmt.Errorf("failed to init the P2P stack: %w", err)
	}
```

### Dependency set handling
In theory, I do not need any dependency set information inside of the op-node anymore other than for unsafe blocks to be ignored by the sequencer. The problem is that if I pass in the dependency set, then I am probably implicitly enabling interop and that would hydrate a lot of annoying code paths.

Things to handle:
* For the PoC I think that I **will not** support the allowlist
* I will need to register a new hardfork in the op-node which deploys the interop contracts, probably `interop2`



### Reorg op-node
I will need to add an API to the op-node which lets me reorg it in the case that the optimistic op-node derived an invalid block
- op-node waits until the interop hardfork activation block is final before allowing any executing messages at all so that we do not need to worry about reorgs during upgrade.


### Sequencer mode handling?
I need to figure out a good way to support sequencing...
- Does the execution client drive the consensus client? If the execution client says something is finalized, the consensus client will sync to that right? I think this is correct with the execption that the safeDB may need to be reset each time.
  - I think, yes
- There might be two options for what to do here:
  1) Run a third op-node which is focused on only the unsafe head
  2) Run the sequencer using the `cross-safe` following op-node. That should just work, behaving the same way the pre-interop nodes behave
- In theory it is possible that instead of bundling both cross-safe and local-safe into the new supervisor, I could consider an approach where I run the op-node outside of the supervisor process. However, the downside with not embedding all of the op-nodes is that the PoC will be significantly less compelling.
- **I think it would be more compelling to avoid that and instead simply embed the op-nodes.


### Implementation Plan
Consider starting the supervisor-v2 implementation with the following:
- Implement an op-node proxy service. It runs the op-node in memory and passes through RPC requests.
- Implement a multi-tenent op-node proxy. It runs multiple op-nodes for multiple ELs and passes through RPC requests to each over different endpoints
  - This gets us execution multi-plexing and should be a feature which is preserved.
  - The configuration of this supervisor node is that you must run one or more op-node per chain in your dep-set. For dep-set of 1, we can still test running multiple op-nodes!
  - Each op-node should be a go routine
- Add the ability to reorg the op-nodes at will
- Solving unverified, valid, invalid
  - Add this to the op-node, if it is unverified then the
  - Todo: we can remove 'unverified' by just not exposing the L1 block data until we've verified
  - The way we can expose valid / invalid is that we can just **send the [batch hash?tx hash?blockhash] that is invalid to the op-node and it will automatically ignore that blockhash if it derives a block with it. Treating it as an invalid block.**
    - TODO: Determine if steady batch derivation (holocene) means that we get rid of the whole batch
      - If it is, then we are GOLDEN and we just invalidate batches
      - If it isn't, then we will need to invalidate single blocks at a time which seems super dumb... I mean I guess what I could do is I could make it so the blocklist is on a batch level and then add logic which adds a new validity condition or something...
  - Test this by just making every `x` blocks invalid
- Integrate the cross safety checks
- Test!


Aug 9 Implementation notes
- Instead of having two op-nodes, I use a single op-node per chain.
- The single op-node per chain tracks the local safe as safe
- The supervisor tracks cross-safety using all of the local safe heads
- If the supervisor realizes that one of the op-node's cross safety does not match that node's local safe, it will shut down the op-node and denylist the blockhash that has invalid initating messages. It will then rewind the op-node to before the denylist block was derived.
- The op-node will boot up, and once it derives the denylist block it will throw it away
  - This does mean that I need to implement the denylist thing but I think that should be pretty easy
- Meme: I can read from solana with a special denylist
- Need to make sure safety is queried from the supervisor instead of el now tho


Updated execution plan
- Create supervisor which spins up op nodes as sub processes
- Add the denylist logic where it rolls back if a deny condition returns true
- Make an initial deny condition be idk reading from an external RPC
- Add cross safety package


---


## Goal

- **Define cross-safe as an additional consensus layer** that validates whether `local-safe` is globally safe across the superchain.
- Provide minimal op-node changes and clear RPC surfaces for cross-safe, preserving current `safe` semantics for users.

## Terms

- **local-safe**: op-node derived safe head from locally confirmed L1 data.
- **cross-safe (safe)**: supervisor-validated head coherent across chains and participants; mapped to external `safe`.
- **finalized**: global finality as per finality rules (L1/AltDA).

## Architecture

- Supervisor maintains a cross-chain validation service that ingests:
  - Local-safe head updates from nodes (stream or poll via RPC)
  - Cross-chain consistency signals (e.g., proposals, attestations, or oracle inputs)
  - L1 finality/finality-like signals (when applicable)

- Supervisor produces a stream of cross-safe updates:
  - API: `CrossSafeHead()` and subscription `OnCrossSafeUpdate(CrossSafe, LocalSafe)`
  - Guarantees: monotonic non-decreasing cross-safe unless explicit cross-layer reorg protocol invoked

- op-node consumption
  - Interop subsystem subscribes to supervisor updates and emits `engine.CrossSafeUpdateEvent{CrossSafe, LocalSafe}`
  - `status.StatusTracker` sets public `SafeL2 = CrossSafeL2`, keeps `LocalSafeL2` for diagnostics

## Minimal change plan

1) RPC interface (supervisor → node)
   - Streaming subscription: push cross-safe updates with the tuple `(cross_safe_ref, local_safe_ref)`.
   - Backfill: on reconnect, provide latest cross-safe and its justification window to let the node validate monotonically.

2) Node event bridge
   - Extend interop subsystem to translate supervisor notifications to `engine.CrossSafeUpdateEvent`.
   - If interop disabled, mirror `local-safe` into `cross-safe` at the bridge.

3) Status and metrics
   - `status.StatusTracker` already maps `SafeL2 = CrossSafeL2` and tracks `LocalSafeL2`; ensure metrics report both.
   - Add divergence metrics: `cross_safe_number - local_safe_number` and time-based lag.

4) Finalization
   - With interop: supervisor defines which L2 blocks can be finalized and notifies node; node emits `PromoteFinalizedEvent` accordingly.
   - Without interop: unchanged; finalizer uses L1 finality and provenance.

## Failure modes and handling

- Supervisor unavailable: cross-safe halts; local-safe continues advancing. Public `safe` stalls (by design). Alerting on sustained divergence.
- Conflicting cross-safe proposals: follow supervisor conflict resolution; node remains a consumer.
- Reconnect/reorg: supervisor provides latest cross-safe and justification provenance for monotonic catch-up.

## Test plan

- Contract tests for supervisor → node subscription reconnection and monotonicity guarantees.
- End-to-end with interop off: mirror behavior with cross-safe == local-safe.
- End-to-end with interop on: induce local-safe advancement while holding cross-safe; verify RPC presents stalled `safe` and advancing `local_safe`.
- Finalization integration: verify promotion only after cross-safe allows and finality rules are satisfied.

## Rollout

- Feature-flag driven by interop config. No config migration for users; public `safe` semantics unchanged, now denoting cross-safe where available.


