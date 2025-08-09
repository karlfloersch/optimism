# Supervisor v2 Design Document

Note: I am writing this as a draft and may contain confusing or incomplete information. Please ask questions and then correct / clarify the information on this page before proceeding with implementation.

## Goal
Refactor the supervisor so that it does not require significant modifications to the op-node. Pre-interop hard fork op-node API should be sufficient for integrating this Supervisor v2 and thereby enabling interop.

## Architecture
supervisor-v2 creates op-node subprocesses for each chain in the dependency set (note: these op-nodes DO NOT use the interop hardfork, they should be pectra op-nodes, no dependency set information should be sent to the op-node).

supervisor-v2 puts all of the safe blocks which are being synced by the op-nodes into the `local-safe` db. The cross-safe validation is then run on the latest `local-safe` block at the same block height (just like the normal op-supervisor). The `cross-safe` block is calculated. This block may either be a) valid; or b) invalid.

If valid, then the op-supervisor just continues. If invalid, then the supervisor-v2 will:
1. Stop the op-node for each of the chains effected
2. Roll back the execution layer to the block **before** the first invalid block
3. Add the payloadId which was **invalid** to the `payloadId denylist` (maybe blockhash denylist?). This payloadId denylist is a list of payloadId which are INVALID and therefore should trigger an error in derivation. This is as if that block contains an invalid transaction - same type of thing. It will trigger the 'steady derivation' logic and discard the batch if the op-node were to sync it.
4. Rollback the execution client's safe and unsafe heads to the L1 block before the invalid block detected by the cross-safe handling logic
5. Restart the op-node with a new `safe db` (so that we make sure we wipe the safe db as well)
6. When the op-node derives the block which was detected as INVALID, it will look up whether that payloadId is in the blocklist for that blockheight (this is new behavior). It will see that indeed the payloadId IS in the denylist and trigger the error handling logic for a malformed block (using steady derivation)

This means that the op-node will start syncing again but this time it will skip over the block which is invalid based on the cross-safety checks. This is all that is required to support interop and doesn't require significant changes to the op-node!


Implementation plan:

Note: Please work with me to come up with implementation plans for each milestone. Add a checklist of things that we need to do for each of these milestones. Also ensure that at all times:
1. We don't break existing tests
2. We unit test all of our work
3. We integration test / e2e test all of our work

Please commit changes after each milestone (can do even more frequently but at minimum after milestones) and ensure that before committing all tests pass.

### 1. Testing Setup

Enable fast testing feedback by integrating op-supervisor-v2 with devstack and sysgo immediately. It should be possible to:

1. Run op-supervisor-v2 in the devstack with some basic tests
2. Run op-supervisor-v2 as a standalone system with sysgo

Both of these services would of course be hooked up with an execution client (geth).

Implementation plan
- [ ] Create op-supervisor-v2 which just creates a subprocess that is a single op-node.
- [ ] ...[todo] please fill out the next steps. Should we integrate into devstack first? Or sysgo? Or other? What are the options?


### 2. Introduce op-node rollbacks triggered by op-supervisor-v2

Show in a test that it is possible to trigger the rollback logic in the op-supervisor-v2 using a devstack test

### 3. Add denylist

Create a payloadId (or blockhash) denylist BUT don't introduce any interop logic yet. Instead, we just say that 1 in every 10 blocks should be added to the denylist. This way we can test out the integration of our op-node rollback logic

We should show this working both in the devstack tests as well as in the sysgo system that we spin up

### 4. Add a second op-node and second execution client

We've got the core logic working for one chain, to make this interesting we want to integrate the cross-safe package and that works best with two chains.

Add this new two chain setup to the devstack and create a simple test making it work. Also ensure it's added to our sysgo setup.

### 5. Create a new hardfork (interop2) which deploys the pre-deploys

Because we are NOT using the interop hardfork (it introduces too much complexity into the op-node), we will still need to deploy the pre-deploys. For this we will introduce a new hardfork which is interop2 that deploys the same predeploys as the normal interop system. This can be done with a `if interop OR interop2` in the pre-deploy setup bit.

### 6. Integrate cross safe

This is where we integrate cross safe by populating the cross-db with all of our block data / event data. We will want to test this similarly to how interop is tested currently - creating valid and invalid executing messages which are validated or trigger reorgs.

Once we've done this we are done!




Please ask clarifying questions!
