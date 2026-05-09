# Combined op-supernode + op-reth image

This directory contains an experimental Docker image that runs:

- one `op-supernode` process
- one `op-reth` execution client for OP Mainnet
- one `op-reth` execution client for Unichain mainnet

The goal is to make a supernode deployment look as much as possible like a
normal `op-node` plus execution-layer deployment to downstream operators. The
operator still gets normal L2 execution RPC endpoints, while `op-supernode`
replaces the per-chain `op-node` process and exposes per-chain consensus-node
RPCs under chain-specific paths.

## Build

Build the full image from the monorepo root:

```bash
docker build \
  -f ops/docker/combined/Dockerfile \
  --build-arg GIT_COMMIT="$(git rev-parse --short=10 HEAD)" \
  --build-arg GIT_DATE="$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
  -t op-combined:local .
```

The full build compiles both `op-supernode` and `op-reth`. The
`Dockerfile.supernode-only` helper is only for operators that already have a
combined base image and want to replace the `op-supernode` binary without
rebuilding `op-reth`.

## Runtime layout

The image starts all processes under `supervisord`.

| Component | Purpose | Port |
| --- | --- | --- |
| chain A `op-reth` HTTP RPC | OP Mainnet execution RPC | `8545` |
| chain A `op-reth` Engine API | OP Mainnet engine API, internal to the container | `8551` |
| chain A `op-reth` metrics | OP Mainnet EL metrics | `9001` |
| chain B `op-reth` HTTP RPC | Unichain execution RPC | `8546` |
| chain B `op-reth` Engine API | Unichain engine API, internal to the container | `8552` |
| chain B `op-reth` metrics | Unichain EL metrics | `9002` |
| `op-supernode` RPC | Namespaced consensus-node RPCs | `9545` |
| `op-supernode` metrics | Supernode metrics | `9003` |

The supernode RPC is namespaced by chain ID. For example:

- OP Mainnet: `http://localhost:9545/10`
- Unichain mainnet: `http://localhost:9545/130`

If an existing integration only talks to the execution RPC, it can keep using
the same JSON-RPC API by pointing to the relevant execution RPC port. For an OP
Mainnet replacement, point it at `:8545`. For a Unichain replacement, point it
at `:8546`. The other chain can be ignored by that integration, even though it
is available inside the same supernode deployment.

## Required storage and machine sizing

Run this on fast persistent SSD or NVMe-backed storage. Do not run the
execution databases on network filesystems with high latency.

As a baseline, size the host as at least the sum of two production L2 execution
nodes plus headroom for the shared supernode state:

- CPU: at least 8 modern cores
- Memory: at least 32 GiB RAM
- Disk: fast SSD/NVMe, with separate persistent directories for OP Mainnet and
  Unichain
- Network: stable low-latency connectivity to an L1 execution RPC, an L1 beacon
  API, and L2 peers

OP Mainnet public operator guidance has historically recommended at least
16 GiB RAM and a modern CPU for a full node, with full-node storage in the
hundreds of GiB and growing over time. Archive nodes require many TiB. Unichain
mainnet has one-second blocks, so treat it as its own production L2 execution
node and provision storage independently from OP Mainnet.

For a combined OP Mainnet plus Unichain mainnet supernode, start with at least
2 TiB of fast usable storage for full-node sync experiments, and leave enough
room to grow or resnapshot without emergency maintenance. Operators who intend
to retain more history, run archive mode, or keep multiple snapshots should
provision substantially more.

## Validation history

As of May 2026, this combined deployment pattern has been exercised in an
internal validation environment for roughly three weeks of normal supernode
operation, including several days of active interop verifier activity. During
that validation, the node has successfully:

- run OP Mainnet and Unichain mainnet together in one supernode deployment
- tracked `latest`, `safe`, and `finalized` heads at tip for both chains
- restarted cleanly after image updates
- backed up and restored execution-layer database storage without resyncing
  from genesis

This is not a substitute for operator monitoring, but it is useful operational
evidence for teams evaluating whether the combined image can replace a
single-chain `op-node` plus execution-layer setup.

## Environment variables

Required:

| Variable | Description |
| --- | --- |
| `L1_RPC` | L1 execution RPC URL shared by all virtual nodes |
| `L1_BEACON` | L1 beacon API URL shared by all virtual nodes |
| `CHAIN_A_ID` | First chain ID, usually `10` for OP Mainnet |
| `CHAIN_B_ID` | Second chain ID, usually `130` for Unichain mainnet |

Common optional variables:

| Variable | Description |
| --- | --- |
| `JWT_SECRET_PATH` | Path to the Engine API JWT secret inside the container. Defaults to `/jwt.hex` |
| `CHAIN_A_NETWORK` | Network name for chain A, for example `op-mainnet` |
| `CHAIN_B_NETWORK` | Network name for chain B, for example `unichain-mainnet` |
| `CHAIN_A_ROLLUP_CONFIG` | Rollup config file path for chain A, used instead of `CHAIN_A_NETWORK` |
| `CHAIN_B_ROLLUP_CONFIG` | Rollup config file path for chain B, used instead of `CHAIN_B_NETWORK` |
| `CHAIN_A_SEQUENCER_HTTP` | Optional sequencer HTTP endpoint passed to chain A `op-reth` |
| `CHAIN_B_SEQUENCER_HTTP` | Optional sequencer HTTP endpoint passed to chain B `op-reth` |
| `CHAIN_A_EXTRA_RETH_ARGS` | Additional args appended to chain A `op-reth` |
| `CHAIN_B_EXTRA_RETH_ARGS` | Additional args appended to chain B `op-reth` |
| `SUPERNODE_EXTRA_ARGS` | Additional args appended to `op-supernode` |

## Example `docker compose`

This example preserves the usual OP Mainnet execution RPC on host port `8545`
and exposes the Unichain execution RPC on host port `8546`.

```yaml
services:
  supernode:
    image: op-combined:local
    restart: unless-stopped
    environment:
      L1_RPC: "${L1_RPC}"
      L1_BEACON: "${L1_BEACON}"
      CHAIN_A_ID: "10"
      CHAIN_A_NETWORK: "op-mainnet"
      CHAIN_B_ID: "130"
      CHAIN_B_NETWORK: "unichain-mainnet"
      JWT_SECRET_PATH: "/jwt.hex"
      SUPERNODE_EXTRA_ARGS: >-
        --vn.10.syncmode=execution-layer
        --vn.130.syncmode=execution-layer
        --vn.all.syncmode.req-resp
    volumes:
      - ./jwt.hex:/jwt.hex:ro
      - op-mainnet-data:/data/chain-a
      - unichain-mainnet-data:/data/chain-b
    ports:
      - "8545:8545"
      - "8546:8546"
      - "9545:9545"
      - "30303:30303/tcp"
      - "30303:30303/udp"
      - "30304:30304/tcp"
      - "30304:30304/udp"

volumes:
  op-mainnet-data:
  unichain-mainnet-data:
```

Keep the L1 RPC URLs and JWT secret out of committed compose files. Pass them
through a local `.env` file, a secret manager, or your orchestrator's secret
mechanism.

## Migration from a standard `op-node` plus execution client

This migration assumes the operator currently runs one OP Stack chain with a
normal consensus client and execution client, possibly bundled into one image.
The goal is to change the runtime wiring while preserving the externally
visible APIs used by existing monitoring, indexers, and applications.

1. Record the existing API contract.

   Note which host ports and URLs downstream services use today. The most
   common requirement is preserving the L2 execution JSON-RPC URL, such as
   `http://node.example:8545`.

2. Decide which chain is the primary replacement.

   For an OP Mainnet operator, use chain A as the replacement and keep OP
   Mainnet on `:8545`. For a Unichain operator, either expose chain B on the
   old port or swap the chain A and chain B configuration in your compose file.

3. Prepare persistent storage.

   Mount separate persistent directories for each execution database:
   `/data/chain-a` and `/data/chain-b`. Do not place both chains in the same
   datadir. Keep the JWT secret persistent and mounted read-only.

   If you already operate `op-reth` for one or both chains, you can attach the
   combined image to those existing `op-reth` databases instead of syncing from
   genesis. Stop the old `op-node` and `op-reth` services cleanly, mount the OP
   Mainnet `op-reth` datadir at `/data/chain-a`, and mount the Unichain
   `op-reth` datadir at `/data/chain-b`. The datadir must have been created for
   the same network and execution client version family you intend to run.

   Do not point two live execution clients at the same database. If you want a
   rollback path, snapshot or copy the datadir first, then start the combined
   image against the copy or against the original after the old service is fully
   stopped.

4. Configure L1 access once.

   Set `L1_RPC` and `L1_BEACON` at the supernode level. `op-supernode` shares
   these L1 resources across virtual nodes, so per-chain `op-node` L1 flags are
   not needed.

5. Map old `op-node` flags to virtual-node flags.

   Most per-chain `op-node` flags become `--vn.<chainID>.<flag>`. A flag that
   should apply to every virtual node can use `--vn.all.<flag>`.

   Examples:

   ```text
   old: --syncmode=execution-layer
   new: --vn.10.syncmode=execution-layer

   old: --l2.jwt-secret=/jwt.hex
   new: --vn.10.l2.jwt-secret=/jwt.hex

   old: --p2p.listen.tcp=9003
   new: --vn.10.p2p.listen.tcp=9003
   ```

   The combined image sets the Engine API wiring for both bundled execution
   clients automatically.

6. Preserve the old execution RPC URL.

   If downstream services expect the OP Mainnet execution RPC at `:8545`, map
   host `8545` to container `8545`. If they expect Unichain on that same old
   port, map host `8545` to container `8546` instead.

7. Update consensus-node RPC consumers.

   Consumers that used a normal `op-node` RPC should point to the namespaced
   supernode path for the relevant chain:

   ```text
   OP Mainnet: http://host:9545/10
   Unichain:  http://host:9545/130
   ```

   Execution-layer consumers do not need this path unless they explicitly used
   `op-node` RPC methods.

8. Start the combined image and watch catch-up.

   Compare local `latest`, `safe`, and `finalized` heads against trusted public
   RPCs or your existing node:

   ```bash
   curl -s -X POST http://localhost:8545 \
     -H 'content-type: application/json' \
     --data '{"jsonrpc":"2.0","id":1,"method":"eth_getBlockByNumber","params":["safe",false]}'

   curl -s -X POST http://localhost:9545/10 \
     -H 'content-type: application/json' \
     --data '{"jsonrpc":"2.0","id":1,"method":"optimism_syncStatus","params":[]}'
   ```

9. Cut traffic over only after the primary chain is at tip.

   Wait until `latest`, `safe`, and `finalized` are progressing normally. Small
   differences in `safe` or `finalized` versus another public endpoint can be
   normal around L1 update boundaries, but sustained lag should be investigated
   before cutover.

10. Keep rollback simple.

    Keep the old node stopped but available until the combined image has held
    tip for long enough to satisfy your operational policy. Rollback should be
    a port or service target change, not an emergency resync.

## Notes and caveats

- This image runs two execution clients. Storage and IO pressure are materially
  higher than a single-chain node.
- `op-supernode` replaces the `op-node` process, not the execution-layer RPC
  API. Existing execution RPC consumers should continue to talk to the bundled
  `op-reth` endpoint for their chain.
- The second chain can be ignored by existing single-chain consumers, but it
  still requires CPU, memory, network, and disk.
- Keep secrets, private RPC URLs, cloud project names, and private registry
  paths out of images and committed compose files.
