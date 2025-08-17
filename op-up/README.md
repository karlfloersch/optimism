# op-up (external L1 mode)

This mode runs a single L2 (chainId you choose) against an external L1 RPC (e.g., Sepolia). Supervisor v2 embeds an op-node per L2 and exposes /v1/sync_status and an /opnode/{chainId}/ proxy.

## One-time setup

1) Create an env file (ignored by git):

```bash
# Edit op-up/external-l1.env to set:
# L1_PK, OP_L1_RPC, OP_L1_BEACON_RPC, OP_L1_CHAIN_ID, OP_L2_CHAIN_ID
# OP_L2_ROLLUP_PATH, OP_L2_GENESIS_PATH (or use the artifacts dir below)
```

2) Generate artifacts (idempotent):

```bash
# Uses op-deployer, writes to op-up/artifacts/
source op-up/external-l1.env
./op-up/deploy-sepolia.sh 901
# Files: op-up/artifacts/rollup.json, op-up/artifacts/l2_genesis.json
```

3) Point env to artifacts (if not already):

```bash
export OP_L2_ROLLUP_PATH="$PWD/op-up/artifacts/rollup.json"
export OP_L2_GENESIS_PATH="$PWD/op-up/artifacts/l2_genesis.json"
```

## Run

```bash
source op-up/external-l1.env
OP_EXTERNAL_L1=1 OP_SV2_CONFIRM_DEPTH=2 OP_UP_STOP_AFTER=120 go run ./op-up
```

- SV2 HTTP is printed at startup (e.g., `[sv2] http: http://127.0.0.1:PORT`).
- Query sync status: `curl -s $SV2_URL/v1/sync_status | jq`.
- The embedded op-node user RPC is proxied at `$SV2_URL/opnode/$OP_L2_CHAIN_ID/`.

## Logs

- The run prints to stdout by default; you can redirect to `op-up/logs/`.

## Notes

- The batcher uses `L1_PK` to submit calldata to L1.
- `OP_SV2_CONFIRM_DEPTH` allows faster safe advancement in dev runs.
- `op-up/artifacts/` and `op-up/external-l1.env` are ignored by git.
