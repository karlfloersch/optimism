# op-up (external L1 mode)

This mode runs an L2 (chainId you choose) against an external L1 RPC (e.g., Sepolia). You can deploy one or multiple chains by calling the deployer once per chain. Supervisor v2 embeds an op-node per L2 and exposes /v1/sync_status and an /opnode/{chainId}/ proxy.

## One-time setup

1) Create an env file (ignored by git):

```bash
# Edit op-up/external-l1.env to set:
# DEPLOYER_PK, BATCHER_PK, OP_L1_RPC, OP_L1_BEACON_RPC, OP_L1_CHAIN_ID, OP_L2_CHAIN_ID
# OP_L2_ROLLUP_PATH, OP_L2_GENESIS_PATH (or use the artifacts dir below)
```

2) Generate artifacts (idempotent):

```bash
# Preferred: use just (keeps commands consistent)
just -f op-up/justfile deploy 901

# Single-chain files: op-up/artifacts/rollup.json, op-up/artifacts/l2_genesis.json (written via stdout redirection)
```

Notes about artifacts and template:
- The deploy script seeds `WORKDIR/intent.toml` from the committed template at `op-up/deploy-sepolia/intent.toml` (WORKDIR is `op-up/artifacts/` for single-chain, or `op-up/artifacts/chain-<id>/` for multi-chain).
- It expands `file://__ROOT__` to the repo root and rewrites every `0xBATCHER` placeholder to the address derived from `BATCHER_PK_<CHAIN_ID>` if set, otherwise `BATCHER_PK`.
- You can delete `op-up/artifacts/` any time; the script will regenerate the intent before inspecting/applying.

End-to-end (clean → deploy → run):
```bash
just -f op-up/justfile up 901 120
```

## Pre-flight checks (important)

- Ensure the configured batcher matches your batcher key(s) and is funded on Sepolia; otherwise L2 safe will not advance.

```bash
# 1) Confirm artifact paths are used (avoid hard-coded non-existent paths)
# Single-chain
export OP_L2_ROLLUP_PATH="$PWD/op-up/artifacts/rollup.json"
export OP_L2_GENESIS_PATH="$PWD/op-up/artifacts/l2_genesis.json"

# Multi-chain (example chain 901)
# export OP_L2_ROLLUP_PATH="$PWD/op-up/artifacts/chain-901/rollup.json"
# export OP_L2_GENESIS_PATH="$PWD/op-up/artifacts/chain-901/l2_genesis.json"

# 2) Derive the BATCHER_PK address and compare with configured batcher
echo "BATCHER_PK addr: $(cast wallet address --private-key $BATCHER_PK)"
echo "configured batcher: $(jq -r '.genesis.system_config.batcherAddr' "$OP_L2_ROLLUP_PATH")"

# If using per-chain keys (e.g., 901/902):
# echo "BATCHER_PK_901 addr: $(cast wallet address --private-key ${BATCHER_PK_901})"
# echo "BATCHER_PK_902 addr: $(cast wallet address --private-key ${BATCHER_PK_902})"

# If they differ, edit your intent to set the batcher to your BATCHER_PK address, then re-run step (2):
#   op-up/deploy-sepolia/intent.toml  ->  [chains.roles].batcher = "<BATCHER_PK addr>"

# 3) Fund the batcher on Sepolia so it can post calldata
cast balance $(cast wallet address --private-key $BATCHER_PK) --rpc-url $OP_L1_RPC --ether
```

3) Point env to artifacts (if not already):

```bash
export OP_L2_ROLLUP_PATH="$PWD/op-up/artifacts/rollup.json"
export OP_L2_GENESIS_PATH="$PWD/op-up/artifacts/l2_genesis.json"
```

## Run

```bash
# Preferred: use just (single-chain with default `op-up/artifacts/`)
just -f op-up/justfile run 120

# Multi-chain: point to the desired chain's artifacts before running
# Chain 901
# OP_L2_ROLLUP_PATH=$PWD/op-up/artifacts/chain-901/rollup.json \
# OP_L2_GENESIS_PATH=$PWD/op-up/artifacts/chain-901/l2_genesis.json \
# OP_L2_CHAIN_ID=901 \
# just -f op-up/justfile run 120

# Chain 902
# OP_L2_ROLLUP_PATH=$PWD/op-up/artifacts/chain-902/rollup.json \
# OP_L2_GENESIS_PATH=$PWD/op-up/artifacts/chain-902/l2_genesis.json \
# OP_L2_CHAIN_ID=902 \
# just -f op-up/justfile run 120
```

- SV2 HTTP is printed at startup (e.g., `[sv2] http: http://127.0.0.1:PORT`).
- Query sync status: `curl -s $SV2_URL/v1/sync_status | jq`.
- The embedded op-node user RPC is proxied at `$SV2_URL/opnode/$OP_L2_CHAIN_ID/`.

## Multi-chain deployment quickstart

```bash
# Deploy two chains (901 and 902) into separate artifact dirs
. op-up/external-l1.env
just -f op-up/justfile deploy2 901 902

# Optional: per-chain batcher keys (recommended)
# export BATCHER_PK_901="$BATCHER_PK"     # uses default key
# export BATCHER_PK_902="0x<your-second-key>"

# Query OptimismPortal (L1) addresses via inspect (example for 901)
L2_HEX=$(printf "0x%064x" 901)
go run op-deployer/cmd/op-deployer inspect l1 --workdir op-up/artifacts/chain-901 "$L2_HEX" | jq -r '.OptimismPortalProxy'
```

## Quick health checks

```bash
# Discover SV2 URL from logs
SV2_URL=$(grep -a "[sv2] http:" op-up/logs/op-up.latest.log | tail -n1 | awk '{print $3}')
echo "$SV2_URL"

# SV2 sync snapshot (L1/L2 heads)
curl -s "$SV2_URL/v1/sync_status" | jq '{head_l1: .head_l1, current_l1: .current_l1, unsafe_l2: .unsafe_l2, safe_l2: .safe_l2, finalized_l2: .finalized_l2}'

# Log-based snapshot
grep -a "computed sync actions" op-up/logs/op-up.latest.log | tail -n1
grep -a "poll: heads" op-up/logs/op-up.latest.log | tail -n1

# L2 block number (direct user RPC)
cast block-number --rpc-url http://localhost:8545
```

## Logs

- The run prints to stdout by default; you can redirect to `op-up/logs/`.

## Notes

- The batcher uses `BATCHER_PK` to submit calldata to L1.
- `OP_SV2_CONFIRM_DEPTH` allows faster safe advancement in dev runs.
- `op-up/artifacts/` and `op-up/external-l1.env` are ignored by git.
- If you see `tx in inbox with unauthorized submitter`, your `BATCHER_PK` address does not match the configured batcher; update the intent or use the correct key and redeploy.
- You can use separate `DEPLOYER_PK` and `BATCHER_PK`; setting them to the same key keeps things simple. Scripts now read these explicitly (no `L1_PK`).
