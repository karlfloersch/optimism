#!/usr/bin/env bash
set -euo pipefail

CHAIN_ID="${1:-901}"
WORKDIR="${2:-$(cd "$(dirname "$0")" && pwd)/artifacts}"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"

mkdir -p "$WORKDIR"

: "${OP_L1_RPC:?OP_L1_RPC must be set}"
# Use DEPLOYER_PK for contract deployment
: "${DEPLOYER_PK:?DEPLOYER_PK must be set}"

# Ensure op-deployer picks up the L1 RPC URL (expects L1_RPC_URL)
# and pass flags explicitly as well for clarity.
export L1_RPC_URL="${L1_RPC_URL:-$OP_L1_RPC}"

# Seed intent from committed template and rewrite BATCHER placeholder when missing
if [ ! -f "$WORKDIR/intent.toml" ]; then
  if [ -f "$ROOT/op-up/deploy-sepolia/intent.toml" ]; then
    cp "$ROOT/op-up/deploy-sepolia/intent.toml" "$WORKDIR/intent.toml"
    if [ -n "${BATCHER_PK:-}" ]; then
      ADDR=$(cast wallet address --private-key "$BATCHER_PK")
      tmp=$(mktemp)
      sed -E "s/0xBATCHER/$ADDR/g; s#file://__ROOT__#$ROOT#g" "$WORKDIR/intent.toml" > "$tmp" && mv "$tmp" "$WORKDIR/intent.toml"
    fi
  fi
fi

if [ -d "$ROOT/packages/contracts-bedrock/forge-artifacts" ] && [ ! -f "$WORKDIR/forge-artifacts.tgz" ]; then
  tar -czf "$WORKDIR/forge-artifacts.tgz" -C "$ROOT/packages/contracts-bedrock" forge-artifacts
fi

if [ ! -f "$WORKDIR/rollup.json" ]; then
  go run "$ROOT/op-deployer/cmd/op-deployer" inspect rollup --workdir "$WORKDIR" "$CHAIN_ID" --outfile "$WORKDIR/rollup.json"
fi
if [ ! -f "$WORKDIR/l2_genesis.json" ]; then
  go run "$ROOT/op-deployer/cmd/op-deployer" inspect genesis --workdir "$WORKDIR" "$CHAIN_ID" --outfile "$WORKDIR/l2_genesis.json"
fi

go run "$ROOT/op-deployer/cmd/op-deployer" apply \
  --workdir "$WORKDIR" \
  --l1-rpc-url "$OP_L1_RPC" \
  --private-key "$DEPLOYER_PK" \
  "$CHAIN_ID"

echo "Artifacts written to $WORKDIR"
echo "- rollup.json"
echo "- l2_genesis.json"
