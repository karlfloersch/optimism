#!/usr/bin/env bash
set -euo pipefail

CHAIN_ID="${1:-901}"
WORKDIR="${2:-$(cd "$(dirname "$0")" && pwd)/artifacts}"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"

mkdir -p "$WORKDIR"

: "${OP_L1_RPC:?OP_L1_RPC must be set}"
: "${L1_PK:?L1_PK must be set}"

if [ ! -f "$WORKDIR/intent.toml" ] && [ -f "$ROOT/deploy-sepolia/intent.toml" ]; then
  cp "$ROOT/deploy-sepolia/intent.toml" "$WORKDIR/intent.toml"
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

go run "$ROOT/op-deployer/cmd/op-deployer" apply --workdir "$WORKDIR" "$CHAIN_ID"

echo "Artifacts written to $WORKDIR"
echo "- rollup.json"
echo "- l2_genesis.json"
