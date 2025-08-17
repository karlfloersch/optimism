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

# Convert decimal CHAIN_ID to 256-bit hex for template/init use
L2_HEX=$(printf "0x%064x" "$CHAIN_ID")

# Initialize state.json (required by apply/inspect)
if [ ! -f "$WORKDIR/state.json" ]; then
  go run "$ROOT/op-deployer/cmd/op-deployer" init \
    --workdir "$WORKDIR" \
    --intent-type standard-overrides \
    --l1-chain-id "${OP_L1_CHAIN_ID}" \
    --l2-chain-ids "$L2_HEX"
fi

# Overwrite intent.toml with our template and apply placeholders each run
if [ -f "$ROOT/op-up/deploy-sepolia/intent.toml" ]; then
  cp "$ROOT/op-up/deploy-sepolia/intent.toml" "$WORKDIR/intent.toml"
  # Always expand __ROOT__ placeholder
  tmp=$(mktemp)
  sed -E "s#__ROOT__#$ROOT#g" "$WORKDIR/intent.toml" > "$tmp" && mv "$tmp" "$WORKDIR/intent.toml"

  # Ensure the chain ID inside the template matches the CHAIN_ID we are deploying
  # Convert decimal CHAIN_ID to 256-bit hex (already computed as L2_HEX)
  tmp=$(mktemp)
  sed -E "s#id = \"0x[0-9a-fA-F]+\"#id = \"$L2_HEX\"#" "$WORKDIR/intent.toml" > "$tmp" && mv "$tmp" "$WORKDIR/intent.toml"

  # Choose per-chain batcher key if provided, else fall back to BATCHER_PK
  BATCHER_PK_FOR_CHAIN=""
  if [ -n "${CHAIN_ID:-}" ]; then
    # Indirect expansion to read env var BATCHER_PK_<CHAIN_ID> if set
    CHAIN_SPECIFIC_VAR="BATCHER_PK_${CHAIN_ID}"
    if [ -n "${!CHAIN_SPECIFIC_VAR:-}" ]; then
      BATCHER_PK_FOR_CHAIN="${!CHAIN_SPECIFIC_VAR}"
    fi
  fi
  if [ -z "$BATCHER_PK_FOR_CHAIN" ] && [ -n "${BATCHER_PK:-}" ]; then
    BATCHER_PK_FOR_CHAIN="$BATCHER_PK"
  fi

  # Optionally rewrite batcher placeholder if we have a key
  if [ -n "$BATCHER_PK_FOR_CHAIN" ]; then
    ADDR=$(cast wallet address --private-key "$BATCHER_PK_FOR_CHAIN")
    tmp=$(mktemp)
    sed -E "s/0xBATCHER/$ADDR/g" "$WORKDIR/intent.toml" > "$tmp" && mv "$tmp" "$WORKDIR/intent.toml"
  fi
fi

# (init already handled above)

if [ -d "$ROOT/packages/contracts-bedrock/forge-artifacts" ] && [ ! -f "$WORKDIR/forge-artifacts.tgz" ]; then
  tar -czf "$WORKDIR/forge-artifacts.tgz" -C "$ROOT/packages/contracts-bedrock" forge-artifacts
fi

# Deploy (creates/updates state.json) before generating derived artifacts
go run "$ROOT/op-deployer/cmd/op-deployer" apply \
  --workdir "$WORKDIR" \
  --l1-rpc-url "$OP_L1_RPC" \
  --private-key "$DEPLOYER_PK" \
  "$CHAIN_ID"

# Some versions of op-deployer print to stdout and ignore --outfile; use shell redirection
if [ ! -f "$WORKDIR/rollup.json" ]; then
  go run "$ROOT/op-deployer/cmd/op-deployer" inspect rollup --workdir "$WORKDIR" "$CHAIN_ID" > "$WORKDIR/rollup.json"
fi
if [ ! -f "$WORKDIR/l2_genesis.json" ]; then
  go run "$ROOT/op-deployer/cmd/op-deployer" inspect genesis --workdir "$WORKDIR" "$CHAIN_ID" > "$WORKDIR/l2_genesis.json"
fi
echo "Artifacts written to $WORKDIR"
echo "- rollup.json"
echo "- l2_genesis.json"
