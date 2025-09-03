#!/usr/bin/env bash

# shellcheck disable=SC1091
source "$(dirname "$0")/boilerplate.sh"

jq_bin=$(command -v jq || true)
if [ -z "$jq_bin" ]; then
  echo "jq is required" >&2
  exit 1
fi

# Extract the first L2 node EL RPC endpoint from environment JSON
EL_URL=$(jq -r '.l2[0].nodes[0].services.el.endpoints.rpc | "http://\(.host):\(.port)"' "$ENVIRONMENT")
if [ -z "$EL_URL" ] || [ "$EL_URL" = "null" ]; then
  echo "Could not find L2 EL RPC endpoint in environment JSON" >&2
  exit 1
fi

echo "Using EL endpoint: $EL_URL"

# Poll eth_blockNumber for progression (baseline health)
get_block() {
  curl -sS -X POST -H 'Content-Type: application/json' \
    --data '{"jsonrpc":"2.0","id":1,"method":"eth_blockNumber","params":[]}' \
    "$EL_URL" | jq -r '.result' 2>/dev/null
}

start_hex=$(get_block)
if [ -z "$start_hex" ] || [ "$start_hex" = "null" ]; then
  echo "Failed to query eth_blockNumber from $EL_URL" >&2
  exit 1
fi

start_dec=$((start_hex))
echo "Start block: $start_dec ($start_hex)"

# Wait up to 90s for at least 3 blocks of progress
deadline=$((SECONDS + 90))
progressed=false
while [ $SECONDS -lt $deadline ]; do
  sleep 3
  cur_hex=$(get_block)
  if [ -z "$cur_hex" ] || [ "$cur_hex" = "null" ]; then
    continue
  fi
  cur_dec=$((cur_hex))
  echo "Current block: $cur_dec ($cur_hex)"
  if [ $cur_dec -ge $((start_dec + 3)) ]; then
    progressed=true
    break
  fi
done

if [ "$progressed" != true ]; then
  echo "No sufficient block progression observed on $EL_URL" >&2
  exit 1
fi

echo "Block progression OK"

# Try sending a simple self-transfer using msgcheck (single-chain mode) or fallback to cast if msgcheck is unavailable
PK=$(jq -r '.l2[0].wallets["dev-account-0"].private_key' "$ENVIRONMENT" 2>/dev/null || echo "")
if [ -z "$PK" ] || [ "$PK" = "null" ]; then
  echo "Could not find funded dev-account-0 private key in environment JSON" >&2
  exit 1
fi

# Normalize key to 0x-prefixed
if [[ "$PK" != 0x* ]]; then
  PK="0x$PK"
fi

# Prefer msgcheck if available on PATH; otherwise use cast
if command -v msgcheck >/dev/null 2>&1; then
  echo "Running msgcheck tx mode"
  msgcheck --mode=tx --rpc-901 "$EL_URL" --rpc-902 "" --priv-key "$PK" --timeout 2m
else
  if ! command -v cast >/dev/null 2>&1; then
    echo "Neither msgcheck nor cast is available in this environment" >&2
    exit 1
  fi
  echo "Running cast self-transfer"
  FROM=$(cast wallet address "$PK")
  TXHASH=$(cast send --rpc-url "$EL_URL" --private-key "$PK" "$FROM" --value 0 2>/dev/null | awk '/Transaction hash/ {print $3}')
  if [ -z "$TXHASH" ]; then
    echo "Failed to send tx with cast" >&2
    exit 1
  fi
  echo "Sent tx: $TXHASH"
  # Wait for inclusion up to 60s
  incl_deadline=$((SECONDS + 60))
  included=false
  while [ $SECONDS -lt $incl_deadline ]; do
    STATUS=$(cast tx --rpc-url "$EL_URL" "$TXHASH" 2>/dev/null | awk -F': ' '/status/ {print tolower($2)}')
    if [ -n "$STATUS" ]; then
      echo "Tx status: $STATUS"
      included=true
      break
    fi
    sleep 2
  done
  if [ "$included" != true ]; then
    echo "Timed out waiting for tx inclusion: $TXHASH" >&2
    exit 1
  fi
fi

echo "Smoke test OK: progression and tx send verified"
