#!/usr/bin/env bash
set -euo pipefail

# Simple sysgo smoke test:
# - starts op-up (sysgo) in the background
# - waits for EL RPC(s) to be ready and extracts the printed test private key
# - sends a tx using cast to each chain RPC and waits for the receipt(s)

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
LOG_FILE="${ROOT_DIR}/.sysgo-smoke.log"

echo "[sysgo-smoke] activating mise (if available)"
if command -v mise >/dev/null 2>&1; then
  # shellcheck disable=SC1090
  eval "$(mise activate bash)" || true
fi

echo "[sysgo-smoke] starting sysgo harness (op-up)"
cd "${ROOT_DIR}"
rm -f "$LOG_FILE"
(
  # Run with a timeout guard in case the internal auto-stop is increased
  # Note: 'command timeouts' differ per shell; use gnu-timeout if available
  export OP_UP_STOP_AFTER=${OP_UP_STOP_AFTER:-45s}
  if command -v timeout >/dev/null 2>&1; then
    timeout 120s go run ./op-up 2>&1 | tee "$LOG_FILE"
  else
    go run ./op-up 2>&1 | tee "$LOG_FILE"
  fi
) &
UP_PID=$!
echo "[sysgo-smoke] op-up pid: $UP_PID (logs: $LOG_FILE)"

cleanup() {
  echo "[sysgo-smoke] cleaning up (pid=$UP_PID)"
  if kill -0 "$UP_PID" >/dev/null 2>&1; then
    kill "$UP_PID" >/dev/null 2>&1 || true
    wait "$UP_PID" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

# Determine RPC targets
RPCS=("http://127.0.0.1:8545")
if [[ "${OP_LOCAL_TWO_CHAIN:-}" == "1" || -n "${OP_L2_CHAIN_IDS:-}" ]]; then
  RPCS=("http://127.0.0.1:9545" "http://127.0.0.1:9546")
fi

# Wait for all EL RPCs
for EL_URL in "${RPCS[@]}"; do
  echo "[sysgo-smoke] waiting for EL RPC at $EL_URL"
  READY=0
  for i in {1..180}; do
    if curl -sf -H 'content-type: application/json' -d '{"jsonrpc":"2.0","id":1,"method":"eth_chainId","params":[]}' "$EL_URL" >/dev/null; then
      echo "[sysgo-smoke] EL RPC is up: $EL_URL"
      READY=1
      break
    fi
    sleep 0.5
    if ! kill -0 "$UP_PID" >/dev/null 2>&1; then
      echo "[sysgo-smoke] op-up exited early; check logs: $LOG_FILE" >&2
      exit 1
    fi
  done
  if [[ $READY -ne 1 ]]; then
    echo "[sysgo-smoke] timeout waiting for EL RPC: $EL_URL" >&2
    exit 1
  fi
done

# Extract the printed private key from op-up logs
echo "[sysgo-smoke] waiting for printed test private key in logs"
PK=""
for i in {1..60}; do
  if [[ -f "$LOG_FILE" ]]; then
    # Example log line: "Test Account Private Key: 0x..."
    if PK_LINE=$(grep -m1 -E "Test Account Private Key: 0x[0-9a-fA-F]+" "$LOG_FILE" || true); then
      if [[ -n "$PK_LINE" ]]; then
        PK=$(echo "$PK_LINE" | sed -E 's/.*(0x[0-9a-fA-F]+).*/\1/')
        break
      fi
    fi
  fi
  sleep 0.5
done
if [[ -z "$PK" ]]; then
  echo "[sysgo-smoke] failed to parse private key from logs; see $LOG_FILE" >&2
  exit 1
fi
echo "[sysgo-smoke] using private key: ${PK:0:10}..."

if ! command -v cast >/dev/null 2>&1; then
  echo "[sysgo-smoke] 'cast' not found in PATH. Ensure Foundry is installed (via mise/prereqs)." >&2
  exit 1
fi

for EL_URL in "${RPCS[@]}"; do
  export ETH_RPC_URL="$EL_URL"
  # Derive sender address from PK (cast wallet)
  SENDER=$(cast wallet address --private-key "$PK")
  echo "[sysgo-smoke] sender: $SENDER (rpc=$EL_URL)"
  # Send a self-transfer of 0 ETH (consumes gas, exercises tx path). Let node estimate fees.
  echo "[sysgo-smoke] sending tx (rpc=$EL_URL)"
  # Prefer async mode which prints the hash directly
  TX_HASH=$(cast send "$SENDER" --private-key "$PK" --value 0 --async 2>&1 | grep -Eo '0x[0-9a-fA-F]{64}' | head -n1 || true)
  if [[ -z "$TX_HASH" ]]; then
    # Fallback to JSON mode and parse
    TX_JSON=$(cast send "$SENDER" --private-key "$PK" --value 0 --json 2>&1 || true)
    if command -v jq >/dev/null 2>&1; then
      TX_HASH=$(echo "$TX_JSON" | jq -r '(.transactionHash // .hash // .result // .txHash) // empty' 2>/dev/null | head -n1)
    fi
    if [[ -z "$TX_HASH" ]]; then
      TX_HASH=$(echo "$TX_JSON" | sed -n 's/.*"transactionHash"[[:space:]]*:[[:space:]]*"\(0x[0-9a-fA-F]\+\)".*/\1/p' | head -n1)
    fi
    if [[ -z "$TX_HASH" ]]; then
      TX_HASH=$(echo "$TX_JSON" | grep -Eo '0x[0-9a-fA-F]{64}' | head -n1 || true)
    fi
  fi
  if [[ -z "$TX_HASH" ]]; then
    echo "[sysgo-smoke] failed to parse tx hash from cast output (rpc=$EL_URL)" >&2
    echo "$TX_JSON" >&2
    exit 1
  fi
  echo "[sysgo-smoke] tx: $TX_HASH (rpc=$EL_URL)"
  echo "[sysgo-smoke] waiting for receipt (rpc=$EL_URL)"
  CONFIRMED=0
  for i in {1..240}; do
    REC_JSON=$(cast receipt "$TX_HASH" --json 2>/dev/null || true)
    # consider confirmed if we have a blockNumber and status fields
    if echo "$REC_JSON" | grep -q '"blockNumber"' && echo "$REC_JSON" | grep -q '"status"'; then
      echo "[sysgo-smoke] receipt confirmed (rpc=$EL_URL)"
      CONFIRMED=1
      break
    fi
    sleep 0.5
  done
  if [[ $CONFIRMED -ne 1 ]]; then
    echo "[sysgo-smoke] timeout waiting for receipt (rpc=$EL_URL)" >&2
    exit 1
  fi
done

# Summarize unsafe/safe head progress from logs
echo "[sysgo-smoke] summarizing head progression"
UNSAFE=$(grep -n "Inserted new L2 unsafe block" "$LOG_FILE" | wc -l | tr -d ' ')
SAFE=$(grep -n "Forkchoice update" "$LOG_FILE" | grep -Eo 'safe=0x[0-9a-fA-F]+' | wc -l | tr -d ' ')
echo "[sysgo-smoke] unsafe blocks inserted: $UNSAFE"
echo "[sysgo-smoke] safe head updates seen: $SAFE"

echo "[sysgo-smoke] querying SV2 /v1/sync_status for safe progress"
SV2_URL="http://127.0.0.1:9750"
for i in {1..60}; do
  if curl -sf "$SV2_URL/healthz" >/dev/null; then
    break
  fi
  sleep 0.5

done
SYNC=$(curl -sf "$SV2_URL/v1/sync_status" || true)
if [[ -z "$SYNC" ]]; then
  echo "[sysgo-smoke] warning: SV2 sync_status not available"
else
  echo "[sysgo-smoke] sv2 sync_status: $SYNC"
fi

echo "[sysgo-smoke] SUCCESS"

