#!/usr/bin/env bash
set -euo pipefail

# Simple sysgo smoke test:
# - starts op-up (sysgo) in the background
# - waits for EL RPC to be ready and extracts the printed test private key
# - sends a tx using cast and waits for its receipt

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

# Wait for EL RPC
EL_URL="http://127.0.0.1:8545"
echo "[sysgo-smoke] waiting for EL RPC at $EL_URL"
for i in {1..120}; do
  if curl -sf -H 'content-type: application/json' -d '{"jsonrpc":"2.0","id":1,"method":"eth_chainId","params":[]}' "$EL_URL" >/dev/null; then
    echo "[sysgo-smoke] EL RPC is up"
    break
  fi
  sleep 0.5
  if ! kill -0 "$UP_PID" >/dev/null 2>&1; then
    echo "[sysgo-smoke] op-up exited early; check logs: $LOG_FILE" >&2
    exit 1
  fi
  if [[ $i -eq 120 ]]; then
    echo "[sysgo-smoke] timeout waiting for EL RPC" >&2
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

export ETH_RPC_URL="$EL_URL"

# Derive sender address from PK (cast wallet)
SENDER=$(cast wallet address --private-key "$PK")
echo "[sysgo-smoke] sender: $SENDER"

# Send a self-transfer of 0 ETH (consumes gas, exercises tx path). Let node estimate fees.
echo "[sysgo-smoke] sending tx"
TX_JSON=$(cast send "$SENDER" --private-key "$PK" --value 0 --json 2>/dev/null || true)
TX_HASH=$(echo "$TX_JSON" | sed -n 's/.*"transactionHash"[[:space:]]*:[[:space:]]*"\(0x[0-9a-fA-F]\+\)".*/\1/p' | head -n1)
if [[ -z "$TX_HASH" ]]; then
  # Fallback: sometimes cast prints the hash alone; try to extract any 0x... hash from last lines
  TX_HASH=$(echo "$TX_JSON" | grep -Eo '0x[0-9a-fA-F]+' | head -n1 || true)
fi
if [[ -z "$TX_HASH" ]]; then
  echo "[sysgo-smoke] failed to parse tx hash from cast output" >&2
  echo "$TX_JSON" >&2
  exit 1
fi
echo "[sysgo-smoke] tx: $TX_HASH"

echo "[sysgo-smoke] waiting for receipt"
cast receipt "$TX_HASH" --poll --confirmations 1
echo "[sysgo-smoke] receipt confirmed"

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

