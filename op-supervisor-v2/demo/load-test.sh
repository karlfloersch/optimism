#!/usr/bin/env bash

set -euo pipefail

# Load environment variables
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1090
source "$SCRIPT_DIR/variables.sh"

# Configuration
DEFAULT_WORKERS=100
MAX_WORKERS=300
KEYS_FILE="$SCRIPT_DIR/funded-keys.txt"
MSGCHECK_DIR="$SCRIPT_DIR/../../../karls-op/op-up"

# Parse arguments
WORKERS="${1:-$DEFAULT_WORKERS}"
DURATION="${2:-60}"  # Duration in seconds, 0 for infinite

# Validate arguments
if ! [[ "$WORKERS" =~ ^[0-9]+$ ]] || [ "$WORKERS" -lt 1 ] || [ "$WORKERS" -gt "$MAX_WORKERS" ]; then
    echo "ERROR: Workers must be a number between 1 and $MAX_WORKERS" >&2
    exit 1
fi

if ! [[ "$DURATION" =~ ^[0-9]+$ ]]; then
    echo "ERROR: Duration must be a number (seconds, 0 for infinite)" >&2
    exit 1
fi

echo "=== Cross-Chain Message Load Test ==="
echo "Workers: $WORKERS"
echo "Duration: $([ "$DURATION" -eq 0 ] && echo "infinite" || echo "${DURATION}s")"
echo ""

# Verify required files and variables
if [ ! -f "$KEYS_FILE" ]; then
    echo "ERROR: Keys file not found: $KEYS_FILE" >&2
    echo "Run ./generate-funded-keys.sh first" >&2
    exit 1
fi

if [ -z "${CHAIN_2151908_RPC:-}" ] || [ -z "${CHAIN_2151909_RPC:-}" ]; then
    echo "ERROR: Required environment variables not set. Make sure Docker containers are running." >&2
    exit 1
fi

if [ ! -d "$MSGCHECK_DIR" ]; then
    echo "ERROR: msgcheck directory not found: $MSGCHECK_DIR" >&2
    exit 1
fi

# Count available keys (skip header lines)
AVAILABLE_KEYS=$(grep -c "^0x" "$KEYS_FILE" || true)
if [ "$AVAILABLE_KEYS" -lt "$WORKERS" ]; then
    echo "ERROR: Not enough funded keys ($AVAILABLE_KEYS) for $WORKERS workers" >&2
    echo "Run ./generate-funded-keys.sh to generate more keys" >&2
    exit 1
fi

echo "Using $WORKERS unique keys from $AVAILABLE_KEYS available"
echo ""

# Create worker function
worker_function() {
    local worker_id=$1
    local private_key=$2
    local address=$3
    local end_time=$4

    local valid_count=0
    local invalid_count=0
    local error_count=0

    echo "Worker $worker_id started (address: $address)"

    while true; do
        # Check if we should stop (duration-based)
        if [ "$end_time" -ne 0 ] && [ "$(date +%s)" -ge "$end_time" ]; then
            break
        fi

        # 50/50 chance: valid or invalid message
        if [ $((RANDOM % 2)) -eq 0 ]; then
            mode="valid-msg"
        else
            mode="invalid-msg"
        fi

        # Run the test
        cd "$MSGCHECK_DIR"
        result=$(go run ./cmd/msgcheck \
            -mode "$mode" \
            -from 901 \
            -priv-key "$private_key" \
            -rpc-901 "$CHAIN_2151908_RPC" \
            -rpc-902 "$CHAIN_2151909_RPC" 2>/dev/null | grep -E "(Valid Message:|Invalid Message:)" || echo "ERROR")

        if [[ "$result" == *"Valid Message:"* ]]; then
            ((valid_count++))
            echo "Worker $worker_id: $result"
        elif [[ "$result" == *"Invalid Message:"* ]]; then
            ((invalid_count++))
            echo "Worker $worker_id: $result"
        else
            ((error_count++))
            echo "Worker $worker_id: ERROR - $result"
        fi

        # Continue running (removed 50/50 chance to stop)

        # Small delay to prevent overwhelming the system
        sleep 0.1
    done

    echo "Worker $worker_id finished: valid=$valid_count invalid=$invalid_count errors=$error_count total=$((valid_count + invalid_count + error_count))"
}

# Calculate end time
if [ "$DURATION" -eq 0 ]; then
    END_TIME=0
else
    END_TIME=$(($(date +%s) + DURATION))
fi

# Extract unique keys for workers
echo "Extracting $WORKERS unique keys..."
# Use portable random selection (works on macOS and Linux)
if command -v shuf >/dev/null 2>&1; then
    WORKER_KEYS=$(grep "^0x" "$KEYS_FILE" | shuf | head -n "$WORKERS")
elif command -v sort >/dev/null 2>&1 && sort -R /dev/null >/dev/null 2>&1; then
    # Use sort -R if available (GNU coreutils)
    WORKER_KEYS=$(grep "^0x" "$KEYS_FILE" | sort -R | head -n "$WORKERS")
else
    # Fallback: just take the first N keys (not random but works)
    WORKER_KEYS=$(grep "^0x" "$KEYS_FILE" | head -n "$WORKERS")
fi

# Start workers in background
WORKER_PIDS=()
worker_id=1

while IFS= read -r key_line; do
    if [ -z "$key_line" ]; then
        continue
    fi

    private_key=$(echo "$key_line" | cut -d, -f1)
    address=$(echo "$key_line" | cut -d, -f2)

    # Start worker in background
    worker_function "$worker_id" "$private_key" "$address" "$END_TIME" &
    WORKER_PIDS+=($!)

    ((worker_id++))

    # Small delay between worker starts
    sleep 0.05
done <<< "$WORKER_KEYS"

echo "Started $WORKERS workers (PIDs: ${WORKER_PIDS[*]})"
echo ""
echo "Press Ctrl+C to stop all workers"
echo ""

# Function to cleanup workers on exit
cleanup() {
    echo ""
    echo "Stopping all workers..."
    for pid in "${WORKER_PIDS[@]}"; do
        if kill -0 "$pid" 2>/dev/null; then
            kill "$pid" 2>/dev/null || true
        fi
    done
    wait 2>/dev/null || true
    echo "All workers stopped"
    exit 0
}

# Set trap for cleanup
trap cleanup SIGINT SIGTERM

# Wait for all workers to finish
for pid in "${WORKER_PIDS[@]}"; do
    wait "$pid" 2>/dev/null || true
done

echo ""
echo "=== Load Test Complete ==="
