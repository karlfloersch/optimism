#!/bin/bash
set -e

# Run overnight test with full observability (multi-chain)
#
# Usage:
#   ./scripts/run-overnight.sh
#
# This script:
#   1. Starts the filter service with metrics (OP Sepolia + Unichain Sepolia)
#   2. Starts spammers for each chain
#   3. Shows how to run the dashboard
#
# To stop: Ctrl+C or kill the background processes

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(dirname "$SCRIPT_DIR")"
cd "$ROOT_DIR/.."

# Load dev environment
if [ -f "$ROOT_DIR/.env.dev" ]; then
    source "$ROOT_DIR/.env.dev"
fi

# Chain config (set in .env.dev)
OP_SEPOLIA_CHAIN_ID="11155420"
UNICHAIN_SEPOLIA_CHAIN_ID="1301"

# Validate required env vars
if [ -z "$OP_SEPOLIA_RPC" ]; then
    echo "ERROR: OP_SEPOLIA_RPC not set. Create .env.dev or export it."
    exit 1
fi
if [ -z "$UNICHAIN_SEPOLIA_RPC" ]; then
    echo "ERROR: UNICHAIN_SEPOLIA_RPC not set. Create .env.dev or export it."
    exit 1
fi

# General config
BACKFILL_DURATION="${BACKFILL_DURATION:-5m}"
QUERY_INTERVAL="${QUERY_INTERVAL:-5s}"
NUM_QUERIES="${NUM_QUERIES:-0}"  # 0 = run forever
BLOCK_RANGE="${BLOCK_RANGE:-100}"  # Should be less than blocks in BACKFILL_DURATION

# Ports
FILTER_RPC_PORT=8560
FILTER_METRICS_PORT=7300
SPAMMER_OP_METRICS_PORT=7301
SPAMMER_UNI_METRICS_PORT=7302

echo "=================================================="
echo "  OP-INTEROP-FILTER OVERNIGHT TEST (MULTI-CHAIN)"
echo "=================================================="
echo ""
echo "Chains:"
echo "  OP Sepolia (${OP_SEPOLIA_CHAIN_ID}): $OP_SEPOLIA_RPC"
echo "  Unichain Sepolia (${UNICHAIN_SEPOLIA_CHAIN_ID}): $UNICHAIN_SEPOLIA_RPC"
echo ""
echo "Configuration:"
echo "  Backfill Duration: $BACKFILL_DURATION"
echo "  Query Interval: $QUERY_INTERVAL"
echo "  Num Queries: $NUM_QUERIES (0=forever)"
echo "  Block Range: $BLOCK_RANGE"
echo ""

# Build binaries
echo "Building binaries..."
go build -o ./bin/op-interop-filter ./op-interop-filter/cmd/main.go
go build -o ./bin/filter-spammer ./op-interop-filter/cmd/spammer/main.go
go build -o ./bin/filter-dashboard ./op-interop-filter/cmd/dashboard/main.go
echo "Build complete."
echo ""

# Cleanup function
cleanup() {
    echo ""
    echo "Stopping services..."
    kill $FILTER_PID 2>/dev/null || true
    kill $SPAMMER_OP_PID 2>/dev/null || true
    kill $SPAMMER_UNI_PID 2>/dev/null || true
    echo "Done."
    exit 0
}
trap cleanup SIGINT SIGTERM

# Start filter service with both chains
echo "Starting filter service (multi-chain)..."
./bin/op-interop-filter \
    --l2-rpcs="$OP_SEPOLIA_RPC" \
    --l2-rpcs="$UNICHAIN_SEPOLIA_RPC" \
    --backfill-duration="$BACKFILL_DURATION" \
    --rpc.port=$FILTER_RPC_PORT \
    --rpc.enable-admin \
    --metrics.enabled \
    --metrics.port=$FILTER_METRICS_PORT \
    --log.level=info \
    > /tmp/filter.log 2>&1 &
FILTER_PID=$!
echo "  Filter PID: $FILTER_PID"
echo "  RPC: http://localhost:$FILTER_RPC_PORT"
echo "  Metrics: http://localhost:$FILTER_METRICS_PORT/metrics"
echo "  Log: /tmp/filter.log"
echo ""

# Wait for filter to be ready
echo "Waiting for filter to be ready..."
for i in {1..120}; do
    if curl -s http://localhost:$FILTER_RPC_PORT -H "Content-Type: application/json" \
        -d '{"jsonrpc":"2.0","method":"admin_getFailsafeEnabled","params":[],"id":1}' | grep -q "result"; then
        echo "  Filter is responding!"
        break
    fi
    sleep 2
done
echo ""

# Start spammer for OP Sepolia
echo "Starting spammer for OP Sepolia..."
./bin/filter-spammer \
    --l2-rpc="$OP_SEPOLIA_RPC" \
    --filter-rpc="http://localhost:$FILTER_RPC_PORT" \
    --chain-id=$OP_SEPOLIA_CHAIN_ID \
    --num-queries=$NUM_QUERIES \
    --block-range=$BLOCK_RANGE \
    --query-interval="$QUERY_INTERVAL" \
    --metrics.enabled \
    --metrics.port=$SPAMMER_OP_METRICS_PORT \
    --log.level=info \
    > /tmp/spammer-op.log 2>&1 &
SPAMMER_OP_PID=$!
echo "  Spammer (OP Sepolia) PID: $SPAMMER_OP_PID"
echo "  Metrics: http://localhost:$SPAMMER_OP_METRICS_PORT/metrics"
echo "  Log: /tmp/spammer-op.log"
echo ""

# Start spammer for Unichain Sepolia
echo "Starting spammer for Unichain Sepolia..."
./bin/filter-spammer \
    --l2-rpc="$UNICHAIN_SEPOLIA_RPC" \
    --filter-rpc="http://localhost:$FILTER_RPC_PORT" \
    --chain-id=$UNICHAIN_SEPOLIA_CHAIN_ID \
    --num-queries=$NUM_QUERIES \
    --block-range=$BLOCK_RANGE \
    --query-interval="$QUERY_INTERVAL" \
    --metrics.enabled \
    --metrics.port=$SPAMMER_UNI_METRICS_PORT \
    --log.level=info \
    > /tmp/spammer-uni.log 2>&1 &
SPAMMER_UNI_PID=$!
echo "  Spammer (Unichain Sepolia) PID: $SPAMMER_UNI_PID"
echo "  Metrics: http://localhost:$SPAMMER_UNI_METRICS_PORT/metrics"
echo "  Log: /tmp/spammer-uni.log"
echo ""

echo "=================================================="
echo "  SERVICES RUNNING"
echo "=================================================="
echo ""
echo "To view dashboard, run in another terminal:"
echo "  ./bin/filter-dashboard --spammer-metrics=http://localhost:$SPAMMER_OP_METRICS_PORT/metrics,http://localhost:$SPAMMER_UNI_METRICS_PORT/metrics"
echo ""
echo "To view logs:"
echo "  tail -f /tmp/filter.log"
echo "  tail -f /tmp/spammer-op.log"
echo "  tail -f /tmp/spammer-uni.log"
echo ""
echo "To view raw metrics:"
echo "  curl http://localhost:$FILTER_METRICS_PORT/metrics"
echo "  curl http://localhost:$SPAMMER_OP_METRICS_PORT/metrics"
echo "  curl http://localhost:$SPAMMER_UNI_METRICS_PORT/metrics"
echo ""
echo "Press Ctrl+C to stop all services"
echo ""

# Wait for processes
wait
