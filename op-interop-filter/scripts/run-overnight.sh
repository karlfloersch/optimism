#!/bin/bash
set -e

# Run overnight test with full observability
#
# Usage:
#   ./scripts/run-overnight.sh
#
# This script:
#   1. Starts the filter service with metrics
#   2. Starts the spammer with metrics
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

# Config (edit these as needed)
L2_RPC="${OP_SEPOLIA_RPC:-https://cosmopolitan-crimson-snowflake.optimism-sepolia.quiknode.pro/a4ae3d96a7a743de80036fc0ad88841159b8482e}"
CHAIN_ID="${CHAIN_ID:-11155420}"
BACKFILL_DURATION="${BACKFILL_DURATION:-30m}"
QUERY_INTERVAL="${QUERY_INTERVAL:-5s}"
NUM_QUERIES="${NUM_QUERIES:-0}"  # 0 = run forever
BLOCK_RANGE="${BLOCK_RANGE:-500}"  # Should be less than blocks in BACKFILL_DURATION

# Ports
FILTER_RPC_PORT=8560
FILTER_METRICS_PORT=7300
SPAMMER_METRICS_PORT=7301

echo "=================================================="
echo "  OP-INTEROP-FILTER OVERNIGHT TEST"
echo "=================================================="
echo ""
echo "Configuration:"
echo "  L2 RPC: $L2_RPC"
echo "  Chain ID: $CHAIN_ID"
echo "  Backfill Duration: $BACKFILL_DURATION"
echo "  Query Interval: $QUERY_INTERVAL"
echo "  Num Queries: $NUM_QUERIES (0=forever)"
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
    kill $SPAMMER_PID 2>/dev/null || true
    echo "Done."
    exit 0
}
trap cleanup SIGINT SIGTERM

# Start filter service
echo "Starting filter service..."
./bin/op-interop-filter \
    --l2-rpcs="${CHAIN_ID}:${L2_RPC}" \
    --backfill-duration="$BACKFILL_DURATION" \
    --rpc.port=$FILTER_RPC_PORT \
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
for i in {1..60}; do
    if curl -s http://localhost:$FILTER_RPC_PORT -H "Content-Type: application/json" \
        -d '{"jsonrpc":"2.0","method":"admin_getFailsafeEnabled","params":[],"id":1}' | grep -q "result"; then
        echo "  Filter is ready!"
        break
    fi
    sleep 2
done
echo ""

# Start spammer
echo "Starting spammer..."
./bin/filter-spammer \
    --l2-rpc="$L2_RPC" \
    --filter-rpc="http://localhost:$FILTER_RPC_PORT" \
    --chain-id=$CHAIN_ID \
    --num-queries=$NUM_QUERIES \
    --block-range=$BLOCK_RANGE \
    --query-interval="$QUERY_INTERVAL" \
    --metrics.enabled \
    --metrics.port=$SPAMMER_METRICS_PORT \
    --log.level=info \
    > /tmp/spammer.log 2>&1 &
SPAMMER_PID=$!
echo "  Spammer PID: $SPAMMER_PID"
echo "  Metrics: http://localhost:$SPAMMER_METRICS_PORT/metrics"
echo "  Log: /tmp/spammer.log"
echo ""

echo "=================================================="
echo "  SERVICES RUNNING"
echo "=================================================="
echo ""
echo "To view dashboard, run in another terminal:"
echo "  ./bin/filter-dashboard"
echo ""
echo "To view logs:"
echo "  tail -f /tmp/filter.log"
echo "  tail -f /tmp/spammer.log"
echo ""
echo "To view raw metrics:"
echo "  curl http://localhost:$FILTER_METRICS_PORT/metrics"
echo "  curl http://localhost:$SPAMMER_METRICS_PORT/metrics"
echo ""
echo "Press Ctrl+C to stop all services"
echo ""

# Wait for processes
wait
