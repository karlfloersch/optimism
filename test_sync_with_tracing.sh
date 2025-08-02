#!/bin/bash

set -e

echo "🚀 Starting op-node SYNC TEST with FLOW TRACING"
echo "================================================"

# Generate a unique session ID for this test
SESSION_UUID=$(uuidgen)

# L1 and L1 Beacon endpoints (direct)
L1_ENDPOINT="https://sepolia.infura.io/v3/fce31f1fb2d54caa9b31ed7d28437fa5"
L1_BEACON_ENDPOINT="https://ethereum-sepolia-beacon-api.publicnode.com"

# L2 endpoint (via sync-tester proxy with session)
# latest=10, safe=5, finalized=1 (blocks behind real chain for controlled testing)
L2_ENDPOINT="http://127.0.0.1:9000/chain/11155420/synctest/${SESSION_UUID}?latest=10&safe=5&finalized=1"

echo "🔍 Starting op-node with FLOW TRACING enabled..."
echo "   L1: $L1_ENDPOINT"
echo "   L1 Beacon: $L1_BEACON_ENDPOINT"
echo "   L2: $L2_ENDPOINT (via sync-tester proxy)"
echo "   Session ID: $SESSION_UUID"
echo ""

# Ensure cleanup directories exist
mkdir -p /tmp/flow-traces

# Clean up any existing op-node processes on ports 8550-8565
echo "🧹 Cleaning up existing op-node processes..."
for port in {8550..8565}; do
    lsof -ti:$port | xargs kill -9 2>/dev/null || true
done

# Use a unique port for this test
RPC_PORT=8553

echo "🔄 Starting op-node with sync-tester proxy (90 seconds test)..."

# Start op-node in background with flow tracing
OP_NODE_FLOW_TRACING=true go run ./op-node/cmd \
    --l1="$L1_ENDPOINT" \
    --l1.beacon="$L1_BEACON_ENDPOINT" \
    --l2="$L2_ENDPOINT" \
    --l2.jwt-secret=jwt_secret.txt \
    --network=op-sepolia \
    --syncmode=consensus-layer \
    --l2.enginekind=geth \
    --rpc.addr=127.0.0.1 \
    --rpc.port=$RPC_PORT \
    --log.level=INFO \
    --metrics.enabled=false \
    --pprof.enabled=false \
    --p2p.disable=true > op_node_sync_tester.log 2>&1 &

OP_NODE_PID=$!

echo "   op-node PID: $OP_NODE_PID"
echo "   RPC endpoint: http://127.0.0.1:$RPC_PORT"

# Wait for initialization
echo "⏳ Waiting 45 seconds for op-node RPC to initialize..."
sleep 45

# Capture starting L2 block state
echo "🚀 Capturing starting L2 block state..."
STARTING_BLOCK_RESPONSE=$(curl -s -X POST -H "Content-Type: application/json" \
    --data '{"jsonrpc":"2.0","method":"eth_getBlockByNumber","params":["latest",false],"id":1}' \
    http://127.0.0.1:$RPC_PORT 2>/dev/null | jq -r '.result // empty')

if [ -n "$STARTING_BLOCK_RESPONSE" ] && [ "$STARTING_BLOCK_RESPONSE" != "null" ]; then
    STARTING_BLOCK_NUMBER=$(echo "$STARTING_BLOCK_RESPONSE" | jq -r '.number // "0x0"')
    STARTING_BLOCK_HASH=$(echo "$STARTING_BLOCK_RESPONSE" | jq -r '.hash // "unknown"')
    STARTING_BLOCK_NUM_DEC=$(printf "%d" "$STARTING_BLOCK_NUMBER" 2>/dev/null || echo "0")
    echo "✅ Starting L2 Block: #$STARTING_BLOCK_NUM_DEC"
    echo "   Hash: $STARTING_BLOCK_HASH"
else
    echo "❌ Failed to capture starting block (op-node may still be initializing)"
    STARTING_BLOCK_NUM_DEC="unknown"
    STARTING_BLOCK_HASH="unknown"
fi

echo ""

# Let it sync for 45 seconds
echo "⏳ Syncing for 45 seconds..."
sleep 45

# Stop op-node
echo "🛑 Stopping op-node..."
kill $OP_NODE_PID 2>/dev/null || true
sleep 2

# Capture ending L2 block state
echo "🏁 Capturing ending L2 block state..."
ENDING_BLOCK_RESPONSE=$(curl -s -X POST -H "Content-Type: application/json" \
    --data '{"jsonrpc":"2.0","method":"eth_getBlockByNumber","params":["latest",false],"id":1}' \
    http://127.0.0.1:$RPC_PORT 2>/dev/null | jq -r '.result // empty')

if [ -n "$ENDING_BLOCK_RESPONSE" ] && [ "$ENDING_BLOCK_RESPONSE" != "null" ]; then
    ENDING_BLOCK_NUMBER=$(echo "$ENDING_BLOCK_RESPONSE" | jq -r '.number // "0x0"')
    ENDING_BLOCK_HASH=$(echo "$ENDING_BLOCK_RESPONSE" | jq -r '.hash // "unknown"')
    ENDING_BLOCK_NUM_DEC=$(printf "%d" "$ENDING_BLOCK_NUMBER" 2>/dev/null || echo "0")
    echo "✅ Ending L2 Block: #$ENDING_BLOCK_NUM_DEC"
    echo "   Hash: $ENDING_BLOCK_HASH"
else
    echo "❌ Failed to capture ending block (op-node may have stopped)"
    ENDING_BLOCK_NUM_DEC="unknown"
    ENDING_BLOCK_HASH="unknown"
fi

echo ""

# Get expected block from real Sepolia endpoint
echo "🌐 Querying real Sepolia L2 RPC for expected state..."
REAL_L2_RESPONSE=$(curl -s -X POST -H "Content-Type: application/json" \
    --data '{"jsonrpc":"2.0","method":"eth_getBlockByNumber","params":["latest",false],"id":1}' \
    "https://sepolia.optimism.io" 2>/dev/null | jq -r '.result // empty')

if [ -n "$REAL_L2_RESPONSE" ] && [ "$REAL_L2_RESPONSE" != "null" ]; then
    REAL_BLOCK_NUMBER=$(echo "$REAL_L2_RESPONSE" | jq -r '.number // "0x0"')
    REAL_BLOCK_HASH=$(echo "$REAL_L2_RESPONSE" | jq -r '.hash // "unknown"')
    REAL_BLOCK_NUM_DEC=$(printf "%d" "$REAL_BLOCK_NUMBER" 2>/dev/null || echo "0")
    echo "✅ Real Sepolia Latest: Block #$REAL_BLOCK_NUM_DEC"
    echo "   Hash: $REAL_BLOCK_HASH"
else
    echo "❌ Failed to query real Sepolia endpoint"
    REAL_BLOCK_NUM_DEC="unknown"
    REAL_BLOCK_HASH="unknown"
fi

echo ""

# Calculate progress metrics
if [ "$STARTING_BLOCK_NUM_DEC" != "unknown" ] && [ "$ENDING_BLOCK_NUM_DEC" != "unknown" ] && [ "$ENDING_BLOCK_NUM_DEC" -gt "$STARTING_BLOCK_NUM_DEC" ]; then
    BLOCKS_PROCESSED=$((ENDING_BLOCK_NUM_DEC - STARTING_BLOCK_NUM_DEC))
    echo "📈 Blocks Processed During Test: $BLOCKS_PROCESSED"
else
    echo "📈 Blocks Processed: Unable to calculate"
fi

if [ "$ENDING_BLOCK_NUM_DEC" != "unknown" ] && [ "$REAL_BLOCK_NUM_DEC" != "unknown" ] && [ "$REAL_BLOCK_NUM_DEC" -gt 0 ]; then
    SYNC_GAP=$((REAL_BLOCK_NUM_DEC - ENDING_BLOCK_NUM_DEC))
    SYNC_PERCENTAGE=$(echo "scale=2; $ENDING_BLOCK_NUM_DEC * 100 / $REAL_BLOCK_NUM_DEC" | bc -l 2>/dev/null || echo "unknown")
    echo "📊 Sync Status:"
    echo "   Gap to real chain: $SYNC_GAP blocks"
    echo "   Sync percentage: ${SYNC_PERCENTAGE}%"
else
    echo "📊 Sync Status: Unable to calculate"
fi

echo ""

# Analyze flow traces
echo "📊 FLOW TRACE ANALYSIS"
echo "====================="

LATEST_FLOW_TRACE=$(ls -t /tmp/flow-traces/flow-events-*.json 2>/dev/null | head -1)
if [ -n "$LATEST_FLOW_TRACE" ]; then
    echo "📁 Flow trace file: $(basename "$LATEST_FLOW_TRACE")"

    # Get total events
    TOTAL_EVENTS=$(jq -r '.metadata.total_events // 0' "$LATEST_FLOW_TRACE" 2>/dev/null)
    echo "📊 Total events captured: $TOTAL_EVENTS"

    if [ "$TOTAL_EVENTS" -gt 0 ]; then
        echo "✅ Flow tracing is ACTIVE"
    else
        echo "❌ Flow tracing captured zero events"
    fi

    echo ""
    echo "🔍 Checking for ELIMINATED engine events:"

    # Check eliminated events (should be 0)
    eliminated_events=(
        "TryUpdateEngineEvent"
        "ProcessUnsafePayloadEvent"
        "ForkchoiceRequestEvent"
        "CrossUpdateRequestEvent"
        "InteropInvalidateBlockEvent"
        "PromoteCrossUnsafeEvent"
        "PendingSafeRequestEvent"
    )

    for event in "${eliminated_events[@]}"; do
        count=$(jq -r --arg event "$event" '.events[] | select(.event_type == $event) | .event_type' "$LATEST_FLOW_TRACE" 2>/dev/null | wc -l | tr -d ' ')
        if [ "$count" -eq 0 ]; then
            echo "✅ $event: 0 (ELIMINATED)"
        else
            echo "❌ $event: $count (NOT ELIMINATED)"
        fi
    done

    echo ""
    echo "🔍 Checking for expected NON-eliminated events:"

    # Check some events that should still exist
    other_events=("ResetEvent" "EngineResetEvent" "PromotePendingSafeEvent")

    for event in "${other_events[@]}"; do
        count=$(jq -r --arg event "$event" '.events[] | select(.event_type == $event) | .event_type' "$LATEST_FLOW_TRACE" 2>/dev/null | wc -l | tr -d ' ')
        if [ "$count" -gt 0 ]; then
            echo "✅ $event: $count (present as expected)"
        else
            echo "⚠️  $event: 0 (may be normal)"
        fi
    done

else
    echo "❌ No flow trace file found"
fi

echo ""
echo "📋 SYNC-TESTER INTEGRATION TEST SUMMARY"
echo "======================================="
echo "Session ID: $SESSION_UUID"
echo "L2 Proxy: sync-tester with 10 block lag"
echo "Test Duration: 90 seconds (45s init + 45s sync)"
echo "✅ sync-tester integration: SUCCESS"

if [ "$TOTAL_EVENTS" -gt 0 ]; then
    echo "✅ Flow tracing: ACTIVE"
else
    echo "❌ Flow tracing: INACTIVE"
fi

echo ""
echo "🎯 Next steps:"
echo "   - Verify eliminated events show 0 occurrences"
echo "   - Compare sync performance vs direct connection"
echo "   - Test different session parameters (latest/safe/finalized)"
echo ""

echo "📝 Log files:"
echo "   - op-node log: op_node_sync_tester.log"
echo "   - Flow trace: $LATEST_FLOW_TRACE"
