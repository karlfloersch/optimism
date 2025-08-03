#!/bin/bash

set -e

# Usage function
usage() {
    echo "Usage: $0 <L1_RPC_URL> <L2_RPC_URL> <L1_BEACON_URL> [DURATION_SECONDS]"
    echo ""
    echo "Examples:"
    echo "  $0 'https://sepolia.infura.io/v3/YOUR_KEY' 'https://optimism-sepolia.infura.io/v3/YOUR_KEY' 'https://chaotic-dark-river.ethereum-sepolia.quiknode.pro/cfce48bb8605b8c9f7ca515e0cfd17c562b61112/'"
    echo "  $0 'https://sepolia.infura.io/v3/YOUR_KEY' 'https://optimism-sepolia.infura.io/v3/YOUR_KEY' 'https://ethereum-sepolia-beacon-api.publicnode.com' 120"
    echo ""
    echo "Arguments:"
    echo "  L1_RPC_URL      - Ethereum Sepolia RPC endpoint (required)"
    echo "  L2_RPC_URL      - Optimism Sepolia RPC endpoint (required)"
    echo "  L1_BEACON_URL   - Ethereum Sepolia beacon API endpoint (required)"
    echo "  DURATION_SECONDS - Test duration in seconds (optional, default: 60)"
    exit 1
}

# Check command line arguments
if [ $# -lt 3 ]; then
    echo "❌ Error: Missing required arguments"
    echo ""
    usage
fi

echo "🚀 Starting op-node sync test"
echo "============================="

# Generate a unique session ID for this test
SESSION_UUID=$(uuidgen)

# RPC endpoints and configuration from command line arguments
L1_ENDPOINT="$1"
L2_RPC_ENDPOINT="$2"
L1_BEACON_ENDPOINT="$3"
DURATION_SECONDS="${4:-60}"

# L2 endpoint (via sync-tester proxy with session)
# Testing with 100 block offset (was failing before context fix)
L2_ENDPOINT="http://127.0.0.1:9000/chain/11155420/synctest/${SESSION_UUID}?latest=2000&safe=2000&finalized=2100"

echo "🔧 Configuration:"
echo "   L1: $L1_ENDPOINT"
echo "   L1 Beacon: $L1_BEACON_ENDPOINT"
echo "   L2 Backend: $L2_RPC_ENDPOINT"
echo "   Duration: ${DURATION_SECONDS}s"
echo "   Session ID: $SESSION_UUID"
echo ""

# Clean up any existing processes
echo "🧹 Cleaning up existing processes..."
pkill -f "op-sync-tester" 2>/dev/null || true
pkill -f "op-node" 2>/dev/null || true
for port in {8550..8565} 9000; do
    lsof -ti:$port | xargs kill -9 2>/dev/null || true
done
sleep 2

# Start sync-tester
echo "🚀 Starting sync-tester..."
if [ ! -f "bin/op-sync-tester" ]; then
    echo "   Building sync-tester..."
    go build -o bin/op-sync-tester ./cmd
fi

# Update config with environment L2 RPC endpoint
cat > sepolia_config.yaml << EOF
synctesters:
  sepolia:
    chain_id: 11155420
    el_rpc: $L2_RPC_ENDPOINT
EOF

./bin/op-sync-tester \
    --config=sepolia_config.yaml \
    --rpc.addr=127.0.0.1 \
    --rpc.port=9000 \
    --log.level=info > sync_tester.log 2>&1 &

SYNC_TESTER_PID=$!
echo "   sync-tester PID: $SYNC_TESTER_PID"

# Wait for sync-tester to start
echo "⏳ Waiting for sync-tester to initialize..."
for i in {1..10}; do
    if curl -s -X POST -H "Content-Type: application/json" \
        --data '{"jsonrpc":"2.0","method":"eth_chainId","params":[],"id":1}' \
        "http://127.0.0.1:9000/chain/11155420/synctest/$(uuidgen)?latest=1&safe=1&finalized=1" 2>/dev/null | grep -q "result"; then
        echo "✅ sync-tester is ready"
        break
    fi
    if [ $i -eq 10 ]; then
        echo "❌ sync-tester failed to start"
        kill $SYNC_TESTER_PID 2>/dev/null || true
        exit 1
    fi
    sleep 2
done
echo ""

# Start op-node
echo "🚀 Starting op-node..."
RPC_PORT=8553

# Create JWT secret if it doesn't exist
if [ ! -f "../jwt_secret.txt" ]; then
    echo "   Creating JWT secret..."
    openssl rand -hex 32 > ../jwt_secret.txt
fi

# Build op-node binary if it doesn't exist or is older than source
if [ ! -f "../op-node-bin" ] || [ ../op-node/cmd/main.go -nt "../op-node-bin" ]; then
    echo "   Building op-node binary..."
    go build -o ../op-node-bin ../op-node/cmd
fi

# Set up flow tracing for event migration work
echo "   Setting up event flow tracing..."
mkdir -p /tmp/flow-traces

../op-node-bin \
    --flow-tracing \
    --l1="$L1_ENDPOINT" \
    --l1.beacon="$L1_BEACON_ENDPOINT" \
    --l2="$L2_ENDPOINT" \
    --l2.jwt-secret=../jwt_secret.txt \
    --network=op-sepolia \
    --syncmode=consensus-layer \
    --l2.enginekind=geth \
    --rpc.addr=127.0.0.1 \
    --rpc.port=$RPC_PORT \
    --log.level=INFO \
    --metrics.enabled=false \
    --pprof.enabled=false \
    --p2p.disable=true > op_node.log 2>&1 &

OP_NODE_PID=$!
echo "   op-node PID: $OP_NODE_PID"

# Wait for op-node to initialize
echo "⏳ Waiting for op-node to initialize..."
for i in {1..15}; do
    if curl -s -X POST -H "Content-Type: application/json" \
        --data '{"jsonrpc":"2.0","method":"health_status","params":[],"id":1}' \
        http://127.0.0.1:$RPC_PORT 2>/dev/null | grep -q "result"; then
        echo "✅ op-node is ready"
        break
    fi
    if [ $i -eq 15 ]; then
        echo "❌ op-node failed to initialize"
        tail -10 op_node.log 2>/dev/null || echo "No log file found"
        kill $SYNC_TESTER_PID $OP_NODE_PID 2>/dev/null || true
        exit 1
    fi
    sleep 3
done

# Function to get block number for different head types
get_block_number() {
    local head_type="$1"
    local response=$(curl -s -X POST -H "Content-Type: application/json" \
        --data "{\"jsonrpc\":\"2.0\",\"method\":\"eth_getBlockByNumber\",\"params\":[\"$head_type\",false],\"id\":1}" \
        "$L2_ENDPOINT" 2>/dev/null | jq -r '.result // empty')

    if [ -n "$response" ] && [ "$response" != "null" ]; then
        echo "$response" | jq -r '.number // "0x0"' | xargs printf "%d" 2>/dev/null || echo "0"
    else
        echo "unknown"
    fi
}

# Capture starting state for all head types
echo "📊 Capturing starting head positions..."
STARTING_LATEST=$(get_block_number "latest")
STARTING_SAFE=$(get_block_number "safe")
STARTING_FINALIZED=$(get_block_number "finalized")

echo "   Latest head: #$STARTING_LATEST"
echo "   Safe head: #$STARTING_SAFE"
echo "   Finalized head: #$STARTING_FINALIZED"

# Wait for L1 walkback phase to complete
echo "⏳ Waiting for L1 walkback phase to complete..."
echo "   (This may take a while with 2000 block offset)"

WALKBACK_START=$(date +%s)
WALKBACK_TIMEOUT=300  # 5 minutes timeout for walkback

while true; do
    # Check if we're still in walkback phase
    WALKBACK_COUNT=$(tail -20 op_node.log 2>/dev/null | grep -c "Walking back L1Block by hash" 2>/dev/null || echo "0")
    WALKBACK_COUNT=$(echo "$WALKBACK_COUNT" | tr -d '\n' | head -1)

    # If no recent walkback messages, check if derivation has started
    if [ "$WALKBACK_COUNT" -eq 0 ]; then
        sleep 5  # Wait a bit more to be sure
        RECENT_WALKBACK=$(tail -30 op_node.log 2>/dev/null | grep -c "Walking back L1Block by hash" 2>/dev/null || echo "0")
        RECENT_WALKBACK=$(echo "$RECENT_WALKBACK" | tr -d '\n' | head -1)

        if [ "$RECENT_WALKBACK" -eq 0 ]; then
            echo "✅ L1 walkback phase completed!"
            break
        fi
    fi

    # Check timeout
    CURRENT_TIME=$(date +%s)
    ELAPSED=$((CURRENT_TIME - WALKBACK_START))
    if [ $ELAPSED -gt $WALKBACK_TIMEOUT ]; then
        echo "⚠️  Walkback timeout reached (${WALKBACK_TIMEOUT}s) - proceeding anyway"
        break
    fi

    # Show progress every 30 seconds
    if [ $((ELAPSED % 30)) -eq 0 ] && [ $ELAPSED -gt 0 ]; then
        RECENT_L2_BLOCK=$(tail -5 op_node.log 2>/dev/null | grep "Walking back L1Block" | tail -1 | grep -o "l2block=0x[a-f0-9]*:[0-9]*" | cut -d: -f2 || echo "unknown")
        echo "   Still walking back... (${ELAPSED}s elapsed, L2 block: $RECENT_L2_BLOCK)"
    fi

    sleep 3
done

# Record start time for performance calculation (after walkback completes)
START_TIME=$(date +%s)

# Sync with status checks
echo "⏳ Now syncing for ${DURATION_SECONDS} seconds..."
CHECKS=$((DURATION_SECONDS / 15))
if [ $CHECKS -lt 1 ]; then CHECKS=1; fi
for i in $(seq 1 $CHECKS); do
    echo "   📊 Status check $i/$CHECKS (after $((i*15))s):"

    # Get current head positions
    CURRENT_LATEST=$(get_block_number "latest")
    CURRENT_SAFE=$(get_block_number "safe")
    CURRENT_FINALIZED=$(get_block_number "finalized")

    # Calculate progress for each head type
    if [ "$STARTING_LATEST" != "unknown" ] && [ "$CURRENT_LATEST" != "unknown" ] && [ "$CURRENT_LATEST" -gt "$STARTING_LATEST" ]; then
        LATEST_PROGRESS=$((CURRENT_LATEST - STARTING_LATEST))
        echo "      Latest: +$LATEST_PROGRESS blocks (#$CURRENT_LATEST)"
    else
        echo "      Latest: #$CURRENT_LATEST"
    fi

    if [ "$STARTING_SAFE" != "unknown" ] && [ "$CURRENT_SAFE" != "unknown" ] && [ "$CURRENT_SAFE" -gt "$STARTING_SAFE" ]; then
        SAFE_PROGRESS=$((CURRENT_SAFE - STARTING_SAFE))
        echo "      Safe: +$SAFE_PROGRESS blocks (#$CURRENT_SAFE)"
    else
        echo "      Safe: #$CURRENT_SAFE"
    fi

    if [ "$STARTING_FINALIZED" != "unknown" ] && [ "$CURRENT_FINALIZED" != "unknown" ] && [ "$CURRENT_FINALIZED" -gt "$STARTING_FINALIZED" ]; then
        FINALIZED_PROGRESS=$((CURRENT_FINALIZED - STARTING_FINALIZED))
        echo "      Finalized: +$FINALIZED_PROGRESS blocks (#$CURRENT_FINALIZED)"
    else
        echo "      Finalized: #$CURRENT_FINALIZED"
    fi

    sleep 15
done

# Sleep for any remaining time
ELAPSED_TIME=$((CHECKS * 15))
REMAINING_TIME=$((DURATION_SECONDS - ELAPSED_TIME))
if [ $REMAINING_TIME -gt 0 ]; then
    echo "   ⏱️  Waiting for remaining ${REMAINING_TIME}s..."
    sleep $REMAINING_TIME
fi

# Capture ending state for all head types
echo "📊 Capturing ending head positions..."
ENDING_LATEST=$(get_block_number "latest")
ENDING_SAFE=$(get_block_number "safe")
ENDING_FINALIZED=$(get_block_number "finalized")

echo "   Latest head: #$ENDING_LATEST"
echo "   Safe head: #$ENDING_SAFE"
echo "   Finalized head: #$ENDING_FINALIZED"

# Record end time for performance calculation
END_TIME=$(date +%s)

# Stop processes
echo "🛑 Stopping processes..."
kill $SYNC_TESTER_PID $OP_NODE_PID 2>/dev/null || true
sleep 2

echo ""
echo "📋 SYNC TEST RESULTS"
echo "===================="
echo "Session ID: $SESSION_UUID"
echo "Starting position: 1000 blocks behind (unsafe), 5000 behind (safe), 5024 behind (finalized)"
echo ""

# Calculate progress for each head type
DURATION_SECONDS=$((END_TIME - START_TIME))

echo "📈 HEAD PROGRESSION:"

# Latest head progress
if [ "$STARTING_LATEST" != "unknown" ] && [ "$ENDING_LATEST" != "unknown" ] && [ "$ENDING_LATEST" -gt "$STARTING_LATEST" ]; then
    LATEST_BLOCKS=$((ENDING_LATEST - STARTING_LATEST))
    LATEST_RATE=$(echo "scale=2; $LATEST_BLOCKS / $DURATION_SECONDS" | bc -l 2>/dev/null || echo "unknown")
    echo "   🟢 Latest head: +$LATEST_BLOCKS blocks (${LATEST_RATE}/sec)"
else
    echo "   🔴 Latest head: Unable to calculate"
fi

# Safe head progress
if [ "$STARTING_SAFE" != "unknown" ] && [ "$ENDING_SAFE" != "unknown" ] && [ "$ENDING_SAFE" -gt "$STARTING_SAFE" ]; then
    SAFE_BLOCKS=$((ENDING_SAFE - STARTING_SAFE))
    SAFE_RATE=$(echo "scale=2; $SAFE_BLOCKS / $DURATION_SECONDS" | bc -l 2>/dev/null || echo "unknown")
    echo "   🟡 Safe head: +$SAFE_BLOCKS blocks (${SAFE_RATE}/sec)"
else
    echo "   🔴 Safe head: Unable to calculate"
fi

# Finalized head progress
if [ "$STARTING_FINALIZED" != "unknown" ] && [ "$ENDING_FINALIZED" != "unknown" ] && [ "$ENDING_FINALIZED" -gt "$STARTING_FINALIZED" ]; then
    FINALIZED_BLOCKS=$((ENDING_FINALIZED - STARTING_FINALIZED))
    FINALIZED_RATE=$(echo "scale=2; $FINALIZED_BLOCKS / $DURATION_SECONDS" | bc -l 2>/dev/null || echo "unknown")
    echo "   🔵 Finalized head: +$FINALIZED_BLOCKS blocks (${FINALIZED_RATE}/sec)"
else
    echo "   🔴 Finalized head: Unable to calculate"
fi

# Validation metrics
echo ""
echo "🔍 VALIDATION METRICS:"
COMPREHENSIVE_VALIDATIONS=$(grep -c "Comprehensive payload validation" sync_tester.log 2>/dev/null || echo "0")
PERFECT_MATCHES=$(grep -c "matches real Sepolia block perfectly" sync_tester.log 2>/dev/null || echo "0")
VALIDATION_ERRORS=$(grep -c "Payload does not match real Sepolia block" sync_tester.log 2>/dev/null || echo "0")
FORKCHOICE_VALIDATIONS=$(grep -c "ForkchoiceUpdated validation successful" sync_tester.log 2>/dev/null || echo "0")

echo "   🔬 Comprehensive validations: $COMPREHENSIVE_VALIDATIONS"
echo "   ✅ Perfect payload matches: $PERFECT_MATCHES"
echo "   ❌ Validation errors: $VALIDATION_ERRORS"
echo "   🔄 ForkchoiceUpdated calls: $FORKCHOICE_VALIDATIONS"

echo ""
echo "⏱️  Total duration: ${DURATION_SECONDS}s"

# Overall success check
if [ "$LATEST_BLOCKS" -gt 0 ] 2>/dev/null; then
    echo "✅ Test completed successfully"
else
    echo "⚠️  Test may have encountered issues"
fi

echo ""
echo "📝 Log files:"
echo "   - sync-tester: sync_tester.log"
echo "   - op-node: op_node.log"
echo "   - flow traces: /tmp/flow-traces/"
