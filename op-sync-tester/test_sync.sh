#!/bin/bash

set -e

echo "🚀 Starting op-node sync test"
echo "============================="

# Generate a unique session ID for this test
SESSION_UUID=$(uuidgen)

# L1 and L1 Beacon endpoints from environment variables with defaults
L1_ENDPOINT="${L1_ENDPOINT:-https://sepolia.infura.io/v3/YOUR_INFURA_KEY}"
L1_BEACON_ENDPOINT="${L1_BEACON_ENDPOINT:-https://ethereum-sepolia-beacon-api.publicnode.com}"
L2_RPC_ENDPOINT="${L2_RPC_ENDPOINT:-https://optimism-sepolia.infura.io/v3/YOUR_INFURA_KEY}"

# Check for required environment variables
if [[ "$L1_ENDPOINT" == *"YOUR_INFURA_KEY"* ]]; then
    echo "❌ Please set L1_ENDPOINT environment variable with your Infura key"
    echo "   Example: export L1_ENDPOINT='https://sepolia.infura.io/v3/YOUR_KEY'"
    exit 1
fi

if [[ "$L2_RPC_ENDPOINT" == *"YOUR_INFURA_KEY"* ]]; then
    echo "❌ Please set L2_RPC_ENDPOINT environment variable with your Infura key"
    echo "   Example: export L2_RPC_ENDPOINT='https://optimism-sepolia.infura.io/v3/YOUR_KEY'"
    exit 1
fi

# L2 endpoint (via sync-tester proxy with session)
# latest=10, safe=5, finalized=1 (blocks behind real chain for controlled testing)
L2_ENDPOINT="http://127.0.0.1:9000/chain/11155420/synctest/${SESSION_UUID}?latest=10&safe=5&finalized=1"

echo "🔧 Configuration:"
echo "   L1: $L1_ENDPOINT"
echo "   L1 Beacon: $L1_BEACON_ENDPOINT"
echo "   L2 Backend: $L2_RPC_ENDPOINT"
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

go run ../op-node/cmd \
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

# Capture starting block state
STARTING_BLOCK_RESPONSE=$(curl -s -X POST -H "Content-Type: application/json" \
    --data '{"jsonrpc":"2.0","method":"eth_getBlockByNumber","params":["latest",false],"id":1}' \
    "$L2_ENDPOINT" 2>/dev/null | jq -r '.result // empty')

if [ -n "$STARTING_BLOCK_RESPONSE" ] && [ "$STARTING_BLOCK_RESPONSE" != "null" ]; then
    STARTING_BLOCK_NUMBER=$(echo "$STARTING_BLOCK_RESPONSE" | jq -r '.number // "0x0"')
    STARTING_BLOCK_NUM_DEC=$(printf "%d" "$STARTING_BLOCK_NUMBER" 2>/dev/null || echo "0")
    echo "📊 Starting block: #$STARTING_BLOCK_NUM_DEC"
else
    echo "❌ Failed to get starting block"
    STARTING_BLOCK_NUM_DEC="unknown"
fi

# Sync with status checks
echo "⏳ Syncing for 60 seconds..."
for i in {1..4}; do
    # Check current block
    CURRENT_RESPONSE=$(curl -s -X POST -H "Content-Type: application/json" \
        --data '{"jsonrpc":"2.0","method":"eth_getBlockByNumber","params":["latest",false],"id":1}' \
        "$L2_ENDPOINT" 2>/dev/null | jq -r '.result // empty')
    
    if [ -n "$CURRENT_RESPONSE" ] && [ "$CURRENT_RESPONSE" != "null" ]; then
        CURRENT_BLOCK_NUMBER=$(echo "$CURRENT_RESPONSE" | jq -r '.number // "0x0"')
        CURRENT_BLOCK_NUM_DEC=$(printf "%d" "$CURRENT_BLOCK_NUMBER" 2>/dev/null || echo "0")
        
        if [ "$STARTING_BLOCK_NUM_DEC" != "unknown" ] && [ "$CURRENT_BLOCK_NUM_DEC" -gt "$STARTING_BLOCK_NUM_DEC" ]; then
            PROGRESS=$((CURRENT_BLOCK_NUM_DEC - STARTING_BLOCK_NUM_DEC))
            echo "   Progress: $PROGRESS blocks (current: #$CURRENT_BLOCK_NUM_DEC)"
        fi
    fi
    
    sleep 15
done

# Capture ending block state
ENDING_BLOCK_RESPONSE=$(curl -s -X POST -H "Content-Type: application/json" \
    --data '{"jsonrpc":"2.0","method":"eth_getBlockByNumber","params":["latest",false],"id":1}' \
    "$L2_ENDPOINT" 2>/dev/null | jq -r '.result // empty')

# Stop processes
echo "🛑 Stopping processes..."
kill $SYNC_TESTER_PID $OP_NODE_PID 2>/dev/null || true
sleep 2

# Calculate results
if [ -n "$ENDING_BLOCK_RESPONSE" ] && [ "$ENDING_BLOCK_RESPONSE" != "null" ]; then
    ENDING_BLOCK_NUMBER=$(echo "$ENDING_BLOCK_RESPONSE" | jq -r '.number // "0x0"')
    ENDING_BLOCK_NUM_DEC=$(printf "%d" "$ENDING_BLOCK_NUMBER" 2>/dev/null || echo "0")
    echo "📊 Ending block: #$ENDING_BLOCK_NUM_DEC"
else
    echo "❌ Failed to get ending block"
    ENDING_BLOCK_NUM_DEC="unknown"
fi

echo ""
echo "📋 SYNC TEST RESULTS"
echo "===================="
echo "Session ID: $SESSION_UUID"

# Calculate blocks processed
if [ "$STARTING_BLOCK_NUM_DEC" != "unknown" ] && [ "$ENDING_BLOCK_NUM_DEC" != "unknown" ] && [ "$ENDING_BLOCK_NUM_DEC" -gt "$STARTING_BLOCK_NUM_DEC" ]; then
    BLOCKS_PROCESSED=$((ENDING_BLOCK_NUM_DEC - STARTING_BLOCK_NUM_DEC))
    echo "✅ Blocks processed: $BLOCKS_PROCESSED"
    echo "✅ Test completed successfully"
else
    echo "❌ Unable to calculate blocks processed"
    echo "⚠️  Test may have encountered issues"
fi

echo ""
echo "📝 Log files:"
echo "   - sync-tester: sync_tester.log"
echo "   - op-node: op_node.log"
