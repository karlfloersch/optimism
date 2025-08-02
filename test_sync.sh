#!/bin/bash

set -e

echo "🚀 Starting op-node SYNC TEST"
echo "=============================="

# Generate a unique session ID for this test
SESSION_UUID=$(uuidgen)

# L1 and L1 Beacon endpoints (direct)
L1_ENDPOINT="https://sepolia.infura.io/v3/fce31f1fb2d54caa9b31ed7d28437fa5"
L1_BEACON_ENDPOINT="https://ethereum-sepolia-beacon-api.publicnode.com"

# L2 endpoint (via sync-tester proxy with session)
# latest=10, safe=5, finalized=1 (blocks behind real chain for controlled testing)
L2_ENDPOINT="http://127.0.0.1:9000/chain/11155420/synctest/${SESSION_UUID}?latest=10&safe=5&finalized=1"

echo "🔍 Starting op-node..."
echo "   L1: $L1_ENDPOINT"
echo "   L1 Beacon: $L1_BEACON_ENDPOINT"
echo "   L2: $L2_ENDPOINT (via sync-tester proxy)"
echo "   Session ID: $SESSION_UUID"
echo ""

# Check if sync-tester is running
echo "🔍 Checking if sync-tester is running..."
if curl -s -X POST -H "Content-Type: application/json" \
    --data '{"jsonrpc":"2.0","method":"eth_chainId","params":[],"id":1}' \
    http://127.0.0.1:9000/chain/11155420/synctest/test-session 2>/dev/null | grep -q "result"; then
    echo "✅ sync-tester is running on port 9000"
else
    echo "❌ sync-tester is NOT running on port 9000!"
    echo "   Please start it first:"
    echo "   ./op-sync-tester/bin/op-sync-tester --config=op-sync-tester/sepolia_config.yaml --rpc.addr=127.0.0.1 --rpc.port=9000 --log.level=info &"
    exit 1
fi
echo ""

# Clean up any existing op-node processes on ports 8550-8565
echo "🧹 Cleaning up existing op-node processes..."
for port in {8550..8565}; do
    lsof -ti:$port | xargs kill -9 2>/dev/null || true
done

# Use a unique port for this test
RPC_PORT=8553

echo "🔄 Starting op-node with sync-tester proxy (90 seconds test)..."

# Start op-node in background
go run ./op-node/cmd \
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

# Wait for initialization with periodic checks
echo "⏳ Waiting for op-node RPC to initialize..."
for i in {1..15}; do
    echo "   Attempt $i/15: Checking if op-node RPC is ready..."
    if curl -s -X POST -H "Content-Type: application/json" \
        --data '{"jsonrpc":"2.0","method":"eth_chainId","params":[],"id":1}' \
        http://127.0.0.1:$RPC_PORT 2>/dev/null | grep -q "result"; then
        echo "✅ op-node RPC is ready!"
        break
    fi
    if [ $i -eq 15 ]; then
        echo "❌ op-node RPC failed to initialize after 15 attempts"
        echo "📋 Recent op-node logs:"
        tail -10 op_node_sync_tester.log 2>/dev/null || echo "No log file found"
        exit 1
    fi
    sleep 3
done

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

# Let it sync with periodic status checks
echo "⏳ Syncing for 60 seconds with status checks..."
for i in {1..4}; do
    echo "   Status check $i/4 (after $((i*15)) seconds)..."
    
    # Check current block
    CURRENT_RESPONSE=$(curl -s -X POST -H "Content-Type: application/json" \
        --data '{"jsonrpc":"2.0","method":"eth_getBlockByNumber","params":["latest",false],"id":1}' \
        http://127.0.0.1:$RPC_PORT 2>/dev/null | jq -r '.result // empty')
    
    if [ -n "$CURRENT_RESPONSE" ] && [ "$CURRENT_RESPONSE" != "null" ]; then
        CURRENT_BLOCK_NUMBER=$(echo "$CURRENT_RESPONSE" | jq -r '.number // "0x0"')
        CURRENT_BLOCK_NUM_DEC=$(printf "%d" "$CURRENT_BLOCK_NUMBER" 2>/dev/null || echo "0")
        echo "   📊 Current L2 Block: #$CURRENT_BLOCK_NUM_DEC"
        
        # Calculate progress so far
        if [ "$STARTING_BLOCK_NUM_DEC" != "unknown" ] && [ "$CURRENT_BLOCK_NUM_DEC" -gt "$STARTING_BLOCK_NUM_DEC" ]; then
            PROGRESS=$((CURRENT_BLOCK_NUM_DEC - STARTING_BLOCK_NUM_DEC))
            echo "   📈 Progress: $PROGRESS blocks processed so far"
        fi
    else
        echo "   ❌ Failed to get current block status"
    fi
    
    sleep 15
done

# Capture ending L2 block state BEFORE stopping op-node
echo "🏁 Capturing ending L2 block state..."
ENDING_BLOCK_RESPONSE=$(curl -s -X POST -H "Content-Type: application/json" \
    --data '{"jsonrpc":"2.0","method":"eth_getBlockByNumber","params":["latest",false],"id":1}' \
    http://127.0.0.1:$RPC_PORT 2>/dev/null | jq -r '.result // empty')

# Stop op-node
echo "🛑 Stopping op-node..."
kill $OP_NODE_PID 2>/dev/null || true
sleep 2

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

# Show op-node logs
echo "📋 OP-NODE LOGS (last 30 lines)"
echo "==============================="
if [ -f "op_node_sync_tester.log" ]; then
    tail -30 op_node_sync_tester.log
else
    echo "❌ No op-node log file found"
fi

echo ""
echo "📋 SYNC-TESTER INTEGRATION TEST SUMMARY"
echo "======================================="
echo "Session ID: $SESSION_UUID"
echo "L2 Proxy: sync-tester with 10 block lag"
echo "Test Duration: ~105 seconds (45s init + 60s sync)"

# Show blocks processed prominently in summary
if [ "$STARTING_BLOCK_NUM_DEC" != "unknown" ] && [ "$ENDING_BLOCK_NUM_DEC" != "unknown" ] && [ "$ENDING_BLOCK_NUM_DEC" -gt "$STARTING_BLOCK_NUM_DEC" ]; then
    BLOCKS_PROCESSED=$((ENDING_BLOCK_NUM_DEC - STARTING_BLOCK_NUM_DEC))
    echo "✅ Blocks Processed: $BLOCKS_PROCESSED blocks"
    echo "✅ sync-tester integration: SUCCESS"
else
    echo "❌ Blocks Processed: Unable to calculate"
    echo "⚠️  sync-tester integration: May have issues"
fi

echo ""
echo "🎯 Next steps:"
echo "   - Compare sync performance vs direct connection"
echo "   - Test different session parameters (latest/safe/finalized)"
echo "   - Verify sync-tester validation logs"
echo ""

echo "📝 Log files:"
echo "   - op-node log: op_node_sync_tester.log"
