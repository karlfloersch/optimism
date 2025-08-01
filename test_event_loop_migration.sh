#!/bin/bash

echo "🧪 EVENT LOOP MIGRATION TESTING PIPELINE"
echo "========================================"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Test configuration
TRACE_DIR="/tmp/flow_traces"
BASELINE_TRACE=""
CURRENT_TRACE=""

function run_devstack_test() {
    local test_name=$1
    echo -e "${BLUE}🧪 Running $test_name...${NC}"
    
    # Enable flow tracing for comparison
    export OP_NODE_FLOW_TRACING=true
    
    # Run the op-node flow integration test
    cd op-node/flow
    if go test -run TestEnvironmentVariableIntegration -timeout 2m; then
        echo -e "${GREEN}✅ $test_name PASSED${NC}"
        
        # Capture the trace file
        CURRENT_TRACE=$(ls $TRACE_DIR/*.json | tail -1)
        echo "   📊 Trace captured: $CURRENT_TRACE"
        return 0
    else
        echo -e "${RED}❌ $test_name FAILED${NC}"
        return 1
    fi
}

function analyze_trace() {
    local trace_file=$1
    local analysis_name=$2
    
    echo -e "${BLUE}📊 Analyzing $analysis_name trace...${NC}"
    
    cd op-node/flow
    if go run cmd/analyzer.go $trace_file > "${analysis_name}_analysis.txt"; then
        echo -e "${GREEN}✅ Analysis complete: ${analysis_name}_analysis.txt${NC}"
        
        # Extract key metrics
        local total_events=$(grep "Total Events:" "${analysis_name}_analysis.txt" | cut -d: -f2 | tr -d ' ')
        local forkchoice_events=$(grep "forkchoice-update" "${analysis_name}_analysis.txt" | wc -l)
        local engine_events=$(grep "try-update-engine" "${analysis_name}_analysis.txt" | wc -l)
        
        echo "   📈 Total Events: $total_events"
        echo "   ⚡ Forkchoice Events: $forkchoice_events"
        echo "   🎯 Engine Events: $engine_events"
        
        return 0
    else
        echo -e "${RED}❌ Analysis failed${NC}"
        return 1
    fi
}

function compare_traces() {
    local baseline=$1
    local current=$2
    
    echo -e "${BLUE}🔍 Comparing causal patterns...${NC}"
    
    if [ ! -f "$baseline" ] || [ ! -f "$current" ]; then
        echo -e "${YELLOW}⚠️  Missing trace files for comparison${NC}"
        return 1
    fi
    
    # Compare causal patterns (ignore timestamps)
    cd op-node/flow
    go run cmd/analyzer.go $baseline | grep -v "timestamp" > baseline_patterns.tmp
    go run cmd/analyzer.go $current | grep -v "timestamp" > current_patterns.tmp
    
    if diff baseline_patterns.tmp current_patterns.tmp > /dev/null; then
        echo -e "${GREEN}✅ IDENTICAL CAUSAL PATTERNS${NC}"
        echo "   🎉 Event loop migration preserved behavior!"
        rm baseline_patterns.tmp current_patterns.tmp
        return 0
    else
        echo -e "${RED}❌ CAUSAL PATTERNS DIFFER${NC}"
        echo "   🔍 Differences:"
        diff baseline_patterns.tmp current_patterns.tmp | head -10
        echo "   💡 Review controller implementation"
        rm baseline_patterns.tmp current_patterns.tmp
        return 1
    fi
}

function validate_performance() {
    local baseline_analysis=$1
    local current_analysis=$2
    
    echo -e "${BLUE}📊 Performance validation...${NC}"
    
    if [ ! -f "$baseline_analysis" ] || [ ! -f "$current_analysis" ]; then
        echo -e "${YELLOW}⚠️  Missing analysis files${NC}"
        return 1
    fi
    
    local baseline_events=$(grep "Total Events:" $baseline_analysis | cut -d: -f2 | tr -d ' ')
    local current_events=$(grep "Total Events:" $current_analysis | cut -d: -f2 | tr -d ' ')
    
    if [ "$baseline_events" -eq "$current_events" ]; then
        echo -e "${GREEN}✅ Event count unchanged: $current_events${NC}"
    elif [ "$current_events" -lt "$baseline_events" ]; then
        local saved=$((baseline_events - current_events))
        echo -e "${GREEN}🚀 PERFORMANCE IMPROVEMENT: $saved fewer events!${NC}"
    else
        local increase=$((current_events - baseline_events))
        echo -e "${YELLOW}⚠️  Event count increased by $increase${NC}"
    fi
}

# Main test execution
function main() {
    echo "Starting event loop migration testing pipeline..."
    
    # Step 1: Clean trace directory
    mkdir -p $TRACE_DIR
    rm -f $TRACE_DIR/*.json
    
    # Step 2: Run baseline test (before migration)
    echo -e "\n${YELLOW}🏁 BASELINE TEST (before migration)${NC}"
    if run_devstack_test "Baseline"; then
        BASELINE_TRACE=$CURRENT_TRACE
        analyze_trace $BASELINE_TRACE "baseline"
    else
        echo -e "${RED}❌ Baseline test failed - stopping pipeline${NC}"
        exit 1
    fi
    
    # Step 3: Run current test (after migration changes)
    echo -e "\n${YELLOW}🔄 CURRENT TEST (after migration)${NC}"
    if run_devstack_test "Current"; then
        analyze_trace $CURRENT_TRACE "current"
    else
        echo -e "${RED}❌ Current test failed${NC}"
        exit 1
    fi
    
    # Step 4: Compare and validate
    echo -e "\n${YELLOW}🔍 VALIDATION${NC}"
    compare_traces $BASELINE_TRACE $CURRENT_TRACE
    validate_performance "baseline_analysis.txt" "current_analysis.txt"
    
    echo -e "\n${GREEN}🎉 TESTING PIPELINE COMPLETE${NC}"
}

# Handle script arguments
case "${1:-main}" in
    "baseline")
        run_devstack_test "Baseline"
        ;;
    "current")
        run_devstack_test "Current"
        ;;
    "compare")
        compare_traces $2 $3
        ;;
    "main"|"")
        main
        ;;
    *)
        echo "Usage: $0 [baseline|current|compare|main]"
        exit 1
        ;;
esac