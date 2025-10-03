#!/bin/bash
# Script to run the sync tester external EL test
# This test validates op-node syncing against external execution layer endpoints

set -e

# Configuration
TEST_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOG_FILE="${TEST_DIR}/test_run_$(date +%Y%m%d_%H%M%S).log"
TIP_MODE_RPC="${OP_NODE_TIP_MODE_RPC:-}"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo "=========================================="
echo "OP Stack Sync Tester - External EL"
echo "=========================================="
echo "Test Directory: ${TEST_DIR}"
echo "Log File: ${LOG_FILE}"
if [ -n "${TIP_MODE_RPC}" ]; then
    echo -e "${YELLOW}Tip Mode: ENABLED${NC}"
    echo "Tip Mode RPC: ${TIP_MODE_RPC}"
else
    echo "Tip Mode: DISABLED (standard derivation)"
fi
echo "=========================================="
echo ""

cd "${TEST_DIR}"

# Run the test
echo "Starting test..."
CIRCLECI_PARAMETERS_SYNC_TEST_OP_NODE_DISPATCH=true \
  TAILSCALE_NETWORKING=true \
  NETWORK_PRESET=op-sepolia \
  GOMAXPROCS=5 \
  OP_NODE_TIP_MODE_RPC="${TIP_MODE_RPC}" \
  go test -run '^TestSyncTesterExtEL$' -v -count=1 2>&1 | tee "${LOG_FILE}"

# Check exit code
EXIT_CODE=${PIPESTATUS[0]}

echo ""
echo "=========================================="
if [ ${EXIT_CODE} -eq 0 ]; then
    echo -e "${GREEN}TEST PASSED ✓${NC}"
else
    echo -e "${RED}TEST FAILED ✗${NC}"
fi
echo "Exit Code: ${EXIT_CODE}"
echo "Log File: ${LOG_FILE}"
echo "=========================================="

exit ${EXIT_CODE}
