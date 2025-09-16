#!/usr/bin/env bash

set -euo pipefail

# Load helper vars (discovers container name and ports)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1090
source "$SCRIPT_DIR/variables.sh"

# Parse and validate desired state
RAW_INPUT="${1:-}"
if [ -z "$RAW_INPUT" ] || [ "$RAW_INPUT" = "-h" ] || [ "$RAW_INPUT" = "--help" ]; then
    echo "Usage: $0 <true|false>"
    echo "Example: $0 true    # enable checkAccessList"
    echo "         $0 false   # disable checkAccessList"
    exit 1
fi

VALUE_LOWER=$(echo "$RAW_INPUT" | tr '[:upper:]' '[:lower:]')
case "$VALUE_LOWER" in
    true|1|yes|y|on|enable|enabled)
        ENABLED=true
        ;;
    false|0|no|n|off|disable|disabled)
        ENABLED=false
        ;;
    *)
        echo "Error: expected true/false (or on/off, yes/no, 1/0). Got: '$RAW_INPUT'" >&2
        exit 1
        ;;
esac

# Verify required variables are set
if [ -z "${SV2_URL:-}" ]; then
    echo "ERROR: SV2_URL not set. Make sure Docker containers are running." >&2
    exit 1
fi

echo "Supervisor v2 detected: $SV2_CONTAINER_NAME -> $SV2_URL"

PAYLOAD="{\"enabled\": ${ENABLED}}"

set -x
curl -sS -X POST -H 'Content-Type: application/json' \
  --data "$PAYLOAD" \
  "$SV2_URL/enable_supervisor_checkAccessList" | jq .
set +x

echo "Done."
