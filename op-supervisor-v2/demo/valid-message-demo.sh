#!/usr/bin/env bash

set -euo pipefail

# Load environment variables
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1090
source "$SCRIPT_DIR/variables.sh"

# Verify required variables are set
if [ -z "${CHAIN_2151908_RPC:-}" ] || [ -z "${CHAIN_2151909_RPC:-}" ] || [ -z "${DEFAULT_PRIV_KEY:-}" ]; then
    echo "ERROR: Required environment variables not set. Make sure Docker containers are running." >&2
    exit 1
fi

# Change to op-up directory to run msgcheck
cd "$SCRIPT_DIR/../../op-up" || {
    echo "ERROR: Could not find op-up directory at $SCRIPT_DIR/../../op-up" >&2
    exit 1
}

# Run msgcheck with discovered parameters, only show the final result line
go run ./cmd/msgcheck \
    -mode valid-msg \
    -from 901 \
    -priv-key "$DEFAULT_PRIV_KEY" \
    -rpc-901 "$CHAIN_2151908_RPC" \
    -rpc-902 "$CHAIN_2151909_RPC" 2>/dev/null | grep "Valid Message:"
