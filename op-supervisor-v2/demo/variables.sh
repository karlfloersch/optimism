#!/usr/bin/env bash

# Discover supervisor container name
SV2_CONTAINER_NAME=$(docker ps --format '{{.Names}}' | grep -E '^op-supervisor-v2-' | head -n1 || true)

# Discover Docker port mappings for chains and supervisor
if [ -n "$SV2_CONTAINER_NAME" ]; then
    # Get all container info at once
    CONTAINERS=$(docker ps --format "{{.Names}}\t{{.Ports}}" | grep -E "(op-el-|op-supervisor-v2-)")

    # Extract supervisor port (maps to 8545)
    SV2_HOST_PORT=$(echo "$CONTAINERS" | grep "op-supervisor-v2-" | head -1 | sed -n 's/.*0\.0\.0\.0:\([0-9]*\)->8545.*/\1/p')

    # Extract chain ports - look for the specific chain IDs
    CHAIN_2151908_PORT=$(echo "$CONTAINERS" | grep "op-el-2151908-" | head -1 | sed -n 's/.*0\.0\.0\.0:\([0-9]*\)->8545.*/\1/p')
    CHAIN_2151909_PORT=$(echo "$CONTAINERS" | grep "op-el-2151909-" | head -1 | sed -n 's/.*0\.0\.0\.0:\([0-9]*\)->8545.*/\1/p')

    # Build URLs
    if [ -n "$SV2_HOST_PORT" ]; then
        SV2_URL="http://127.0.0.1:${SV2_HOST_PORT}"
    fi
    if [ -n "$CHAIN_2151908_PORT" ]; then
        CHAIN_2151908_RPC="http://127.0.0.1:${CHAIN_2151908_PORT}"
    fi
    if [ -n "$CHAIN_2151909_PORT" ]; then
        CHAIN_2151909_RPC="http://127.0.0.1:${CHAIN_2151909_PORT}"
    fi
fi

# Default funded private key (standard Hardhat test account - funded in Docker setup)
DEFAULT_PRIV_KEY="0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80"

# Export all variables for use by other scripts
export SV2_CONTAINER_NAME
export SV2_HOST_PORT
export SV2_URL
export CHAIN_2151908_PORT
export CHAIN_2151908_RPC
export CHAIN_2151909_PORT
export CHAIN_2151909_RPC
export DEFAULT_PRIV_KEY
