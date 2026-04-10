#!/bin/bash
set -euo pipefail

# ----------------------------------------------------------------
# Combined op-supernode + op-reth entrypoint
#
# Generates a supervisord config from environment variables,
# then starts all three processes (2x op-reth + 1x op-supernode).
# ----------------------------------------------------------------

# Required
: "${L1_RPC:?Set L1_RPC to the L1 execution RPC URL}"
: "${L1_BEACON:?Set L1_BEACON to the L1 beacon API URL}"
: "${CHAIN_A_ID:?Set CHAIN_A_ID (e.g. 10 for OP Mainnet)}"
: "${CHAIN_B_ID:?Set CHAIN_B_ID (e.g. 130 for Unichain)}"

# Defaults
: "${JWT_SECRET_PATH:=/jwt.hex}"
: "${CHAIN_A_NETWORK:=}"
: "${CHAIN_B_NETWORK:=}"
: "${CHAIN_A_ROLLUP_CONFIG:=}"
: "${CHAIN_B_ROLLUP_CONFIG:=}"
: "${CHAIN_A_SEQUENCER_HTTP:=}"
: "${CHAIN_B_SEQUENCER_HTTP:=}"
: "${CHAIN_A_EXTRA_RETH_ARGS:=}"
: "${CHAIN_B_EXTRA_RETH_ARGS:=}"
: "${SUPERNODE_EXTRA_ARGS:=}"

# ---- Build op-reth commands ----
RETH_A_CMD="/usr/local/bin/op-reth node"
RETH_A_CMD+=" --datadir=/data/chain-a"
RETH_A_CMD+=" --authrpc.port=8551 --authrpc.addr=0.0.0.0"
RETH_A_CMD+=" --authrpc.jwtsecret=${JWT_SECRET_PATH}"
RETH_A_CMD+=" --http --http.port=8545 --http.addr=0.0.0.0"
RETH_A_CMD+=" --metrics=0.0.0.0:9001"
RETH_A_CMD+=" --port=30303"
RETH_A_CMD+=" --rollup.disable-tx-pool-gossip"
[ -n "$CHAIN_A_SEQUENCER_HTTP" ] && RETH_A_CMD+=" --rollup.sequencer-http=${CHAIN_A_SEQUENCER_HTTP}"
[ -n "$CHAIN_A_EXTRA_RETH_ARGS" ] && RETH_A_CMD+=" ${CHAIN_A_EXTRA_RETH_ARGS}"

RETH_B_CMD="/usr/local/bin/op-reth node"
RETH_B_CMD+=" --datadir=/data/chain-b"
RETH_B_CMD+=" --authrpc.port=8552 --authrpc.addr=0.0.0.0"
RETH_B_CMD+=" --authrpc.jwtsecret=${JWT_SECRET_PATH}"
RETH_B_CMD+=" --http --http.port=8546 --http.addr=0.0.0.0"
RETH_B_CMD+=" --metrics=0.0.0.0:9002"
RETH_B_CMD+=" --port=30304"
RETH_B_CMD+=" --rollup.disable-tx-pool-gossip"
[ -n "$CHAIN_B_SEQUENCER_HTTP" ] && RETH_B_CMD+=" --rollup.sequencer-http=${CHAIN_B_SEQUENCER_HTTP}"
[ -n "$CHAIN_B_EXTRA_RETH_ARGS" ] && RETH_B_CMD+=" ${CHAIN_B_EXTRA_RETH_ARGS}"

# ---- Build op-supernode command ----
SN_CMD="/usr/local/bin/op-supernode"
SN_CMD+=" --l1=${L1_RPC}"
SN_CMD+=" --l1.beacon=${L1_BEACON}"
SN_CMD+=" --chains=${CHAIN_A_ID},${CHAIN_B_ID}"
SN_CMD+=" --vn.${CHAIN_A_ID}.l2=http://127.0.0.1:8551"
SN_CMD+=" --vn.${CHAIN_A_ID}.l2.jwt-secret=${JWT_SECRET_PATH}"
SN_CMD+=" --vn.${CHAIN_B_ID}.l2=http://127.0.0.1:8552"
SN_CMD+=" --vn.${CHAIN_B_ID}.l2.jwt-secret=${JWT_SECRET_PATH}"

# Network or rollup-config for each chain
[ -n "$CHAIN_A_NETWORK" ]       && SN_CMD+=" --vn.${CHAIN_A_ID}.network=${CHAIN_A_NETWORK}"
[ -n "$CHAIN_A_ROLLUP_CONFIG" ] && SN_CMD+=" --vn.${CHAIN_A_ID}.rollup.config=${CHAIN_A_ROLLUP_CONFIG}"
[ -n "$CHAIN_B_NETWORK" ]       && SN_CMD+=" --vn.${CHAIN_B_ID}.network=${CHAIN_B_NETWORK}"
[ -n "$CHAIN_B_ROLLUP_CONFIG" ] && SN_CMD+=" --vn.${CHAIN_B_ID}.rollup.config=${CHAIN_B_ROLLUP_CONFIG}"

[ -n "$SUPERNODE_EXTRA_ARGS" ] && SN_CMD+=" ${SUPERNODE_EXTRA_ARGS}"

# ---- Generate supervisord config ----
cat > /etc/supervisor/conf.d/combined.conf <<EOF
[supervisord]
nodaemon=true
user=root
logfile=/var/log/supervisord.log
pidfile=/var/run/supervisord.pid
loglevel=info

[program:reth-chain-a]
command=${RETH_A_CMD}
stdout_logfile=/dev/fd/1
stdout_logfile_maxbytes=0
stderr_logfile=/dev/fd/2
stderr_logfile_maxbytes=0
autorestart=true
startretries=999999
priority=10

[program:reth-chain-b]
command=${RETH_B_CMD}
stdout_logfile=/dev/fd/1
stdout_logfile_maxbytes=0
stderr_logfile=/dev/fd/2
stderr_logfile_maxbytes=0
autorestart=true
startretries=999999
priority=10

[program:op-supernode]
command=${SN_CMD}
stdout_logfile=/dev/fd/1
stdout_logfile_maxbytes=0
stderr_logfile=/dev/fd/2
stderr_logfile_maxbytes=0
autorestart=true
startretries=999999
startsecs=5
priority=20
EOF

echo "============================================"
echo " Combined op-supernode + op-reth"
echo "============================================"
echo " Chain A (${CHAIN_A_ID}): engine=:8551  rpc=:8545  p2p=:30303  metrics=:9001"
echo " Chain B (${CHAIN_B_ID}): engine=:8552  rpc=:8546  p2p=:30304  metrics=:9002"
echo " Supernode:        rpc=:9545  metrics=:9003"
echo "============================================"

exec /usr/bin/supervisord -c /etc/supervisor/conf.d/combined.conf
