# Combined op-supernode + op-reth Docker Image
#
# Produces a single image containing:
#   - op-supernode (Go) — runs multiple OP Stack chains in one process
#   - op-reth (Rust) — execution client, run once per chain
#   - supervisord — process manager for all three services
#
# Build from monorepo root:
#   docker build -f ops/docker/combined/Dockerfile -t op-combined:latest .

# ============================================================
# Stage 1: Rust — cargo-chef base
# ============================================================
FROM lukemathwalker/cargo-chef:latest-rust-1.94 AS rust-chef
WORKDIR /app
RUN apt-get update && apt-get -y upgrade && apt-get install -y libclang-dev pkg-config
RUN cargo install cargo-auditable

# ---- planner: generate dependency recipe ----
FROM rust-chef AS rust-planner
COPY rust/ .
RUN cargo chef prepare --recipe-path recipe.json

# ---- builder: compile op-reth ----
FROM rust-chef AS rust-builder

ARG BUILD_PROFILE=maxperf
ENV BUILD_PROFILE=$BUILD_PROFILE

COPY --from=rust-planner /app/recipe.json recipe.json

# Cook dependencies (may partially fail on patched crates — that's fine,
# the real build below uses the workspace [patch.crates-io] sources)
RUN cargo chef cook --profile $BUILD_PROFILE --recipe-path recipe.json \
    --manifest-path /app/op-reth/bin/Cargo.toml || true

COPY rust/ .
RUN cargo auditable build --profile $BUILD_PROFILE --bin op-reth \
    --manifest-path /app/op-reth/bin/Cargo.toml

RUN cp /app/target/$BUILD_PROFILE/op-reth /app/op-reth-bin

# ============================================================
# Stage 2: Go — build op-supernode
# ============================================================
FROM golang:1.24.13-alpine3.22 AS go-builder

RUN apk add --no-cache make gcc musl-dev linux-headers git jq bash just

COPY go.mod go.sum /app/
WORKDIR /app
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    go mod download

COPY . /app

ARG GIT_COMMIT
ARG GIT_DATE

RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    cd op-supernode && \
    GITCOMMIT=$GIT_COMMIT GITDATE=$GIT_DATE VERSION=v0.0.0 just op-supernode

# ============================================================
# Stage 3: Runtime — Ubuntu with supervisord
# ============================================================
FROM ubuntu:24.04

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        supervisor \
        ca-certificates \
        libssl-dev \
        tini && \
    rm -rf /var/lib/apt/lists/*

# Binaries
COPY --from=rust-builder /app/op-reth-bin /usr/local/bin/op-reth
COPY --from=go-builder /app/op-supernode/bin/op-supernode /usr/local/bin/op-supernode
RUN chmod +x /usr/local/bin/op-reth /usr/local/bin/op-supernode

# Entrypoint (generates supervisord config from env vars)
COPY ops/docker/combined/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

# Data volumes — one per chain
VOLUME ["/data/chain-a", "/data/chain-b"]

# Port layout:
#   chain-a EL:  8551 (engine), 8545 (http rpc), 30303 (p2p), 9001 (metrics)
#   chain-b EL:  8552 (engine), 8546 (http rpc), 30304 (p2p), 9002 (metrics)
#   supernode:   9545 (http rpc), 9003 (metrics)
EXPOSE 8545 8546 8551 8552 9545 30303 30304 9001 9002 9003

ENTRYPOINT ["/usr/bin/tini", "--", "/entrypoint.sh"]
