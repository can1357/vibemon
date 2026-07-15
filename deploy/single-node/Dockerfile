FROM moby/buildkit:v0.20.2 AS buildkit

# Multi-stage Dockerfile for Vibemon Daemon (vmon)
FROM rust:1.88-slim-bookworm AS builder

# Install build dependencies
RUN apt-get update && apt-get install -y \
    pkg-config \
    clang \
    cmake \
    curl \
    libssl-dev \
    musl-tools \
    libslirp-dev \
    git \
    protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /usr/src/vibemon
COPY . .

# Build the monitor and a static guest agent for the current image architecture.
RUN cargo build --release --bin vmon \
    && case "$(dpkg --print-architecture)" in \
        amd64) agent_target=x86_64-unknown-linux-musl ;; \
        arm64) agent_target=aarch64-unknown-linux-musl ;; \
        *) echo "unsupported container architecture" >&2; exit 1 ;; \
    esac \
    && rustup target add "${agent_target}" \
    && cargo build --release -p vmon-agent --target "${agent_target}" \
    && cp "target/${agent_target}/release/vmon-agent" target/release/vmon-agent-static

FROM debian:bookworm-slim

# Install runtime dependencies for VMM, networking, and image pulling
RUN apt-get update && apt-get install -y \
    iptables \
    iproute2 \
    dnsmasq \
    e2fsprogs \
    skopeo \
    umoci \
    qemu-utils \
    libslirp0 \
    ca-certificates \
    curl \
    jq \
    && rm -rf /var/lib/apt/lists/*

# Copy the vmon binary from builder
COPY --from=builder /usr/src/vibemon/target/release/vmon /usr/local/bin/vmon
COPY --from=buildkit /usr/bin/buildctl /usr/local/bin/buildctl
COPY --from=builder /usr/src/vibemon/target/release/vmon-agent-static /usr/local/libexec/vmon-agent

# Configure default directory layout
RUN groupadd --gid 10000 vmon \
    && useradd --uid 10000 --gid vmon --no-create-home --shell /usr/sbin/nologin vmon
RUN mkdir -p /var/lib/vmon /etc/vmon

# Set environment defaults
ENV VMON_HOME=/var/lib/vmon
ENV VMON_SANDBOX_UID=10000
ENV VMON_SANDBOX_GID=10000
ENV VMON_AGENT=/usr/local/libexec/vmon-agent

VOLUME ["/var/lib/vmon", "/etc/vmon"]

# Expose gRPC port (default 8000)
EXPOSE 8000

# Run a self-contained single-node server; Compose supplies the token and builder endpoint.
ENTRYPOINT ["/usr/local/bin/vmon", "serve", "--host", "0.0.0.0", "--home", "/var/lib/vmon"]
