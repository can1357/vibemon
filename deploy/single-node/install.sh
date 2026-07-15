#!/usr/bin/env bash
# Deterministic and Idempotent Installer for Vibemon (vmon) on a single node
set -euo pipefail

# Configuration defaults
BIN_DEST="/usr/local/bin/vmon"
AGENT_DEST="/usr/local/libexec/vmon-agent"
CONFIG_DIR="/etc/vmon"
CONFIG_FILE="${CONFIG_DIR}/serve.toml"
DATA_DIR="/var/lib/vmon"
IDENTITY_FILE="${CONFIG_DIR}/sandbox.env"
SYSTEMD_DIR="/etc/systemd/system"
DEFAULT_PORT=8000
DEFAULT_HOST="127.0.0.1"

echo "=== Installing Vibemon (vmon) ==="

# 1. Check root privilege
if [[ "$EUID" -ne 0 ]]; then
    echo "Error: This script must be run as root (sudo)." >&2
    exit 1
fi

# 2. Check for hypervisor support
echo "Checking virtualization prerequisites..."
if [[ ! -c /dev/kvm ]]; then
    echo "Warning: /dev/kvm not found or is not a character device." >&2
    echo "Ensure KVM is enabled in BIOS/kernel. Vibemon can boot in local mode but microVMs require KVM." >&2
else
    echo "KVM device found at /dev/kvm."
fi

if [[ ! -c /dev/net/tun ]]; then
    echo "Warning: /dev/net/tun not found. Network TAP features may fail." >&2
fi

# 3. Locate the monitor and matching static guest agent.
VMON_SOURCE=""
if [[ $# -gt 0 && -f "$1" ]]; then
    VMON_SOURCE="$1"
elif [[ -f "./target/release/vmon" ]]; then
    VMON_SOURCE="./target/release/vmon"
elif [[ -f "./target/debug/vmon" ]]; then
    VMON_SOURCE="./target/debug/vmon"
elif command -v vmon &>/dev/null; then
    VMON_SOURCE=$(command -v vmon)
fi

if [[ -z "$VMON_SOURCE" ]]; then
    echo "Error: Could not locate vmon binary." >&2
    echo "Build with 'just release' or pass the release binary as the first argument." >&2
    exit 1
fi

case "$(uname -m)" in
    x86_64) AGENT_TARGET="x86_64-unknown-linux-musl"; AGENT_ARCH="x86_64" ;;
    aarch64|arm64) AGENT_TARGET="aarch64-unknown-linux-musl"; AGENT_ARCH="aarch64" ;;
    *) echo "Error: unsupported host architecture $(uname -m)." >&2; exit 1 ;;
esac
AGENT_SOURCE="${2:-}"
SOURCE_DIR="$(cd "$(dirname "$VMON_SOURCE")" && pwd)"
if [[ -z "$AGENT_SOURCE" && -f "${SOURCE_DIR}/vmon-agent-${AGENT_TARGET}" ]]; then
    AGENT_SOURCE="${SOURCE_DIR}/vmon-agent-${AGENT_TARGET}"
elif [[ -z "$AGENT_SOURCE" && -f "${SOURCE_DIR}/vmon-agent" ]]; then
    AGENT_SOURCE="${SOURCE_DIR}/vmon-agent"
elif [[ -z "$AGENT_SOURCE" && -f "./target/test-assets/vmon-agent-${AGENT_ARCH}" ]]; then
    AGENT_SOURCE="./target/test-assets/vmon-agent-${AGENT_ARCH}"
fi
if [[ -z "$AGENT_SOURCE" || ! -f "$AGENT_SOURCE" ]]; then
    echo "Error: matching static vmon-agent not found." >&2
    echo "Pass vmon-agent-${AGENT_TARGET} as the second argument or run 'just agent-musl'." >&2
    exit 1
fi

echo "Using vmon binary from: ${VMON_SOURCE}"

# 4. Create directories
echo "Creating directories..."
mkdir -p "${CONFIG_DIR}"
mkdir -p "${DATA_DIR}"

# 5. Create vmon system user/group if they don't exist
if ! getent group vmon >/dev/null; then
    echo "Creating vmon system group..."
    groupadd -r vmon
fi

if ! getent passwd vmon >/dev/null; then
    echo "Creating vmon system user..."
    useradd -r -g vmon -d "${DATA_DIR}" -s /sbin/nologin -c "Vibemon Daemon User" vmon
fi

# Ensure user is in kvm group if it exists
if getent group kvm >/dev/null; then
    echo "Adding vmon user to kvm group..."
    usermod -aG kvm vmon || true
fi
printf 'VMON_SANDBOX_UID=%s\nVMON_SANDBOX_GID=%s\nVMON_AGENT=%s\n' \
    "$(id -u vmon)" "$(id -g vmon)" "${AGENT_DEST}" > "${IDENTITY_FILE}"
chmod 600 "${IDENTITY_FILE}"

# 6. Generate configuration if missing
if [[ ! -f "${CONFIG_FILE}" ]]; then
    echo "Generating default configuration at ${CONFIG_FILE}..."
    # Generate random API token
    API_TOKEN=$(head -c 32 /dev/urandom | od -An -vtx1 | tr -d ' \n')
    
    cat <<EOF > "${CONFIG_FILE}"
[serve]
host = "${DEFAULT_HOST}"
port = ${DEFAULT_PORT}
home = "${DATA_DIR}"
token = "${API_TOKEN}"
idle_timeout = 300
warm_pool_size = 1
EOF
    chmod 600 "${CONFIG_FILE}"
    chown -R vmon:vmon "${CONFIG_DIR}"
else
    echo "Configuration already exists at ${CONFIG_FILE}. Skipping generation."
fi

# Set directory ownership
chown -R vmon:vmon "${DATA_DIR}"

# 7. Install the monitor and guest agent without failing on an idempotent rerun.
echo "Installing binaries..."
mkdir -p "$(dirname "${AGENT_DEST}")"
if [[ "$(readlink -f "${VMON_SOURCE}")" != "$(readlink -f "${BIN_DEST}")" ]]; then
    install -m 755 "${VMON_SOURCE}" "${BIN_DEST}"
fi
if [[ "$(readlink -f "${AGENT_SOURCE}")" != "$(readlink -f "${AGENT_DEST}")" ]]; then
    install -m 755 "${AGENT_SOURCE}" "${AGENT_DEST}"
fi

# 8. Set up systemd service
SCRIPT_DIR="$(dirname "${BASH_SOURCE[0]}")"
SERVICE_SOURCE="${SCRIPT_DIR}/vmon.service"

if [[ -f "${SERVICE_SOURCE}" ]]; then
    echo "Installing systemd service unit..."
    cp "${SERVICE_SOURCE}" "${SYSTEMD_DIR}/vmon.service"
    chmod 644 "${SYSTEMD_DIR}/vmon.service"
    
    echo "Reloading systemd daemon..."
    systemctl daemon-reload
    
    echo "Enabling vmon service..."
    systemctl enable vmon.service
    
    echo "Starting vmon service..."
    systemctl restart vmon.service
    if ! systemctl is-active --quiet vmon.service; then
        systemctl status --no-pager vmon.service >&2 || true
        echo "Error: vmon service did not become active." >&2
        exit 1
    fi
else
    echo "Error: systemd service source file not found at ${SERVICE_SOURCE}" >&2
    exit 1
fi

echo "=== Installation complete successfully ==="
echo "You can check service status with: systemctl status vmon"
echo "You can check configuration at: ${CONFIG_FILE}"
