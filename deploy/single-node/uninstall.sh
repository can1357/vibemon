#!/usr/bin/env bash
# Idempotent Uninstaller for Vibemon (vmon)
set -euo pipefail

BIN_DEST="/usr/local/bin/vmon"
AGENT_DEST="/usr/local/libexec/vmon-agent"
CONFIG_DIR="/etc/vmon"
DATA_DIR="/var/lib/vmon"
SYSTEMD_DIR="/etc/systemd/system"
PURGE=false

# Check for --purge argument
for arg in "$@"; do
    if [[ "$arg" == "--purge" ]]; then
        PURGE=true
    fi
done

echo "=== Uninstalling Vibemon (vmon) ==="

# Check root privilege
if [[ "$EUID" -ne 0 ]]; then
    echo "Error: This script must be run as root (sudo)." >&2
    exit 1
fi

# 1. Stop and disable systemd service
if systemctl is-active --quiet vmon.service 2>/dev/null; then
    echo "Stopping vmon service..."
    systemctl stop vmon.service || true
fi

if systemctl is-enabled --quiet vmon.service 2>/dev/null; then
    echo "Disabling vmon service..."
    systemctl disable vmon.service || true
fi

# 2. Remove systemd service file
if [[ -f "${SYSTEMD_DIR}/vmon.service" ]]; then
    echo "Removing systemd service file..."
    rm -f "${SYSTEMD_DIR}/vmon.service"
    echo "Reloading systemd daemon..."
    systemctl daemon-reload
    systemctl reset-failed || true
fi

# 3. Remove binary
if [[ -f "${BIN_DEST}" ]]; then
    echo "Removing binary at ${BIN_DEST}..."
    rm -f "${BIN_DEST}"
fi
if [[ -f "${AGENT_DEST}" ]]; then
    echo "Removing guest agent at ${AGENT_DEST}..."
    rm -f "${AGENT_DEST}"
fi

# 4. Remove system user and group
if getent passwd vmon >/dev/null; then
    echo "Removing vmon system user..."
    userdel vmon || true
fi

if getent group vmon >/dev/null; then
    echo "Removing vmon system group..."
    groupdel vmon || true
fi

# 5. Purge files if requested
if [[ "$PURGE" == "true" ]]; then
    echo "Purging configuration and data directories..."
    if [[ -d "${CONFIG_DIR}" ]]; then
        rm -rf "${CONFIG_DIR}"
        echo "Removed configuration directory ${CONFIG_DIR}"
    fi
    if [[ -d "${DATA_DIR}" ]]; then
        rm -rf "${DATA_DIR}"
        echo "Removed data directory ${DATA_DIR}"
    fi
else
    echo "Retaining configuration (${CONFIG_DIR}) and data (${DATA_DIR}) directories."
    echo "Run with '--purge' if you wish to remove them: $0 --purge"
fi

echo "=== Uninstallation complete successfully ==="
