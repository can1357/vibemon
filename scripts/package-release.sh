#!/usr/bin/env bash
# Packaging and release script for Vibemon (vmon) deployment assets
set -euo pipefail

DIST_DIR="dist"
TAR_FILE="${DIST_DIR}/vmon-deploy-assets.tar.gz"

echo "=== Packaging Vibemon Deployment Assets ==="

# 1. Run validation first
if command -v just &>/dev/null; then
    echo "Running validation checks..."
    just validate-deploy
else
    echo "Warning: 'just' command not found, skipping validation."
fi

# 2. Create distribution directory
mkdir -p "${DIST_DIR}"

# 3. Create tarball of deploy/ directory
echo "Creating release archive: ${TAR_FILE}"
tar -czf "${TAR_FILE}" deploy/

# 4. Generate checksums
echo "Generating SHA256 checksums..."
(
    cd "${DIST_DIR}"
    archive=$(basename "${TAR_FILE}")
    if command -v sha256sum >/dev/null 2>&1; then
        sha256sum "${archive}" > "SHA256SUMS-deploy.txt"
    else
        shasum -a 256 "${archive}" > "SHA256SUMS-deploy.txt"
    fi
)

echo "=== Packaging completed successfully ==="
echo "Artifacts generated:"
echo "  - ${TAR_FILE}"
echo "  - ${DIST_DIR}/SHA256SUMS-deploy.txt"
