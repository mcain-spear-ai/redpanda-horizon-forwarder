#!/usr/bin/env bash
# Build the forwarder image with no cache, then verify horizon-data-core is 5.1.0.
set -e

IMAGE_NAME="${IMAGE_NAME:-redpanda-horizon-forwarder:5.1.0}"
REQUIRED_VERSION="5.1.0"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$REPO_ROOT"

echo "==> Building image (no cache): $IMAGE_NAME"
# docker compose build --no-cache forwarder

echo ""
echo "==> Verifying horizon-data-core version in image (no volume mount)..."
INSTALLED_VERSION=$(docker run --rm "$IMAGE_NAME" uv run python -c "
import importlib.metadata
v = importlib.metadata.version('horizon-data-core')
print(v)
")
echo "    horizon-data-core: $INSTALLED_VERSION"

if [ "$INSTALLED_VERSION" != "$REQUIRED_VERSION" ]; then
  echo ""
  echo "ERROR: Expected horizon-data-core $REQUIRED_VERSION, got $INSTALLED_VERSION"
  exit 1
fi

echo ""
echo "OK: Image $IMAGE_NAME has horizon-data-core $REQUIRED_VERSION"
