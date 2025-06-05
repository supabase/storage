#!/bin/bash

##############################################################################
#
# Replace Existing Container
#
# This script builds a new docker image from the current code,
# and replaces a running supabase storage container with it
# it can be used to test changes in the context of other services (e.g. supabase cli)
#
##############################################################################

set -euo pipefail

if [ "$#" -ne 1 ]; then
  echo "Usage: $0 <replace_container_name>"
  exit 1
fi

REPLACE_CONTAINER_NAME="$1"
NEW_IMAGE_TAG="storage-api:debug-replace-existing"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TARGET_DIR="$SCRIPT_DIR/../../.docker/.debug"
DOCKER_COMPOSE_PATH="$TARGET_DIR/$REPLACE_CONTAINER_NAME.yml"

# check if specified container exists / is running
if ! docker ps --format '{{.Names}}' | grep -q "^${REPLACE_CONTAINER_NAME}$"; then
  echo "Error: Docker container '$REPLACE_CONTAINER_NAME' is not running."
  exit 1
fi

# ensure target docker-compose directory exists
mkdir -p "$TARGET_DIR"

# build storage api
echo " "
echo "[1/5] Building \"$NEW_IMAGE_TAG\" image from current storage code..."
echo " "
docker build -t "$NEW_IMAGE_TAG" .

# create docker compose file based on the running image
echo " "
echo "[2/5] Generating docker-compose file from existing container..."
echo " "
docker pull ghcr.io/red5d/docker-autocompose:latest
docker run --rm -v /var/run/docker.sock:/var/run/docker.sock ghcr.io/red5d/docker-autocompose "$REPLACE_CONTAINER_NAME" > "$DOCKER_COMPOSE_PATH"

# update docker-compose to use newly built image
OLD_IMAGE=$(grep 'image:' "$DOCKER_COMPOSE_PATH" | awk '{print $2}' | head -n 1)
echo " "
echo "[3/5] Replacing image $OLD_IMAGE with \"$NEW_IMAGE_TAG\"..."
echo " "
sed -i.bak "s|image: $OLD_IMAGE|image: $NEW_IMAGE_TAG|" "$DOCKER_COMPOSE_PATH"

# stop and remove storage container
echo " "
echo "[4/5] Stopping existing \"$REPLACE_CONTAINER_NAME\" container..."
echo " "
docker rm -f "$REPLACE_CONTAINER_NAME"

# start new container to replace existing
echo "  "
echo "[5/5] Starting new container..."
echo "  "
docker-compose -f "$DOCKER_COMPOSE_PATH" up -d

echo " "
echo "✅ New \"$REPLACE_CONTAINER_NAME\" container is running"
echo " "
echo "You can update the container by running this script again"
echo " "
echo "To watch logs run: docker logs -f $REPLACE_CONTAINER_NAME"
echo " "
