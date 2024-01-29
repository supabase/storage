#!/usr/bin/env sh
set -Eeuo pipefail


# Check if the DB_MIGRATION_HASH_FILE exists and is not empty
if [ -s DB_MIGRATION_HASH_FILE ]; then
    export DB_MIGRATION_HASH=$(cat DB_MIGRATION_HASH_FILE)
fi

exec "${@}"

