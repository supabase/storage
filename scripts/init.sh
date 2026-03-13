#!/usr/bin/env bash

# Help initialise self hosting to change environment variables
#
# Portions of this code are derived from Inder Singh's setup.sh shell script.
# Copyright 2025 Inder Singh. Licensed under Apache License 2.0.
# Original source: https://github.com/singh-inder/supabase-automated-self-host/blob/main/setup.sh
#

set -e

gen_hex() {
    openssl rand -hex "$1"
}

gen_base64() {
    openssl rand -base64 "$1"
}

base64_url_encode() {
    openssl enc -base64 -A | tr '+/' '-_' | tr -d '='
}

prompt() {
    local var=$1 msg=$2 default=$3
    read -p "$msg [$default]: " value
    printf -v "$var" '%s' "${value:-$default}"
}

promptYN() {
    read -p "$1 y/N: " resp
    resp=$(echo "$resp" | tr '[:upper:]' '[:lower:]')
    [[ "$resp" == "y" ]] && echo true || echo false
}

if ! command -v openssl >/dev/null 2>&1; then
    echo "Error: openssl is required but not found."
    exit 1
fi

multitenant=$(promptYN "Use multitenant?")
if $multitenant; then
    prompt SERVER_ADMIN_API_KEYS "Enter SERVER_ADMIN_API_KEYS" $(gen_hex 16)
    prompt DATABASE_MULTITENANT_URL "Enter DATABASE_MULTITENANT_URL" ""
    prompt DATABASE_MULTITENANT_POOL_URL "Enter DATABASE_MULTITENANT_POOL_URL" ""
fi

prompt AUTH_ENCRYPTION_KEY "Enter AUTH_ENCRYPTION_KEY" $(gen_hex 16)
prompt DATABASE_URL "Enter DATABASE_URL" ""
prompt DATABASE_POOL_URL "Enter DATABASE_URL" ""

updateEnv=$(promptYN "Create .env and update?")
if $updateEnv; then
    echo "Updating .env..."
    cp .env.sample .env
    if $multitenant; then
        sed -i.old \
        -e "s|^# MULTI_TENANT=true$|MULTI_TENANT=true|" \
        -e "s|^SERVER_ADMIN_API_KEYS=.*$|SERVER_ADMIN_API_KEYS=${SERVER_ADMIN_API_KEYS}|" \
        .env

        if [[ -n "$DATABASE_MULTITENANT_URL" ]]; then
            sed -i.old \
                -e "s|^DATABASE_MULTITENANT_URL=.*$|DATABASE_MULTITENANT_URL=${DATABASE_MULTITENANT_URL}|" \
            .env
        fi
        if [[ -n "$DATABASE_MULTITENANT_POOL_URL" ]]; then
            sed -i.old \
                -e "s|^DATABASE_MULTITENANT_POOL_URL=.*$|DATABASE_MULTITENANT_POOL_URL=${DATABASE_MULTITENANT_POOL_URL}|" \
            .env
        fi
    fi

    sed -i.old \
        -e "s|^AUTH_ENCRYPTION_KEY=.*$|AUTH_ENCRYPTION_KEY=${AUTH_ENCRYPTION_KEY}|" \
    .env

    if [[ -n "$DATABASE_URL" ]]; then
        sed -i.old \
            -e "s|^DATABASE_URL=.*$|DATABASE_URL=${DATABASE_URL}|" \
        .env
    fi
    if [[ -n "$DATABASE_POOL_URL" ]]; then
        sed -i.old \
            -e "s|^DATABASE_POOL_URL=.*$|DATABASE_POOL_URL=${DATABASE_POOL_URL}|" \
        .env
    fi
fi

echo -e "\n\n#### Initialised values:\n"

if $multitenant; then
    echo "SERVER_ADMIN_API_KEYS: ${SERVER_ADMIN_API_KEYS}"
    if [[ -n "$DATABASE_MULTITENANT_URL" ]]; then
        echo "DATABASE_MULTITENANT_URL: ${DATABASE_MULTITENANT_URL}"
    fi
    if [[ -n "$DATABASE_MULTITENANT_POOL_URL" ]]; then
        echo "DATABASE_MULTITENANT_POOL_URL: ${DATABASE_MULTITENANT_POOL_URL}"
    fi
fi

echo "AUTH_ENCRYPTION_KEY: ${AUTH_ENCRYPTION_KEY}"
if [[ -n "$DATABASE_URL" ]]; then
    echo "DATABASE_URL: ${DATABASE_URL}"
fi
if [[ -n "$DATABASE_POOL_URL" ]]; then
    echo "DATABASE_POOL_URL: ${DATABASE_POOL_URL}"
fi
