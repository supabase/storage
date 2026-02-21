#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
COMPOSE_FILE="$PROJECT_DIR/docker-compose-bench.yml"
K6_SCRIPT="${1:-/benchmarks/storage-bench.js}"

echo "Starting infrastructure and monitoring..."
docker compose -f "$COMPOSE_FILE" up -d \
  tenant_db pg_bouncer minio minio_setup imgproxy \
  otel jaeger grafana prometheus

echo "Waiting for infrastructure to be ready..."
sleep 15

echo "Building and starting both storage variants..."
docker compose -f "$COMPOSE_FILE" up -d --build storage-standard storage-caged

# Wait for both services to be ready
echo "Waiting for storage-standard (port 5000)..."
for i in $(seq 1 30); do
  if curl -sf http://localhost:5000/status > /dev/null 2>&1; then
    echo "storage-standard is ready."
    break
  fi
  if [ "$i" -eq 30 ]; then
    echo "ERROR: storage-standard did not become ready in time."
    exit 1
  fi
  sleep 2
done

echo "Waiting for storage-caged (port 5002)..."
for i in $(seq 1 30); do
  if curl -sf http://localhost:5002/status > /dev/null 2>&1; then
    echo "storage-caged is ready."
    break
  fi
  if [ "$i" -eq 30 ]; then
    echo "ERROR: storage-caged did not become ready in time."
    exit 1
  fi
  sleep 2
done

echo "============================================"
echo "Running k6 benchmarks simultaneously..."
echo "============================================"

# Run both k6 instances in parallel
docker compose -f "$COMPOSE_FILE" --profile test run -d --rm k6-standard run "$K6_SCRIPT"
docker compose -f "$COMPOSE_FILE" --profile test run -d --rm k6-caged run "$K6_SCRIPT"

# Wait for both k6 containers to finish
echo "Waiting for k6 runs to complete..."
docker compose -f "$COMPOSE_FILE" --profile test wait k6-standard k6-caged 2>/dev/null || true

# Show logs from both runs
echo ""
echo "=== k6-standard results ==="
docker compose -f "$COMPOSE_FILE" --profile test logs k6-standard 2>/dev/null | tail -40
echo ""
echo "=== k6-caged results ==="
docker compose -f "$COMPOSE_FILE" --profile test logs k6-caged 2>/dev/null | tail -40

echo ""
echo "============================================"
echo "Benchmarks complete!"
echo "View results at http://localhost:3000"
echo "  Login: admin / grafana"
echo "  Dashboard: k6 Benchmark: node-caged vs standard"
echo ""
echo "Services still running - compare in Grafana then stop with:"
echo "  docker compose -f docker-compose-bench.yml down"
echo "============================================"
