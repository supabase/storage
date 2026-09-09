#!/usr/bin/env bash
# Stress-test / EXPLAIN harness for storage.search() (migrations/tenant/
# 0063-fix-search-name-relative-to-prefix.sql). See README.md for the full picture;
# this header is just the option summary.
#
#   ./run.sh --rows 1000
#   ./run.sh --rows 1000000 --function /path/to/old-search.sql --label pre-fix
#   ./run.sh --phase explain --rows 1000000
#   ./run.sh --phase cleanup
#
# IMPORTANT: --function does a live `CREATE OR REPLACE FUNCTION storage.search` on
# whatever --db-url points at. Only point this at a local/dev database. To restore
# the real current version afterward:
#   psql "$DB_URL" -f ../../migrations/tenant/0063-fix-search-name-relative-to-prefix.sql

set -euo pipefail

ORIG_CWD="$(pwd)"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

ROWS=1000
COLLISION_RATE=0.05
BUCKET_ID=perf-stress-test
BATCH_SIZE=200000
BENCH_LIMIT=100
SORT_COLUMNS=""
FUNCTION_FILE=""
LABEL=""
PHASES="schema,seed,bench,export"
DB_URL="${PERF_DATABASE_URL:-postgresql://postgres:postgres@localhost:5432/postgres}"
RUN_ID="$(date +%Y%m%d-%H%M%S)"
DROP_PERF_SCHEMA=false

usage() {
  sed -n '2,15p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --rows) ROWS="$2"; shift 2 ;;
    --collision-rate) COLLISION_RATE="$2"; shift 2 ;;
    --bucket-id) BUCKET_ID="$2"; shift 2 ;;
    --batch-size) BATCH_SIZE="$2"; shift 2 ;;
    --bench-limit) BENCH_LIMIT="$2"; shift 2 ;;
    --sort-columns) SORT_COLUMNS="$2"; shift 2 ;;
    --function) FUNCTION_FILE="$2"; shift 2 ;;
    --label) LABEL="$2"; shift 2 ;;
    --phase) PHASES="$2"; shift 2 ;;
    --db-url) DB_URL="$2"; shift 2 ;;
    --run-id) RUN_ID="$2"; shift 2 ;;
    --drop-perf-schema) DROP_PERF_SCHEMA=true; shift ;;
    -h|--help) usage; exit 0 ;;
    *) echo "Unknown option: $1" >&2; usage; exit 1 ;;
  esac
done

if [[ -z "$LABEL" ]]; then
  if [[ -n "$FUNCTION_FILE" ]]; then
    LABEL="$(basename "$FUNCTION_FILE" .sql)"
  else
    LABEL="current"
  fi
fi

# Resolve a possibly-relative --function path against the directory run.sh was
# invoked from, before we cd into sql/ below.
if [[ -n "$FUNCTION_FILE" && "$FUNCTION_FILE" != /* ]]; then
  FUNCTION_FILE="$ORIG_CWD/$FUNCTION_FILE"
fi

has_phase() {
  [[ ",$PHASES," == *",$1,"* ]]
}

mkdir -p "$SCRIPT_DIR/results"

# psql's \i resolves relative to psql's current directory (not the invoking file's
# directory, at least as observed with the client in use), so every invocation below
# runs from sql/ and results/ are addressed as ../results/.
cd "$SCRIPT_DIR/sql"

PSQL=(psql "$DB_URL" -v ON_ERROR_STOP=1 -q)

echo "== storage.search() stress harness =="
echo "run_id=$RUN_ID label=$LABEL rows=$ROWS collision_rate=$COLLISION_RATE phases=$PHASES"
echo "db=$DB_URL"
echo

if has_phase schema; then
  echo "-- schema --"
  "${PSQL[@]}" -f 00_schema.sql
fi

if [[ -n "$FUNCTION_FILE" ]] && { has_phase bench || has_phase explain; }; then
  if [[ ! -f "$FUNCTION_FILE" ]]; then
    echo "Function file not found: $FUNCTION_FILE" >&2
    exit 1
  fi
  echo "-- installing storage.search from $FUNCTION_FILE (label: $LABEL) --"
  "${PSQL[@]}" -f "$FUNCTION_FILE"
fi

if has_phase seed; then
  echo "-- seed (rows=$ROWS, collision_rate=$COLLISION_RATE) --"
  "${PSQL[@]}" \
    -v total_rows="$ROWS" \
    -v collision_rate="$COLLISION_RATE" \
    -v bucket_id="$BUCKET_ID" \
    -v batch_size="$BATCH_SIZE" \
    -f 01_seed.sql
fi

if has_phase bench; then
  SORT_COLUMNS_ARGS=()
  if [[ -n "$SORT_COLUMNS" ]]; then
    SORT_COLUMNS_ARGS=(-v sort_columns="$SORT_COLUMNS")
  fi

  echo "-- bench: root listing --"
  "${PSQL[@]}" \
    -v run_id="$RUN_ID" \
    -v function_label="$LABEL" \
    -v bucket_id="$BUCKET_ID" \
    -v bench_limit="$BENCH_LIMIT" \
    "${SORT_COLUMNS_ARGS[@]+"${SORT_COLUMNS_ARGS[@]}"}" \
    -f 10_bench_root.sql

  echo "-- bench: within-folder listing --"
  "${PSQL[@]}" \
    -v run_id="$RUN_ID" \
    -v function_label="$LABEL" \
    -v bucket_id="$BUCKET_ID" \
    -v bench_limit="$BENCH_LIMIT" \
    "${SORT_COLUMNS_ARGS[@]+"${SORT_COLUMNS_ARGS[@]}"}" \
    -f 11_bench_within_folder.sql
fi

if has_phase explain; then
  EXPLAIN_OUT="../results/explain-${RUN_ID}-${LABEL}-rows${ROWS}.txt"
  echo "-- explain (writing to results/$(basename "$EXPLAIN_OUT")) --"
  "${PSQL[@]}" -v bucket_id="$BUCKET_ID" -f 20_explain.sql | tee "$EXPLAIN_OUT"
fi

if has_phase export; then
  CSV_OUT="../results/run-${RUN_ID}-${LABEL}-rows${ROWS}.csv"
  echo "-- export (writing to results/$(basename "$CSV_OUT")) --"
  "${PSQL[@]}" -v run_id="$RUN_ID" -f 90_export_results.sql > "$CSV_OUT"
fi

if has_phase cleanup; then
  echo "-- cleanup --"
  "${PSQL[@]}" -v bucket_id="$BUCKET_ID" -v drop_perf_schema="$DROP_PERF_SCHEMA" -f 99_cleanup.sql
fi

echo
echo "Done. run_id=$RUN_ID label=$LABEL"
