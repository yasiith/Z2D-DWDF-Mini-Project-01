#!/usr/bin/env bash
# ============================================================================
# run_tests.sh — runs the data validation suite against abc_hub_analytics.
# Exit code 0 = every test passed, 1 = at least one FAIL.
# ============================================================================
set -euo pipefail
export PGHOST="${PGHOST:-localhost}" PGPORT="${PGPORT:-5432}" PGUSER="${PGUSER:-postgres}"
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

out="$(psql -w -X -v ON_ERROR_STOP=1 -d abc_hub_analytics -f "$ROOT/sql/03_tests/01_data_validation_tests.sql")"
echo "$out"
fails=$(grep -c "| FAIL" <<< "$out" || true)
passes=$(grep -c "| PASS" <<< "$out" || true)
echo
echo "Result: $passes passed, $fails failed"
[[ "$fails" -eq 0 ]]
