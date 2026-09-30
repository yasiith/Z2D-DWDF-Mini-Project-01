#!/usr/bin/env bash
# ============================================================================
# run_full_demo.sh — the whole project from scratch, in one command:
#
#   1. rebuild both databases (source reloaded from the CSVs, warehouse empty)
#   2. rebuild the NiFi flow (fresh extract watermarks) and start it
#   3. initial load through NiFi (extract -> bronze/silver -> gold)
#   4. 44 validation checks
#   5. idempotency test (repeat run + full refresh must change nothing)
#   6. screenshots (NiFi still shows the initial load in its 5-minute stats)
#
# Requires: PostgreSQL + pgpass, NiFi running at http://127.0.0.1:8080,
#           Python 3, Node 22+, Chrome/Edge.
# ============================================================================
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

echo "######## 1. databases"
./scripts/setup_databases.sh >/dev/null
./scripts/setup_databases.sh --reset-analytics | tail -2
rm -rf "${NIFI_HOME:-/d/DATA ENGINEERING/tools/nifi-2.12.0}/logs/abc_hub_etl" 2>/dev/null || true

echo "######## 2. NiFi flow"
python nifi/build_flow.py

echo "######## 3. initial load"
time python nifi/run_pipeline.py

echo "######## 4. validation"
./scripts/run_tests.sh | tail -1

echo "######## 5. idempotency"
./scripts/test_idempotency.sh | tail -2

echo "######## 6. screenshots"
./scripts/capture_evidence.sh | grep -c saved
echo "done"
