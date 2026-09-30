#!/usr/bin/env bash
# ============================================================================
# test_idempotency.sh — proves that repeated executions do not duplicate or
# change analytical records (Question 8, Task 3).
#
#   1. fingerprint every gold table
#   2. run the whole NiFi pipeline again (extract + gold build)
#   3. fingerprint again and compare -> must be identical
#   4. force a FULL REFRESH gold rebuild (every fact deleted and re-inserted)
#      through SQL and compare again -> must still be identical
#
# Requires NiFi running with the flow built (python nifi/build_flow.py).
# ============================================================================
set -euo pipefail
export PGHOST="${PGHOST:-localhost}" PGPORT="${PGPORT:-5432}" PGUSER="${PGUSER:-postgres}"
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PSQL=(psql -w -X -q -v ON_ERROR_STOP=1 -d abc_hub_analytics)
fingerprint() { "${PSQL[@]}" -At -F ' | ' -f "$ROOT/sql/03_tests/02_fact_checksums.sql"; }

echo "==> 1. fingerprint before"
before="$(fingerprint)"; echo "$before"

echo "==> 2. re-running the full NiFi pipeline (nothing changed at the source)"
python "$ROOT/nifi/run_pipeline.py" >/dev/null
"${PSQL[@]}" -c "SELECT run_id, status, step_name, rows_inserted, rows_updated, rows_deleted
                   FROM etl.run_step_log JOIN etl.pipeline_run USING (run_id)
                  WHERE run_id = (SELECT max(run_id) FROM etl.pipeline_run) ORDER BY step_log_id"
after_rerun="$(fingerprint)"

echo "==> 3. forced full-refresh rebuild of gold"
"${PSQL[@]}" >/dev/null <<'SQL'
SELECT run_id AS rid FROM etl.start_run('gold_build', true, 'idempotency-test') \gset
SELECT * FROM etl.gold_prepare(:rid, true);
SELECT * FROM etl.load_dim_subscription_plan(:rid);
SELECT * FROM etl.load_dim_content(:rid);
SELECT * FROM etl.load_dim_warehouse(:rid);
SELECT * FROM etl.load_dim_customer(:rid);
SELECT * FROM etl.load_dim_inventory_item(:rid);
SELECT * FROM etl.load_fact_customer_daily_activity(:rid);
SELECT * FROM etl.load_fact_content_monthly_performance(:rid);
SELECT * FROM etl.load_fact_inventory_daily_snapshot(:rid);
SELECT * FROM etl.gold_finalize(:rid);
SELECT * FROM etl.finish_run(:rid);
SQL
after_full="$(fingerprint)"

status=0
if [[ "$before" == "$after_rerun" ]]; then echo "PASS  re-run left every gold table identical"
else echo "FAIL  re-run changed gold:"; diff <(echo "$before") <(echo "$after_rerun") || true; status=1; fi
if [[ "$before" == "$after_full" ]]; then echo "PASS  full-refresh rebuild reproduced every gold table exactly"
else echo "FAIL  full refresh differs:"; diff <(echo "$before") <(echo "$after_full") || true; status=1; fi
exit $status
