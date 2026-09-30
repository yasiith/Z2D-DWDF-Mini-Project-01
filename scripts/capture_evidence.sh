#!/usr/bin/env bash
# ============================================================================
# capture_evidence.sh — produces the screenshots required by the brief:
#   1. runs the NiFi pipeline once (so the canvas shows fresh In/Out stats)
#   2. renders PostgreSQL evidence (real query output, psql -H) to HTML
#   3. captures NiFi canvas + evidence pages as PNGs (headless Chrome)
# Output: docs/screenshots/*.png
# ============================================================================
set -euo pipefail
export PGHOST="${PGHOST:-localhost}" PGPORT="${PGPORT:-5432}" PGUSER="${PGUSER:-postgres}"
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
HTML="$ROOT/docs/screenshots/html"
mkdir -p "$HTML"

echo "==> running the pipeline once"
python "$ROOT/nifi/run_pipeline.py" | tail -3

page() {  # page <file> <title> <sql file or inline sql>
  local file="$1" title="$2" sql="$3"
  {
    cat <<HTML
<!doctype html><html><head><meta charset="utf-8"><title>$title</title><style>
body{font-family:Segoe UI,Arial,sans-serif;margin:24px;color:#1c2330;background:#fff}
h1{font-size:20px;border-bottom:3px solid #b3261e;padding-bottom:6px}
.meta{color:#5b6573;font-size:13px;margin-bottom:10px}
table{border-collapse:collapse;margin:6px 0 18px;font-size:12.5px}
th,td{border:1px solid #cfd6df;padding:3px 8px;text-align:left}
th{background:#eef2f6} caption{font-weight:600;text-align:left;padding:6px 0;font-size:14px}
p{font-size:12px;color:#5b6573;margin:0 0 14px}
</style></head><body><h1>$title</h1>
<div class="meta">psql -d abc_hub_analytics &middot; captured $(date '+%Y-%m-%d %H:%M:%S')</div>
HTML
    # stdin (-f -) so \pset meta-commands and SQL can be mixed
    printf '%s\n' "$sql" | psql -w -X -q -H -d abc_hub_analytics -v ON_ERROR_STOP=1 -f - \
      | sed -e 's/border="1"//'
    echo "</body></html>"
  } > "$HTML/$file"
}

echo "==> rendering PostgreSQL evidence"
page 09_db_tables_populated.html "Analytical database populated - rows per layer and table" "
\\pset title 'Row counts per table (bronze = raw history, silver = cleansed, gold = star schema)'
SELECT schemaname AS layer, relname AS table_name, n_live_tup AS row_count
FROM pg_stat_user_tables WHERE schemaname IN ('bronze','silver','gold','etl')
ORDER BY CASE schemaname WHEN 'bronze' THEN 1 WHEN 'silver' THEN 2 WHEN 'gold' THEN 3 ELSE 4 END, relname;"

page 10_db_etl_run_log.html "Successful ETL execution - run log and step log" "
\\pset title 'etl.pipeline_run (gold builds triggered by NiFi)'
SELECT run_id, pipeline_name, status, triggered_by, full_refresh, started_at, finished_at,
       round(extract(epoch FROM finished_at - started_at)::numeric, 1) AS seconds, error_message
FROM etl.pipeline_run ORDER BY run_id;
\\pset title 'etl.run_step_log - initial load triggered by NiFi (dimensions before facts)'
SELECT step_name, rows_inserted, rows_updated, rows_deleted,
       round(extract(epoch FROM finished_at - started_at)::numeric, 2) AS seconds, message
FROM etl.run_step_log
WHERE run_id = (SELECT min(run_id) FROM etl.pipeline_run WHERE status = 'SUCCEEDED' AND triggered_by = 'nifi')
ORDER BY step_log_id;
\\pset title 'etl.load_audit - batches loaded by NiFi per layer'
SELECT target_layer, count(*) AS batches, sum(record_count) AS records, min(loaded_at) AS first_load, max(loaded_at) AS last_load
FROM etl.load_audit GROUP BY target_layer ORDER BY target_layer;"

page 11_db_gold_sample_rows.html "Gold star-schema facts - sample rows" "
\\pset title 'gold.vw_customer_daily_activity (Scenario A: customer x day)'
SELECT activity_date, customer_no, home_country, subscription_plan, streaming_session_count, streaming_minutes,
       rental_count, returned_item_count, total_amount_spent, support_ticket_count
FROM gold.vw_customer_daily_activity ORDER BY activity_date DESC, customer_no LIMIT 12;
\\pset title 'gold.vw_content_monthly_performance (Scenario B: content x month)'
SELECT performance_month, title, content_type, stream_count, rental_count, rental_revenue,
       allocated_subscription_revenue, total_revenue, avg_customer_rating, wishlist_add_count
FROM gold.vw_content_monthly_performance WHERE performance_month = '2026-05'
ORDER BY popularity_rank_in_month LIMIT 12;
\\pset title 'gold.vw_inventory_daily_utilisation (Scenario C: item x day)'
SELECT snapshot_date, barcode, title, warehouse_name, rental_count, return_count,
       days_rented, days_available, is_overdue, utilisation_pct
FROM gold.vw_inventory_daily_utilisation WHERE snapshot_date = DATE '2026-05-15'
ORDER BY barcode LIMIT 12;"

page 12_db_sample_query_results.html "Sample analytical query results" "
\\pset title 'Monthly business KPIs (gold.vw_monthly_business_kpis)'
SELECT month, active_customers, streaming_hours, rentals, late_returns, subscription_revenue, rental_revenue,
       total_revenue, refunds, inventory_utilisation_pct, revenue_per_active_customer
FROM gold.vw_monthly_business_kpis;
\\pset title 'Streaming and rental cross-usage by subscription plan'
SELECT subscription_plan, count(DISTINCT customer_id) AS active_customers,
       count(DISTINCT customer_id) FILTER (WHERE streaming_session_count > 0) AS streamers,
       count(DISTINCT customer_id) FILTER (WHERE rental_count > 0) AS renters,
       round(sum(total_amount_spent) / NULLIF(count(DISTINCT customer_id), 0), 2) AS avg_spend
FROM gold.vw_customer_daily_activity GROUP BY 1 ORDER BY 2 DESC;
\\pset title 'Warehouse utilisation, May 2026 (top 10)'
SELECT warehouse_name, warehouse_country, content_type, items_in_service, item_days_rented,
       item_days_available, utilisation_pct
FROM gold.vw_warehouse_utilisation_monthly WHERE snapshot_month = '2026-05'
ORDER BY utilisation_pct DESC LIMIT 10;"

page 13_db_data_quality_handling.html "Data-quality handling - rejected records and applied corrections" "
\\pset title 'etl.rejected_record - records NiFi refused to load'
SELECT source_table, reject_reason, count(*) AS records, min(reject_details) AS example_details
FROM etl.rejected_record GROUP BY 1, 2 ORDER BY 1, 2;
\\pset title 'Corrections flagged on silver rows (dq_flags)'
SELECT t AS silver_table, flag AS correction, count(*) AS rows_corrected FROM (
  SELECT 'customer' t, unnest(string_to_array(dq_flags, ',')) flag FROM silver.customer UNION ALL
  SELECT 'customer_address', unnest(string_to_array(dq_flags, ',')) FROM silver.customer_address UNION ALL
  SELECT 'content', unnest(string_to_array(dq_flags, ',')) FROM silver.content UNION ALL
  SELECT 'rental', unnest(string_to_array(dq_flags, ',')) FROM silver.rental UNION ALL
  SELECT 'streaming_session', unnest(string_to_array(dq_flags, ',')) FROM silver.streaming_session UNION ALL
  SELECT 'payment', unnest(string_to_array(dq_flags, ',')) FROM silver.payment UNION ALL
  SELECT 'review', unnest(string_to_array(dq_flags, ',')) FROM silver.review UNION ALL
  SELECT 'support_ticket', unnest(string_to_array(dq_flags, ',')) FROM silver.support_ticket) x
GROUP BY 1, 2 ORDER BY 1, 2;"

# validation suite as an evidence page
{
  echo '<!doctype html><html><head><meta charset="utf-8"><title>Validation tests</title><style>body{font-family:Segoe UI,Arial,sans-serif;margin:24px}h1{font-size:20px;border-bottom:3px solid #b3261e}table{border-collapse:collapse;font-size:12.5px}th,td{border:1px solid #cfd6df;padding:3px 8px}th{background:#eef2f6}</style></head><body><h1>Automated validation suite (sql/03_tests/01_data_validation_tests.sql)</h1>'
  psql -w -X -q -H -d abc_hub_analytics -f "$ROOT/sql/03_tests/01_data_validation_tests.sql" | sed -e 's/border="1"//' \
    -e 's|<td align="left">PASS</td>|<td style="background:#dff3e4;font-weight:600">PASS</td>|' \
    -e 's|<td align="left">FAIL</td>|<td style="background:#fde2e1;font-weight:600">FAIL</td>|'
  echo '</body></html>'
} > "$HTML/14_db_validation_tests.html"

echo "==> capturing screenshots"
node "$ROOT/scripts/capture_screenshots.mjs"
