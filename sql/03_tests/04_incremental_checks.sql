-- ============================================================================
-- 04_incremental_checks.sql                 (database: abc_hub_analytics)
--
-- Run after 03_simulate_source_changes.sql + one pipeline execution.
-- Verifies each simulated scenario end to end (PASS / FAIL per check).
-- ============================================================================
\pset pager off

WITH
last_run AS (SELECT max(run_id) AS run_id FROM etl.pipeline_run WHERE status = 'SUCCEEDED'),
returned AS (SELECT rental_id, inventory_id, customer_id FROM silver.rental
              WHERE updated_at = TIMESTAMP '2026-06-12 10:15:00'),
checks(scenario, check_name, expected, actual) AS (
    -- 1. late return
    SELECT '1 late return', 'bronze keeps BOTH versions of the returned rental', '2',
           (SELECT count(*)::TEXT FROM bronze.rental WHERE rental_id = (SELECT rental_id FROM returned))
    UNION ALL SELECT '1 late return', 'silver rental now Returned on 2026-06-12', 'Returned 2026-06-12',
           (SELECT status || ' ' || return_date FROM silver.rental WHERE rental_id = (SELECT rental_id FROM returned))
    UNION ALL SELECT '1 late return', 'fact A: return counted on 2026-06-12 as late', '1/1',
           (SELECT sum(f.returned_item_count) || '/' || sum(f.late_return_count)
              FROM gold.fact_customer_daily_activity f JOIN gold.dim_customer c USING (customer_key)
             WHERE f.date_key = 20260612 AND c.customer_id = (SELECT customer_id FROM returned))
    UNION ALL SELECT '1 late return', 'fact C: item back on the shelf on 2026-06-12', '0 rented / 1 return',
           (SELECT f.days_rented || ' rented / ' || f.return_count || ' return'
              FROM gold.fact_inventory_daily_snapshot f JOIN gold.dim_inventory_item i USING (inventory_key)
             WHERE f.date_key = 20260612 AND i.inventory_id = (SELECT inventory_id FROM returned))
    -- 2. new rental
    UNION ALL SELECT '2 new rental', 'fact A: customer 42 rented and paid on 2026-06-12', '1 rental / 3.49',
           (SELECT sum(f.rental_count) || ' rental / ' || sum(f.rental_amount_spent)
              FROM gold.fact_customer_daily_activity f JOIN gold.dim_customer c USING (customer_key)
             WHERE f.date_key = 20260612 AND c.customer_id = 42)
    UNION ALL SELECT '2 new rental', 'fact C: item 1 rented on 2026-06-12', '1',
           (SELECT max(f.rental_count)::TEXT FROM gold.fact_inventory_daily_snapshot f
              JOIN gold.dim_inventory_item i USING (inventory_key)
             WHERE f.date_key = 20260612 AND i.inventory_id = 1)
    -- 3. SCD2 status
    UNION ALL SELECT '3 SCD2 status', 'customer 10 has 2 versions, old one closed 2026-06-11',
           '2 versions; closed 2026-06-11; current Suspended',
           (SELECT count(*) || ' versions; closed ' || max(effective_to) FILTER (WHERE NOT is_current)
                   || '; current ' || max(customer_status) FILTER (WHERE is_current)
              FROM gold.dim_customer WHERE customer_id = 10)
    UNION ALL SELECT '3 SCD2 status', 'history preserved: May activity still points at the Active version', 'Active',
           (SELECT DISTINCT c.customer_status FROM gold.fact_customer_daily_activity f
              JOIN gold.dim_customer c USING (customer_key) JOIN gold.dim_date d USING (date_key)
             WHERE c.customer_id = 10 AND d.full_date < DATE '2026-06-12' LIMIT 1)
    -- 4. SCD2 location
    UNION ALL SELECT '4 SCD2 location', 'customer 11 has a new current home city', '2 versions',
           (SELECT count(*) || ' versions' FROM gold.dim_customer
             WHERE customer_id = 11 AND (SELECT count(DISTINCT home_city) FROM gold.dim_customer WHERE customer_id = 11) = 2)
    -- 5. new customer
    UNION ALL SELECT '5 new customer', 'customer 2001 in dim with cleaned status', 'Active',
           (SELECT customer_status FROM gold.dim_customer WHERE customer_id = 2001 AND is_current)
    UNION ALL SELECT '5 new customer', 'fact A: 1 stream (duplicate removed) on the Standard plan', '1 stream / Standard',
           (SELECT f.streaming_session_count || ' stream / ' || p.plan_name
              FROM gold.fact_customer_daily_activity f
              JOIN gold.dim_customer c USING (customer_key)
              JOIN gold.dim_subscription_plan p USING (subscription_plan_key)
             WHERE f.date_key = 20260612 AND c.customer_id = 2001)
    -- 6. SCD2 inventory
    UNION ALL SELECT '6 SCD2 inventory', 'item 5 has 2 versions (warehouse transfer)', '2',
           (SELECT count(*)::TEXT FROM gold.dim_inventory_item WHERE inventory_id = 5)
    UNION ALL SELECT '6 SCD2 inventory', 'item 5 snapshot uses the new warehouse from 2026-06-12', '2',
           (SELECT count(DISTINCT f.warehouse_key)::TEXT FROM gold.fact_inventory_daily_snapshot f
              JOIN gold.dim_inventory_item i USING (inventory_key)
             WHERE i.inventory_id = 5 AND f.date_key IN (20260611, 20260612))
    -- 7. in-batch duplicate
    UNION ALL SELECT '7 in-batch duplicate', 'stream 900002 rejected by NiFi as DUPLICATE', '1',
           (SELECT count(*)::TEXT FROM etl.rejected_record
             WHERE reject_reason = 'DUPLICATE' AND record_payload ->> 'stream_id' = '900002')
    UNION ALL SELECT '7 in-batch duplicate', 'stream 900002 never reached silver', '0',
           (SELECT count(*)::TEXT FROM silver.streaming_session WHERE stream_id = 900002)
    -- 8. cross-batch duplicate
    UNION ALL SELECT '8 cross-batch duplicate', 'stream 900004 flagged in silver as duplicate of stream 1', 'true / 1',
           (SELECT is_duplicate || ' / ' || duplicate_of_id FROM silver.streaming_session WHERE stream_id = 900004)
    -- 9. validation failure
    UNION ALL SELECT '9 invalid record', 'stream 900005 rejected by ValidateRecord with details', '1',
           (SELECT count(*)::TEXT FROM etl.rejected_record
             WHERE reject_reason = 'VALIDATION' AND record_payload ->> 'stream_id' = '900005'
               AND reject_details ILIKE '%start_time%')
    UNION ALL SELECT '9 invalid record', 'stream 900005 never reached silver', '0',
           (SELECT count(*)::TEXT FROM silver.streaming_session WHERE stream_id = 900005)
    -- 10. cleansing on new data
    UNION ALL SELECT '10 cleansing', 'review 900001 rating nulled + flagged', 'NULL RATING_INVALID',
           (SELECT COALESCE(rating::TEXT, 'NULL') || ' ' || dq_flags FROM silver.review WHERE review_id = 900001)
    UNION ALL SELECT '10 cleansing', 'stream 900003 duration derived + completion capped',
           '45 / 100.00 / WATCH_DURATION_DERIVED,COMPLETION_CAPPED',
           (SELECT watch_duration || ' / ' || completion_percentage || ' / ' || dq_flags
              FROM silver.streaming_session WHERE stream_id = 900003)
    -- incremental scope
    UNION ALL SELECT 'incremental', 'last gold run rebuilt only changed customers (< 20 of 1001)', 'true',
           (SELECT (count(*) < 20)::TEXT FROM etl.gold_change_set
             WHERE run_id = (SELECT run_id FROM last_run) AND entity = 'customer')
    UNION ALL SELECT 'incremental', 'snapshot extended to 2026-06-12 for every in-service item', 'true',
           (SELECT (count(*) >= 1600)::TEXT FROM gold.fact_inventory_daily_snapshot WHERE date_key = 20260612)
)
SELECT scenario, check_name, expected, actual,
       CASE WHEN expected IS NOT DISTINCT FROM actual THEN 'PASS' ELSE 'FAIL' END AS result
FROM checks;

\echo '--- steps of the incremental gold run'
SELECT step_name, rows_inserted, rows_updated, rows_deleted, message
FROM etl.run_step_log
WHERE run_id = (SELECT max(run_id) FROM etl.pipeline_run WHERE status = 'SUCCEEDED')
ORDER BY step_log_id;
