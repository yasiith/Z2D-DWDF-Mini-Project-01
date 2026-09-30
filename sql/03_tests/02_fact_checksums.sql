-- ============================================================================
-- 02_fact_checksums.sql                     (database: abc_hub_analytics)
--
-- Content fingerprint of every gold table (row count + md5 of all business
-- columns in key order; lineage columns _run_id/_loaded_at excluded).
-- Used by scripts/test_idempotency.sh: fingerprints taken before and after a
-- repeated pipeline run must be identical.
-- ============================================================================
\pset pager off
SELECT 'fact_customer_daily_activity' AS table_name, count(*) AS row_count,
       md5(string_agg(row(f.date_key, f.customer_key, f.subscription_plan_key, f.streaming_session_count,
                          f.streaming_minutes, f.distinct_titles_streamed, f.completed_stream_count, f.rental_count,
                          f.returned_item_count, f.late_return_count, f.total_amount_spent,
                          f.subscription_amount_spent, f.rental_amount_spent, f.refunded_amount,
                          f.failed_payment_count, f.support_ticket_count, f.review_count,
                          f.wishlist_add_count)::TEXT, '|' ORDER BY f.date_key, f.customer_key)) AS fingerprint
FROM gold.fact_customer_daily_activity f
UNION ALL
SELECT 'fact_content_monthly_performance', count(*),
       md5(string_agg(row(f.month_key, f.content_key, f.stream_count, f.unique_viewer_count, f.streaming_minutes,
                          f.completed_stream_count, f.avg_completion_pct, f.rental_count, f.unique_renter_count,
                          f.rental_revenue, f.allocated_subscription_revenue, f.total_revenue, f.review_count,
                          f.rated_review_count, f.rating_sum, f.avg_customer_rating,
                          f.wishlist_add_count)::TEXT, '|' ORDER BY f.month_key, f.content_key))
FROM gold.fact_content_monthly_performance f
UNION ALL
SELECT 'fact_inventory_daily_snapshot', count(*),
       md5(string_agg(row(f.date_key, f.inventory_key, f.content_key, f.warehouse_key, f.rental_count,
                          f.return_count, f.days_in_service, f.days_rented, f.days_available, f.is_overdue,
                          f.utilisation_pct)::TEXT, '|' ORDER BY f.date_key, f.inventory_key))
FROM gold.fact_inventory_daily_snapshot f
UNION ALL
SELECT 'dim_customer', count(*),
       md5(string_agg(row(d.customer_key, d.customer_id, d.full_name, d.customer_status, d.home_city,
                          d.effective_from, d.effective_to, d.is_current)::TEXT, '|' ORDER BY d.customer_key))
FROM gold.dim_customer d
UNION ALL
SELECT 'dim_inventory_item', count(*),
       md5(string_agg(row(d.inventory_key, d.inventory_id, d.item_status, d.item_condition, d.warehouse_id,
                          d.effective_from, d.effective_to, d.is_current)::TEXT, '|' ORDER BY d.inventory_key))
FROM gold.dim_inventory_item d
UNION ALL
SELECT 'bronze (all versions, 20 tables)',
       (SELECT sum(n_live_tup) FROM pg_stat_user_tables WHERE schemaname = 'bronze'), NULL
ORDER BY 1;
