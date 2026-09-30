-- ============================================================================
-- analytical_queries.sql                    (database: abc_hub_analytics)
--
-- Sample business questions answered from the gold layer. Each one maps to a
-- management requirement from Question 3. Compare the size of these queries
-- with the equivalent against the 26-table operational schema.
-- ============================================================================
\pset pager off

\echo '=== Q1  Revenue: monthly revenue split and active customers (single version of the truth)'
SELECT month, active_customers, streaming_customers, renting_customers,
       subscription_revenue, rental_revenue, total_revenue, refunds, revenue_per_active_customer
FROM gold.vw_monthly_business_kpis;

\echo '=== Q2  Customer activity: how many customers use BOTH streaming and rentals, per plan?'
SELECT subscription_plan,
       count(DISTINCT customer_id)                                              AS active_customers,
       count(DISTINCT customer_id) FILTER (WHERE streaming_session_count > 0)  AS streamers,
       count(DISTINCT customer_id) FILTER (WHERE rental_count > 0)             AS renters,
       round(sum(streaming_minutes) / 60.0 / NULLIF(count(DISTINCT customer_id), 0), 1) AS avg_hours_streamed,
       round(sum(total_amount_spent) / NULLIF(count(DISTINCT customer_id), 0), 2)       AS avg_spend
FROM gold.vw_customer_daily_activity
GROUP BY subscription_plan
ORDER BY active_customers DESC;

\echo '=== Q3  Customer activity by country (location as of the activity date - SCD2)'
SELECT home_country, count(DISTINCT customer_id) AS customers,
       sum(streaming_session_count) AS streams, sum(rental_count) AS rentals,
       sum(total_amount_spent) AS revenue
FROM gold.vw_customer_daily_activity
GROUP BY home_country
ORDER BY revenue DESC
LIMIT 10;

\echo '=== Q4  Streaming performance: weekday vs weekend viewing'
SELECT CASE WHEN is_weekend THEN 'Weekend' ELSE 'Weekday' END AS day_type,
       count(DISTINCT activity_date)                                       AS days,
       round(sum(streaming_session_count)::NUMERIC / count(DISTINCT activity_date), 1) AS streams_per_day,
       round(sum(streaming_minutes)::NUMERIC / NULLIF(sum(streaming_session_count), 0), 1) AS avg_minutes_per_stream,
       round(100.0 * sum(completed_stream_count) / NULLIF(sum(streaming_session_count), 0), 1) AS completion_rate_pct
FROM gold.vw_customer_daily_activity
GROUP BY 1;

\echo '=== Q5  Content popularity: top 10 titles of the last full month'
SELECT popularity_rank_in_month AS rank, title, content_type, primary_genre,
       stream_count, rental_count, total_revenue, avg_customer_rating, wishlist_add_count
FROM gold.vw_content_monthly_performance
WHERE performance_month = '2026-05'
ORDER BY popularity_rank_in_month
LIMIT 10;

\echo '=== Q6  Content popularity by genre over the year (streams, revenue, rating)'
SELECT primary_genre,
       sum(stream_count)                                                        AS streams,
       sum(rental_count)                                                        AS rentals,
       sum(total_revenue)                                                       AS revenue,
       round(sum(avg_customer_rating * rated_review_count) / NULLIF(sum(rated_review_count), 0), 2) AS avg_rating
FROM gold.vw_content_monthly_performance
GROUP BY primary_genre
ORDER BY revenue DESC;

\echo '=== Q7  Physical rental trends: rentals, late returns and utilisation per month'
SELECT k.month, k.rentals, k.returns, k.late_returns,
       round(100.0 * k.late_returns / NULLIF(k.returns, 0), 1) AS late_return_pct,
       k.inventory_utilisation_pct
FROM gold.vw_monthly_business_kpis k;

\echo '=== Q8  Inventory utilisation by warehouse and content type (last full month)'
SELECT warehouse_name, warehouse_country, content_type, items_in_service,
       item_days_rented, item_days_available, utilisation_pct
FROM gold.vw_warehouse_utilisation_monthly
WHERE snapshot_month = '2026-05'
ORDER BY utilisation_pct DESC
LIMIT 12;

\echo '=== Q9  Inventory: titles with high wish-list demand but few physical copies'
SELECT c.title, c.content_type,
       sum(f.wishlist_add_count)                                            AS wishlist_adds,
       (SELECT count(*) FROM gold.dim_inventory_item i
         WHERE i.content_id = c.content_id AND i.is_current AND i.item_status <> 'Retired') AS copies_in_stock
FROM gold.fact_content_monthly_performance f
JOIN gold.dim_content c USING (content_key)
WHERE c.is_rentable
GROUP BY c.content_id, c.title, c.content_type
ORDER BY wishlist_adds DESC, copies_in_stock
LIMIT 10;

\echo '=== Q10 Data Science feature example: 30-day activity profile per customer (last 30 days of data)'
SELECT c.customer_id, c.age_band, c.home_country,
       sum(f.streaming_session_count) AS streams_30d,
       sum(f.rental_count)            AS rentals_30d,
       sum(f.total_amount_spent)      AS spend_30d,
       count(*)                       AS active_days_30d
FROM gold.fact_customer_daily_activity f
JOIN gold.dim_customer c USING (customer_key)
JOIN gold.dim_date d USING (date_key)
WHERE d.full_date > (SELECT max(full_date) FROM gold.dim_date d2
                      JOIN gold.fact_customer_daily_activity f2 USING (date_key)) - 30
GROUP BY c.customer_id, c.age_band, c.home_country
ORDER BY spend_30d DESC
LIMIT 10;
