-- ============================================================================
-- 08_gold_views.sql                         (database: abc_hub_analytics)
--
-- Semantic layer for business users and BI tools (Question 5).
-- Each view pre-joins a fact to its dimensions with business-friendly
-- column names, so an analyst queries ONE object instead of joining 5-26
-- operational tables. Views never expose PII (e-mail, phone, date of birth).
--
-- Non-additive measures (averages, utilisation %) are recomputed from their
-- additive parts at whatever level the view aggregates to.
-- ============================================================================

-- ----------------------------------------------------------------------------
-- Scenario A: customer daily activity, fully labelled
-- ----------------------------------------------------------------------------
CREATE OR REPLACE VIEW gold.vw_customer_daily_activity AS
SELECT d.full_date                 AS activity_date,
       d.year_month_label          AS activity_month,
       d.day_name,
       d.is_weekend,
       c.customer_id,
       c.customer_no,
       c.customer_status,
       c.home_city,
       c.home_country,
       c.gender,
       c.age_band,
       c.registration_month        AS customer_cohort,
       p.plan_name                 AS subscription_plan,
       f.streaming_session_count,
       f.streaming_minutes,
       f.distinct_titles_streamed,
       f.completed_stream_count,
       f.rental_count,
       f.returned_item_count,
       f.late_return_count,
       f.total_amount_spent,
       f.subscription_amount_spent,
       f.rental_amount_spent,
       f.refunded_amount,
       f.failed_payment_count,
       f.support_ticket_count,
       f.review_count,
       f.wishlist_add_count,
       (f.streaming_session_count > 0 AND f.rental_count > 0) AS streamed_and_rented_same_day
FROM gold.fact_customer_daily_activity f
JOIN gold.dim_date d              ON d.date_key = f.date_key
JOIN gold.dim_customer c          ON c.customer_key = f.customer_key
JOIN gold.dim_subscription_plan p ON p.subscription_plan_key = f.subscription_plan_key;

COMMENT ON VIEW gold.vw_customer_daily_activity IS
    'Scenario A - one row per customer per active day, with customer (as of that day), plan and calendar labels';

-- ----------------------------------------------------------------------------
-- Scenario B: content monthly performance, fully labelled
-- ----------------------------------------------------------------------------
CREATE OR REPLACE VIEW gold.vw_content_monthly_performance AS
SELECT m.year_month_label          AS performance_month,
       m.year_number,
       m.quarter_number,
       c.content_id,
       c.title,
       c.content_type,
       c.availability,
       c.primary_genre,
       c.genre_list,
       c.primary_artist,
       c.language,
       c.age_rating,
       c.release_year,
       f.stream_count,
       f.unique_viewer_count,
       f.streaming_minutes,
       f.completed_stream_count,
       f.avg_completion_pct,
       f.rental_count,
       f.unique_renter_count,
       f.rental_revenue,
       f.allocated_subscription_revenue,
       f.total_revenue,
       f.review_count,
       f.rated_review_count,
       f.avg_customer_rating,
       f.wishlist_add_count,
       rank() OVER (PARTITION BY f.month_key ORDER BY f.stream_count + f.rental_count DESC) AS popularity_rank_in_month
FROM gold.fact_content_monthly_performance f
JOIN gold.dim_month m   ON m.month_key = f.month_key
JOIN gold.dim_content c ON c.content_key = f.content_key;

COMMENT ON VIEW gold.vw_content_monthly_performance IS
    'Scenario B - one row per content item per active month, with popularity rank (streams + rentals)';

-- ----------------------------------------------------------------------------
-- Scenario C: inventory utilisation, daily and monthly
-- ----------------------------------------------------------------------------
CREATE OR REPLACE VIEW gold.vw_inventory_daily_utilisation AS
SELECT d.full_date                 AS snapshot_date,
       d.year_month_label          AS snapshot_month,
       i.inventory_id,
       i.barcode,
       i.item_condition,
       i.item_status,
       c.title,
       c.content_type,
       w.warehouse_name,
       w.city                      AS warehouse_city,
       w.country                   AS warehouse_country,
       f.rental_count,
       f.return_count,
       f.days_in_service,
       f.days_rented,
       f.days_available,
       f.is_overdue,
       f.utilisation_pct
FROM gold.fact_inventory_daily_snapshot f
JOIN gold.dim_date d           ON d.date_key = f.date_key
JOIN gold.dim_inventory_item i ON i.inventory_key = f.inventory_key
JOIN gold.dim_content c        ON c.content_key = f.content_key
JOIN gold.dim_warehouse w      ON w.warehouse_key = f.warehouse_key;

CREATE OR REPLACE VIEW gold.vw_warehouse_utilisation_monthly AS
SELECT d.year_month_label                                                  AS snapshot_month,
       w.warehouse_name,
       w.city                                                              AS warehouse_city,
       w.country                                                           AS warehouse_country,
       c.content_type,
       count(DISTINCT f.inventory_key)                                     AS items_in_service,
       sum(f.days_in_service)                                              AS item_days_in_service,
       sum(f.days_rented)                                                  AS item_days_rented,
       sum(f.days_available)                                               AS item_days_available,
       sum(f.rental_count)                                                 AS rentals,
       sum(f.return_count)                                                 AS returns,
       sum(f.is_overdue)                                                   AS overdue_item_days,
       round(100.0 * sum(f.days_rented) / NULLIF(sum(f.days_in_service), 0), 2) AS utilisation_pct
FROM gold.fact_inventory_daily_snapshot f
JOIN gold.dim_date d      ON d.date_key = f.date_key
JOIN gold.dim_warehouse w ON w.warehouse_key = f.warehouse_key
JOIN gold.dim_content c   ON c.content_key = f.content_key
GROUP BY d.year_month_label, w.warehouse_name, w.city, w.country, c.content_type;

COMMENT ON VIEW gold.vw_warehouse_utilisation_monthly IS
    'Scenario C rolled up - utilisation % = rented item-days / in-service item-days (ratio of sums)';

-- ----------------------------------------------------------------------------
-- One trusted monthly KPI set for management (single version of the truth)
-- ----------------------------------------------------------------------------
CREATE OR REPLACE VIEW gold.vw_monthly_business_kpis AS
WITH activity AS (
    SELECT d.year_month_label AS month,
           count(DISTINCT f.customer_key) FILTER (WHERE f.streaming_session_count > 0
                                                     OR f.rental_count > 0)            AS active_customers,
           count(DISTINCT f.customer_key) FILTER (WHERE f.streaming_session_count > 0) AS streaming_customers,
           count(DISTINCT f.customer_key) FILTER (WHERE f.rental_count > 0)            AS renting_customers,
           sum(f.streaming_session_count)                                              AS streaming_sessions,
           round(sum(f.streaming_minutes) / 60.0, 1)                                   AS streaming_hours,
           sum(f.rental_count)                                                         AS rentals,
           sum(f.returned_item_count)                                                  AS returns,
           sum(f.late_return_count)                                                    AS late_returns,
           sum(f.subscription_amount_spent)                                            AS subscription_revenue,
           sum(f.rental_amount_spent)                                                  AS rental_revenue,
           sum(f.total_amount_spent)                                                   AS total_revenue,
           sum(f.refunded_amount)                                                      AS refunds,
           sum(f.support_ticket_count)                                                 AS support_tickets
    FROM gold.fact_customer_daily_activity f
    JOIN gold.dim_date d ON d.date_key = f.date_key
    GROUP BY d.year_month_label),
inventory AS (
    SELECT d.year_month_label AS month,
           round(100.0 * sum(f.days_rented) / NULLIF(sum(f.days_in_service), 0), 2) AS inventory_utilisation_pct
    FROM gold.fact_inventory_daily_snapshot f
    JOIN gold.dim_date d ON d.date_key = f.date_key
    GROUP BY d.year_month_label)
SELECT a.*, i.inventory_utilisation_pct,
       round(a.total_revenue / NULLIF(a.active_customers, 0), 2) AS revenue_per_active_customer
FROM activity a
LEFT JOIN inventory i USING (month)
ORDER BY a.month;

COMMENT ON VIEW gold.vw_monthly_business_kpis IS
    'Management KPI pack per month - every department reads the same numbers from here';
