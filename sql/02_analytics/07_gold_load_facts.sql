-- ============================================================================
-- 07_gold_load_facts.sql                    (database: abc_hub_analytics)
--
-- Silver -> gold, part 2: the three fact loads (run AFTER the dimensions).
--
-- Pattern for every fact (idempotent, incremental):
--   1. read the keys this run must rebuild from etl.gold_change_set
--      (every key on a full refresh or the first run)
--   2. DELETE the fact rows of those keys
--   3. re-aggregate them from silver and INSERT
-- Both happen inside one function call = one transaction, so readers never
-- see a half-loaded fact, and re-running produces exactly the same rows.
-- Duplicates flagged in silver (is_duplicate) are excluded everywhere.
-- Dimension keys are resolved with -1 (Unknown) as a fallback, so referential
-- integrity always holds.
-- ============================================================================

-- ----------------------------------------------------------------------------
-- Scenario A - fact_customer_daily_activity  (customer x day)
-- Each event counts on its own business date:
--   stream -> start_time, rental -> rental_date, return -> return_date,
--   payment -> payment_date, ticket -> opened_date, review -> review_date,
--   wish-list -> added_date.
-- The customer version (SCD2) and the active subscription plan are resolved
-- for that day.
-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION etl.load_fact_customer_daily_activity(p_run_id BIGINT)
RETURNS TABLE (step VARCHAR, rows_inserted BIGINT, rows_updated BIGINT, rows_deleted BIGINT)
LANGUAGE plpgsql AS $$
DECLARE v_started TIMESTAMP := clock_timestamp(); v_full BOOLEAN; v_ins BIGINT; v_del BIGINT;
BEGIN
    SELECT full_refresh INTO v_full FROM etl.gold_run_window WHERE run_id = p_run_id;

    CREATE TEMP TABLE tmp_customers ON COMMIT DROP AS
    SELECT key_id AS customer_id FROM etl.gold_change_set WHERE run_id = p_run_id AND entity = 'customer';

    IF v_full THEN
        DELETE FROM gold.fact_customer_daily_activity;
    ELSE
        DELETE FROM gold.fact_customer_daily_activity f
         USING gold.dim_customer d
         WHERE f.customer_key = d.customer_key
           AND d.customer_id IN (SELECT customer_id FROM tmp_customers);
    END IF;
    GET DIAGNOSTICS v_del = ROW_COUNT;

    WITH events AS (
        SELECT s.customer_id, s.start_time::date AS activity_date,
               1 AS sessions, s.watch_duration AS minutes, s.content_id,
               (s.completion_percentage >= 90)::INT AS completed,
               0 AS rentals, 0 AS returns, 0 AS late_returns,
               0::NUMERIC AS spent, 0::NUMERIC AS sub_spent, 0::NUMERIC AS rental_spent, 0::NUMERIC AS refunded,
               0 AS failed, 0 AS tickets, 0 AS reviews, 0 AS wishlist
        FROM silver.streaming_session s
        WHERE NOT s.is_duplicate AND s.customer_id IN (SELECT customer_id FROM tmp_customers)
        UNION ALL
        SELECT r.customer_id, r.rental_date, 0, 0, NULL, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0
        FROM silver.rental r
        WHERE r.customer_id IN (SELECT customer_id FROM tmp_customers)
        UNION ALL
        SELECT r.customer_id, r.return_date, 0, 0, NULL, 0, 0, 1, (r.return_date > r.due_date)::INT,
               0, 0, 0, 0, 0, 0, 0, 0
        FROM silver.rental r
        WHERE r.return_date IS NOT NULL AND r.customer_id IN (SELECT customer_id FROM tmp_customers)
        UNION ALL
        SELECT p.customer_id, p.payment_date::date, 0, 0, NULL, 0, 0, 0, 0,
               CASE WHEN p.status = 'Completed' THEN p.amount ELSE 0 END,
               CASE WHEN p.status = 'Completed' AND p.payment_type = 'Subscription' THEN p.amount ELSE 0 END,
               CASE WHEN p.status = 'Completed' AND p.payment_type = 'Rental' THEN p.amount ELSE 0 END,
               CASE WHEN p.status = 'Refunded' THEN p.amount ELSE 0 END,
               (p.status = 'Failed')::INT, 0, 0, 0
        FROM silver.payment p
        WHERE NOT p.is_duplicate AND p.customer_id IN (SELECT customer_id FROM tmp_customers)
        UNION ALL
        SELECT t.customer_id, t.opened_date::date, 0, 0, NULL, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0
        FROM silver.support_ticket t
        WHERE t.customer_id IN (SELECT customer_id FROM tmp_customers)
        UNION ALL
        SELECT v.customer_id, v.review_date::date, 0, 0, NULL, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0
        FROM silver.review v
        WHERE v.customer_id IN (SELECT customer_id FROM tmp_customers)
        UNION ALL
        SELECT w.customer_id, w.added_date::date, 0, 0, NULL, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1
        FROM silver.wishlist w
        WHERE w.customer_id IN (SELECT customer_id FROM tmp_customers)),
    daily AS (
        SELECT customer_id, activity_date,
               sum(sessions) AS sessions, sum(minutes) AS minutes, count(DISTINCT content_id) AS titles,
               sum(completed) AS completed, sum(rentals) AS rentals, sum(returns) AS returns,
               sum(late_returns) AS late_returns, sum(spent) AS spent, sum(sub_spent) AS sub_spent,
               sum(rental_spent) AS rental_spent, sum(refunded) AS refunded, sum(failed) AS failed,
               sum(tickets) AS tickets, sum(reviews) AS reviews, sum(wishlist) AS wishlist
        FROM events
        GROUP BY customer_id, activity_date),
    keyed AS (
        SELECT COALESCE(dd.date_key, -1) AS date_key,
               COALESCE(dc.customer_key, -1) AS customer_key,
               COALESCE(dp.subscription_plan_key, CASE WHEN sub.plan_id IS NULL THEN 0 ELSE -1 END)
                   AS subscription_plan_key,
               x.*
        FROM daily x
        LEFT JOIN gold.dim_date dd ON dd.full_date = x.activity_date
        LEFT JOIN gold.dim_customer dc
               ON dc.customer_id = x.customer_id
              AND x.activity_date BETWEEN dc.effective_from AND dc.effective_to
        LEFT JOIN LATERAL (
               SELECT cs.plan_id FROM silver.customer_subscription cs
                WHERE cs.customer_id = x.customer_id
                  AND x.activity_date BETWEEN cs.start_date AND COALESCE(cs.end_date, DATE '9999-12-31')
                ORDER BY cs.start_date DESC, cs.subscription_id DESC
                LIMIT 1) sub ON TRUE
        LEFT JOIN gold.dim_subscription_plan dp ON dp.plan_id = sub.plan_id)
    INSERT INTO gold.fact_customer_daily_activity (
        date_key, customer_key, subscription_plan_key, streaming_session_count, streaming_minutes,
        distinct_titles_streamed, completed_stream_count, rental_count, returned_item_count, late_return_count,
        total_amount_spent, subscription_amount_spent, rental_amount_spent, refunded_amount, failed_payment_count,
        support_ticket_count, review_count, wishlist_add_count, _run_id)
    SELECT date_key, customer_key, max(subscription_plan_key),
           sum(sessions), sum(minutes), sum(titles), sum(completed), sum(rentals), sum(returns), sum(late_returns),
           sum(spent), sum(sub_spent), sum(rental_spent), sum(refunded), sum(failed),
           sum(tickets), sum(reviews), sum(wishlist), p_run_id
    FROM keyed
    GROUP BY date_key, customer_key;   -- only differs from (customer, day) for Unknown (-1) members
    GET DIAGNOSTICS v_ins = ROW_COUNT;

    PERFORM etl.log_step(p_run_id, 'load_fact_customer_daily_activity', v_started, v_ins, 0, v_del,
                         format('%s customers rebuilt', (SELECT count(*) FROM tmp_customers)));
    RETURN QUERY SELECT 'load_fact_customer_daily_activity'::VARCHAR, v_ins, 0::BIGINT, v_del;
END $$;

-- ----------------------------------------------------------------------------
-- Scenario B - fact_content_monthly_performance  (content x month)
-- Revenue attribution (assumption, per the data-package README):
--   rental_revenue                 completed Rental payments, by payment month,
--                                  attributed via rental -> inventory item -> content
--   allocated_subscription_revenue the month's completed Subscription revenue
--                                  shared across titles by their share of that
--                                  month's streaming minutes (usage-based proxy)
-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION etl.load_fact_content_monthly_performance(p_run_id BIGINT)
RETURNS TABLE (step VARCHAR, rows_inserted BIGINT, rows_updated BIGINT, rows_deleted BIGINT)
LANGUAGE plpgsql AS $$
DECLARE v_started TIMESTAMP := clock_timestamp(); v_full BOOLEAN; v_ins BIGINT; v_del BIGINT;
BEGIN
    SELECT full_refresh INTO v_full FROM etl.gold_run_window WHERE run_id = p_run_id;

    CREATE TEMP TABLE tmp_months ON COMMIT DROP AS
    SELECT key_id::INT AS month_key FROM etl.gold_change_set WHERE run_id = p_run_id AND entity = 'month';

    IF v_full THEN
        DELETE FROM gold.fact_content_monthly_performance;
    ELSE
        DELETE FROM gold.fact_content_monthly_performance WHERE month_key IN (SELECT month_key FROM tmp_months);
    END IF;
    GET DIAGNOSTICS v_del = ROW_COUNT;

    WITH streams AS (
        SELECT content_id, to_char(start_time, 'YYYYMM')::INT AS month_key,
               count(*) AS stream_count, count(DISTINCT customer_id) AS viewers,
               sum(watch_duration) AS minutes, count(*) FILTER (WHERE completion_percentage >= 90) AS completed,
               round(avg(completion_percentage), 2) AS avg_completion
        FROM silver.streaming_session
        WHERE NOT is_duplicate AND to_char(start_time, 'YYYYMM')::INT IN (SELECT month_key FROM tmp_months)
        GROUP BY 1, 2),
    rentals AS (
        SELECT i.content_id, to_char(r.rental_date, 'YYYYMM')::INT AS month_key,
               count(*) AS rental_count, count(DISTINCT r.customer_id) AS renters
        FROM silver.rental r JOIN silver.inventory_item i USING (inventory_id)
        WHERE to_char(r.rental_date, 'YYYYMM')::INT IN (SELECT month_key FROM tmp_months)
        GROUP BY 1, 2),
    rental_revenue AS (
        SELECT i.content_id, to_char(p.payment_date, 'YYYYMM')::INT AS month_key, sum(p.amount) AS revenue
        FROM silver.payment p
        JOIN silver.rental r USING (rental_id)
        JOIN silver.inventory_item i ON i.inventory_id = r.inventory_id
        WHERE NOT p.is_duplicate AND p.payment_type = 'Rental' AND p.status = 'Completed'
          AND to_char(p.payment_date, 'YYYYMM')::INT IN (SELECT month_key FROM tmp_months)
        GROUP BY 1, 2),
    subscription_pool AS (
        SELECT to_char(payment_date, 'YYYYMM')::INT AS month_key, sum(amount) AS pool
        FROM silver.payment
        WHERE NOT is_duplicate AND payment_type = 'Subscription' AND status = 'Completed'
          AND to_char(payment_date, 'YYYYMM')::INT IN (SELECT month_key FROM tmp_months)
        GROUP BY 1),
    month_minutes AS (
        SELECT month_key, sum(minutes) AS total_minutes FROM streams GROUP BY 1),
    reviews AS (
        SELECT content_id, to_char(review_date, 'YYYYMM')::INT AS month_key,
               count(*) AS review_count, count(rating) AS rated, COALESCE(sum(rating), 0) AS rating_sum
        FROM silver.review
        WHERE to_char(review_date, 'YYYYMM')::INT IN (SELECT month_key FROM tmp_months)
        GROUP BY 1, 2),
    wishlists AS (
        SELECT content_id, to_char(added_date, 'YYYYMM')::INT AS month_key, count(*) AS adds
        FROM silver.wishlist
        WHERE to_char(added_date, 'YYYYMM')::INT IN (SELECT month_key FROM tmp_months)
        GROUP BY 1, 2),
    grain AS (
        SELECT content_id, month_key FROM streams UNION
        SELECT content_id, month_key FROM rentals UNION
        SELECT content_id, month_key FROM rental_revenue UNION
        SELECT content_id, month_key FROM reviews UNION
        SELECT content_id, month_key FROM wishlists),
    measures AS (
        SELECT g.content_id, g.month_key,
               COALESCE(s.stream_count, 0) AS stream_count, COALESCE(s.viewers, 0) AS viewers,
               COALESCE(s.minutes, 0) AS minutes, COALESCE(s.completed, 0) AS completed, s.avg_completion,
               COALESCE(r.rental_count, 0) AS rental_count, COALESCE(r.renters, 0) AS renters,
               COALESCE(rr.revenue, 0) AS rental_revenue,
               COALESCE(round(sp.pool * s.minutes / NULLIF(mm.total_minutes, 0), 2), 0) AS allocated,
               COALESCE(v.review_count, 0) AS review_count, COALESCE(v.rated, 0) AS rated,
               COALESCE(v.rating_sum, 0) AS rating_sum, COALESCE(w.adds, 0) AS adds
        FROM grain g
        LEFT JOIN streams s            ON s.content_id = g.content_id AND s.month_key = g.month_key
        LEFT JOIN rentals r            ON r.content_id = g.content_id AND r.month_key = g.month_key
        LEFT JOIN rental_revenue rr    ON rr.content_id = g.content_id AND rr.month_key = g.month_key
        LEFT JOIN subscription_pool sp ON sp.month_key = g.month_key
        LEFT JOIN month_minutes mm     ON mm.month_key = g.month_key
        LEFT JOIN reviews v            ON v.content_id = g.content_id AND v.month_key = g.month_key
        LEFT JOIN wishlists w          ON w.content_id = g.content_id AND w.month_key = g.month_key)
    INSERT INTO gold.fact_content_monthly_performance (
        month_key, content_key, stream_count, unique_viewer_count, streaming_minutes, completed_stream_count,
        avg_completion_pct, rental_count, unique_renter_count, rental_revenue, allocated_subscription_revenue,
        total_revenue, review_count, rated_review_count, rating_sum, avg_customer_rating, wishlist_add_count, _run_id)
    SELECT COALESCE(dm.month_key, -1), COALESCE(dc.content_key, -1),
           sum(stream_count), sum(viewers), sum(minutes), sum(completed), max(avg_completion),
           sum(rental_count), sum(renters), sum(rental_revenue), sum(allocated), sum(rental_revenue + allocated),
           sum(review_count), sum(rated), sum(rating_sum),
           round(sum(rating_sum)::NUMERIC / NULLIF(sum(rated), 0), 2),
           sum(adds), p_run_id
    FROM measures m
    LEFT JOIN gold.dim_month dm   ON dm.month_key = m.month_key
    LEFT JOIN gold.dim_content dc ON dc.content_id = m.content_id
    GROUP BY 1, 2;
    GET DIAGNOSTICS v_ins = ROW_COUNT;

    PERFORM etl.log_step(p_run_id, 'load_fact_content_monthly_performance', v_started, v_ins, 0, v_del,
                         format('%s months rebuilt', (SELECT count(*) FROM tmp_months)));
    RETURN QUERY SELECT 'load_fact_content_monthly_performance'::VARCHAR, v_ins, 0::BIGINT, v_del;
END $$;

-- ----------------------------------------------------------------------------
-- Scenario C - fact_inventory_daily_snapshot  (inventory item x day)
-- Dense periodic snapshot over the snapshot window (first -> last business
-- date in silver). Rules:
--   * an item is in service from in_service_date (LEAST(purchase, first rental))
--   * on loan on day D  <=>  rental_date <= D < return_date (a same-day rental
--     and return counts as one day on loan); open rentals stay on loan
--   * several overlapping rentals of one copy (a source data issue) still
--     count as ONE rented day, so utilisation never exceeds 100 %
--   * Retired copies leave service the day after their last return (their
--     retirement date is unknown in the source); a later SCD2 change to
--     'Retired' uses its effective date instead
-- Rebuilt: items in the change set (whole window). All other items: only the
-- new days since the previous run's snapshot end are appended.
-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION etl.load_fact_inventory_daily_snapshot(p_run_id BIGINT)
RETURNS TABLE (step VARCHAR, rows_inserted BIGINT, rows_updated BIGINT, rows_deleted BIGINT)
LANGUAGE plpgsql AS $$
DECLARE
    v_started TIMESTAMP := clock_timestamp();
    v_full BOOLEAN; v_start DATE; v_end DATE; v_prev_end DATE;
    v_ins BIGINT; v_del BIGINT;
BEGIN
    SELECT full_refresh, snapshot_start, snapshot_end, previous_snapshot_end
      INTO v_full, v_start, v_end, v_prev_end
      FROM etl.gold_run_window WHERE run_id = p_run_id;

    -- Which items get which date range in this run.
    CREATE TEMP TABLE tmp_targets ON COMMIT DROP AS
    SELECT i.inventory_id, v_start AS from_date, v_end AS to_date
    FROM silver.inventory_item i
    WHERE v_full OR v_prev_end IS NULL
       OR i.inventory_id IN (SELECT key_id FROM etl.gold_change_set WHERE run_id = p_run_id AND entity = 'inventory')
    UNION ALL
    SELECT i.inventory_id, v_prev_end + 1, v_end
    FROM silver.inventory_item i
    WHERE NOT v_full AND v_prev_end IS NOT NULL AND v_prev_end < v_end
      AND i.inventory_id NOT IN (SELECT key_id FROM etl.gold_change_set WHERE run_id = p_run_id AND entity = 'inventory');

    IF v_full OR v_prev_end IS NULL THEN
        DELETE FROM gold.fact_inventory_daily_snapshot;
    ELSE
        DELETE FROM gold.fact_inventory_daily_snapshot f
         USING gold.dim_inventory_item d, tmp_targets t
         WHERE f.inventory_key = d.inventory_key AND d.inventory_id = t.inventory_id
           AND f.date_key BETWEEN to_char(t.from_date, 'YYYYMMDD')::INT AND to_char(t.to_date, 'YYYYMMDD')::INT;
    END IF;
    GET DIAGNOSTICS v_del = ROW_COUNT;

    WITH item AS (
        SELECT t.inventory_id, t.from_date, t.to_date, i.content_id,
               LEAST(i.purchase_date, fr.first_rental) AS in_service_date,
               -- day the copy leaves service if it is Retired and not on loan
               CASE WHEN i.status = 'Retired'
                    THEN COALESCE((SELECT max(GREATEST(r.rental_date, r.return_date)) + 1 FROM silver.rental r
                                    WHERE r.inventory_id = t.inventory_id),
                                  LEAST(i.purchase_date, fr.first_rental))
               END AS derived_retired_from
        FROM tmp_targets t
        JOIN silver.inventory_item i ON i.inventory_id = t.inventory_id
        LEFT JOIN LATERAL (SELECT min(rental_date) AS first_rental FROM silver.rental r
                            WHERE r.inventory_id = t.inventory_id) fr ON TRUE),
    days AS (
        SELECT it.*, gs::date AS snap_date
        FROM item it
        CROSS JOIN LATERAL generate_series(GREATEST(it.from_date, it.in_service_date), it.to_date,
                                           INTERVAL '1 day') AS gs),
    target_rentals AS (
        SELECT r.* FROM silver.rental r
        WHERE r.inventory_id IN (SELECT inventory_id FROM tmp_targets)),
    -- Every rental expanded once into the days it is on loan
    -- (rental_date .. return_date - 1; open rentals run to the window end).
    loan_days AS (
        SELECT r.inventory_id, gs::date AS snap_date, bool_or(gs::date > r.due_date) AS overdue
        FROM target_rentals r
        CROSS JOIN LATERAL generate_series(
                 r.rental_date,
                 GREATEST(COALESCE(r.return_date, v_end + 1), r.rental_date + 1) - 1,
                 INTERVAL '1 day') AS gs
        GROUP BY 1, 2),                      -- overlapping rentals collapse to one loan day
    starts AS (
        SELECT inventory_id, rental_date AS snap_date, count(*) AS n
        FROM target_rentals GROUP BY 1, 2),
    returns AS (
        SELECT inventory_id, return_date AS snap_date, count(*) AS n
        FROM target_rentals WHERE return_date IS NOT NULL GROUP BY 1, 2),
    daily AS (
        SELECT d.inventory_id, d.snap_date, d.content_id, d.derived_retired_from,
               COALESCE(s.n, 0)              AS rentals_started,
               COALESCE(rt.n, 0)             AS returns,
               (ld.inventory_id IS NOT NULL) AS on_loan,
               COALESCE(ld.overdue, FALSE)   AS overdue
        FROM days d
        LEFT JOIN loan_days ld ON ld.inventory_id = d.inventory_id AND ld.snap_date = d.snap_date
        LEFT JOIN starts s     ON s.inventory_id  = d.inventory_id AND s.snap_date  = d.snap_date
        LEFT JOIN returns rt   ON rt.inventory_id = d.inventory_id AND rt.snap_date = d.snap_date),
    keyed AS (
        SELECT to_char(x.snap_date, 'YYYYMMDD')::INT AS date_key,
               COALESCE(di.inventory_key, -1) AS inventory_key,
               COALESCE(dc.content_key, -1) AS content_key,
               COALESCE(dw.warehouse_key, -1) AS warehouse_key,
               x.rentals_started, x.returns, x.on_loan, x.overdue,
               -- out of service: retired and not on loan
               COALESCE(di.item_status = 'Retired'
                        AND (di.effective_from > DATE '1900-01-01' OR x.snap_date >= x.derived_retired_from)
                        AND NOT x.on_loan, FALSE) AS retired
        FROM daily x
        LEFT JOIN gold.dim_inventory_item di
               ON di.inventory_id = x.inventory_id
              AND x.snap_date BETWEEN di.effective_from AND di.effective_to
        LEFT JOIN gold.dim_content dc   ON dc.content_id = x.content_id
        LEFT JOIN gold.dim_warehouse dw ON dw.warehouse_id = di.warehouse_id)
    INSERT INTO gold.fact_inventory_daily_snapshot (
        date_key, inventory_key, content_key, warehouse_key, rental_count, return_count, days_in_service,
        days_rented, days_available, is_overdue, utilisation_pct, _run_id)
    SELECT date_key, inventory_key, content_key, warehouse_key, rentals_started, returns, 1,
           on_loan::INT, (NOT on_loan)::INT, overdue::INT, CASE WHEN on_loan THEN 100 ELSE 0 END, p_run_id
    FROM keyed
    WHERE NOT retired;
    GET DIAGNOSTICS v_ins = ROW_COUNT;

    PERFORM etl.log_step(p_run_id, 'load_fact_inventory_daily_snapshot', v_started, v_ins, 0, v_del,
                         format('window %s..%s, %s items targeted', v_start, v_end,
                                (SELECT count(*) FROM tmp_targets)));
    RETURN QUERY SELECT 'load_fact_inventory_daily_snapshot'::VARCHAR, v_ins, 0::BIGINT, v_del;
END $$;
