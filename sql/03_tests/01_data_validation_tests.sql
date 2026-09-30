-- ============================================================================
-- 01_data_validation_tests.sql              (database: abc_hub_analytics)
--
-- Automated checks run after every pipeline execution. Each row is one test:
-- PASS / FAIL with the expected and actual value. Run:
--   psql -d abc_hub_analytics -f sql/03_tests/01_data_validation_tests.sql
-- (scripts/run_tests.sh fails when any row says FAIL)
-- ============================================================================
\pset pager off

WITH
-- clean silver event sets the facts must reconcile to
s_stream   AS (SELECT * FROM silver.streaming_session WHERE NOT is_duplicate),
s_pay      AS (SELECT * FROM silver.payment WHERE NOT is_duplicate),
s_rental   AS (SELECT * FROM silver.rental),
fa AS (SELECT * FROM gold.fact_customer_daily_activity),
fb AS (SELECT * FROM gold.fact_content_monthly_performance),
fc AS (SELECT * FROM gold.fact_inventory_daily_snapshot),
w  AS (SELECT snapshot_start, snapshot_end FROM etl.gold_run_window ORDER BY run_id DESC LIMIT 1),
tests(area, test_name, expected, actual) AS (
    -- ------------------------------------------------ silver data quality
    SELECT 'silver', 'no negative payment amounts', 0::NUMERIC,
           (SELECT count(*) FROM silver.payment WHERE amount < 0)::NUMERIC
    UNION ALL SELECT 'silver', 'no negative late fees', 0, (SELECT count(*) FROM silver.rental WHERE late_fee < 0)
    UNION ALL SELECT 'silver', 'completion_percentage within 0-100', 0,
           (SELECT count(*) FROM silver.streaming_session WHERE completion_percentage NOT BETWEEN 0 AND 100)
    UNION ALL SELECT 'silver', 'watch_duration never null', 0,
           (SELECT count(*) FROM silver.streaming_session WHERE watch_duration IS NULL)
    UNION ALL SELECT 'silver', 'ratings are 1-5 or null', 0,
           (SELECT count(*) FROM silver.review WHERE rating NOT BETWEEN 1 AND 5)
    UNION ALL SELECT 'silver', 'no ticket closed before it opened', 0,
           (SELECT count(*) FROM silver.support_ticket WHERE closed_date < opened_date)
    UNION ALL SELECT 'silver', 'customer status canonical', 0,
           (SELECT count(*) FROM silver.customer WHERE status NOT IN ('Active', 'Inactive', 'Suspended'))
    UNION ALL SELECT 'silver', 'rental status canonical', 0,
           (SELECT count(*) FROM silver.rental WHERE status NOT IN ('Active', 'Overdue', 'Returned'))
    UNION ALL SELECT 'silver', 'ticket priority canonical', 0,
           (SELECT count(*) FROM silver.support_ticket WHERE priority NOT IN ('Low', 'Medium', 'High', 'Urgent'))
    UNION ALL SELECT 'silver', 'e-mails lower-case and trimmed', 0,
           (SELECT count(*) FROM silver.customer WHERE email <> lower(trim(email)))
    UNION ALL SELECT 'silver', 'no duplicate customer e-mails (active rows)', 0,
           (SELECT count(*) - count(DISTINCT email) FROM silver.customer WHERE NOT is_duplicate)
    UNION ALL SELECT 'silver', 'every silver row carries a NiFi batch id', 0,
           (SELECT count(*) FROM silver.customer WHERE _batch_id IS NULL)
         + (SELECT count(*) FROM silver.streaming_session WHERE _batch_id IS NULL)
         + (SELECT count(*) FROM silver.payment WHERE _batch_id IS NULL)
    -- ------------------------------------------------ bronze / audit trail
    UNION ALL SELECT 'bronze', 'every silver row has a bronze version', 0,
           (SELECT count(*) FROM silver.rental s
             WHERE NOT EXISTS (SELECT 1 FROM bronze.rental b WHERE b.rental_id = s.rental_id))
         + (SELECT count(*) FROM silver.customer s
             WHERE NOT EXISTS (SELECT 1 FROM bronze.customer b WHERE b.customer_id = s.customer_id))
    UNION ALL SELECT 'bronze', 'every bronze stream is in silver or in the reject log', 0,
           (SELECT count(DISTINCT b.stream_id) FROM bronze.streaming_session b
             WHERE NOT EXISTS (SELECT 1 FROM silver.streaming_session s WHERE s.stream_id = b.stream_id)
               AND NOT EXISTS (SELECT 1 FROM etl.rejected_record r WHERE r.source_table = 'streaming_session'
                                 AND (r.record_payload ->> 'stream_id')::BIGINT = b.stream_id))
    UNION ALL SELECT 'audit', 'the 93 profiled source duplicates are all in the reject log', 93,
           (SELECT count(*) FROM etl.rejected_record
             WHERE reject_reason = 'DUPLICATE'
               AND ((source_table = 'customer'          AND (record_payload ->> 'customer_id')::BIGINT BETWEEN 1001 AND 1008)
                 OR (source_table = 'streaming_session' AND (record_payload ->> 'stream_id')::BIGINT BETWEEN 48001 AND 48060)
                 OR (source_table = 'payment'           AND (record_payload ->> 'payment_id')::BIGINT BETWEEN 16868 AND 16892)))
    -- (a record can be in both places if it first arrived in a later batch than its
    --  original - then silver holds it FLAGGED is_duplicate - and a replayed extract
    --  later rejected it in-batch; what must never happen is an ACTIVE rejected row)
    UNION ALL SELECT 'audit', 'no rejected record is active (unflagged) in silver', 0,
           (SELECT count(*) FROM etl.rejected_record r WHERE r.source_table = 'streaming_session'
               AND EXISTS (SELECT 1 FROM silver.streaming_session s WHERE NOT s.is_duplicate
                             AND s.stream_id = (r.record_payload ->> 'stream_id')::BIGINT))
         + (SELECT count(*) FROM etl.rejected_record r WHERE r.source_table = 'payment'
               AND EXISTS (SELECT 1 FROM silver.payment s WHERE NOT s.is_duplicate
                             AND s.payment_id = (r.record_payload ->> 'payment_id')::BIGINT))
         + (SELECT count(*) FROM etl.rejected_record r WHERE r.source_table = 'customer'
               AND EXISTS (SELECT 1 FROM silver.customer s WHERE NOT s.is_duplicate
                             AND s.customer_id = (r.record_payload ->> 'customer_id')::BIGINT))
    UNION ALL SELECT 'audit', 'latest gold run succeeded', 1,
           (SELECT (status = 'SUCCEEDED')::INT FROM etl.pipeline_run
             WHERE pipeline_name = 'gold_build' ORDER BY run_id DESC LIMIT 1)
    -- ------------------------------------------------ grain & integrity
    UNION ALL SELECT 'gold', 'fact A grain unique (customer, day)', (SELECT count(*) FROM fa),
           (SELECT count(*) FROM (SELECT DISTINCT date_key, customer_key FROM fa) x)
    UNION ALL SELECT 'gold', 'fact B grain unique (content, month)', (SELECT count(*) FROM fb),
           (SELECT count(*) FROM (SELECT DISTINCT month_key, content_key FROM fb) x)
    UNION ALL SELECT 'gold', 'fact C grain unique (item, day)', (SELECT count(*) FROM fc),
           (SELECT count(*) FROM (SELECT DISTINCT date_key, inventory_key FROM fc) x)
    UNION ALL SELECT 'gold', 'no fact rows on Unknown (-1) members', 0,
           (SELECT count(*) FROM fa WHERE -1 IN (date_key, customer_key, subscription_plan_key))
         + (SELECT count(*) FROM fb WHERE -1 IN (month_key, content_key))
         + (SELECT count(*) FROM fc WHERE -1 IN (date_key, inventory_key, content_key, warehouse_key))
    UNION ALL SELECT 'gold', 'dim_customer: one current version per customer', 0,
           (SELECT count(*) FROM (SELECT customer_id FROM gold.dim_customer WHERE customer_key > 0
                                   GROUP BY customer_id HAVING count(*) FILTER (WHERE is_current) <> 1) x)
    UNION ALL SELECT 'gold', 'dim_customer: SCD2 versions contiguous (no gaps/overlaps)', 0,
           (SELECT count(*) FROM (
                SELECT effective_from, lag(effective_to) OVER (PARTITION BY customer_id ORDER BY effective_from) prev_to
                FROM gold.dim_customer) x WHERE prev_to IS NOT NULL AND effective_from <> prev_to + 1)
    UNION ALL SELECT 'gold', 'dim_inventory_item: one current version per item', 0,
           (SELECT count(*) FROM (SELECT inventory_id FROM gold.dim_inventory_item WHERE inventory_key > 0
                                   GROUP BY inventory_id HAVING count(*) FILTER (WHERE is_current) <> 1) x)
    UNION ALL SELECT 'gold', 'dim_customer rows = active silver customers',
           (SELECT count(*) FROM silver.customer WHERE NOT is_duplicate),
           (SELECT count(*) FROM gold.dim_customer WHERE is_current AND customer_key > 0)
    -- ------------------------------------------------ Scenario A reconciliation
    UNION ALL SELECT 'fact A', 'streaming sessions = silver', (SELECT count(*) FROM s_stream),
           (SELECT sum(streaming_session_count) FROM fa)
    UNION ALL SELECT 'fact A', 'streaming minutes = silver', (SELECT sum(watch_duration) FROM s_stream),
           (SELECT sum(streaming_minutes) FROM fa)
    UNION ALL SELECT 'fact A', 'rentals = silver', (SELECT count(*) FROM s_rental), (SELECT sum(rental_count) FROM fa)
    UNION ALL SELECT 'fact A', 'returned items = silver', (SELECT count(return_date) FROM s_rental),
           (SELECT sum(returned_item_count) FROM fa)
    UNION ALL SELECT 'fact A', 'amount spent = completed payments',
           (SELECT sum(amount) FROM s_pay WHERE status = 'Completed'), (SELECT sum(total_amount_spent) FROM fa)
    UNION ALL SELECT 'fact A', 'support tickets = silver', (SELECT count(*) FROM silver.support_ticket),
           (SELECT sum(support_ticket_count) FROM fa)
    UNION ALL SELECT 'fact A', 'wish-list adds = silver', (SELECT count(*) FROM silver.wishlist),
           (SELECT sum(wishlist_add_count) FROM fa)
    -- ------------------------------------------------ Scenario B reconciliation
    UNION ALL SELECT 'fact B', 'streams = silver', (SELECT count(*) FROM s_stream), (SELECT sum(stream_count) FROM fb)
    UNION ALL SELECT 'fact B', 'rentals = silver', (SELECT count(*) FROM s_rental), (SELECT sum(rental_count) FROM fb)
    UNION ALL SELECT 'fact B', 'rental revenue = completed rental payments',
           (SELECT sum(amount) FROM s_pay WHERE payment_type = 'Rental' AND status = 'Completed'),
           (SELECT sum(rental_revenue) FROM fb)
    UNION ALL SELECT 'fact B', 'allocated subscription revenue = pool (+/- 1.00 rounding)',
           round((SELECT sum(amount) FROM s_pay WHERE payment_type = 'Subscription' AND status = 'Completed'
                    AND to_char(payment_date, 'YYYYMM') IN (SELECT DISTINCT to_char(start_time, 'YYYYMM') FROM s_stream))),
           round((SELECT sum(allocated_subscription_revenue) FROM fb))
    UNION ALL SELECT 'fact B', 'reviews = silver', (SELECT count(*) FROM silver.review), (SELECT sum(review_count) FROM fb)
    UNION ALL SELECT 'fact B', 'rating sum = silver valid ratings', (SELECT sum(rating) FROM silver.review),
           (SELECT sum(rating_sum) FROM fb)
    UNION ALL SELECT 'fact B', 'wish-list adds = silver', (SELECT count(*) FROM silver.wishlist),
           (SELECT sum(wishlist_add_count) FROM fb)
    -- ------------------------------------------------ Scenario C reconciliation
    UNION ALL SELECT 'fact C', 'rentals started in window = silver',
           (SELECT count(*) FROM s_rental, w WHERE rental_date BETWEEN w.snapshot_start AND w.snapshot_end),
           (SELECT sum(rental_count) FROM fc)
    UNION ALL SELECT 'fact C', 'returns in window = silver',
           (SELECT count(*) FROM s_rental, w WHERE return_date BETWEEN w.snapshot_start AND w.snapshot_end),
           (SELECT sum(return_count) FROM fc)
    UNION ALL SELECT 'fact C', 'rented + available = in service on every row', 0,
           (SELECT count(*) FROM fc WHERE days_rented + days_available <> days_in_service)
    UNION ALL SELECT 'fact C', 'utilisation within 0-100', 0,
           (SELECT count(*) FROM fc WHERE utilisation_pct NOT BETWEEN 0 AND 100)
    UNION ALL SELECT 'fact C', 'every day of the window is present', 1,
           (SELECT (count(DISTINCT date_key) = (SELECT snapshot_end - snapshot_start + 1 FROM w))::INT FROM fc)
)
SELECT area, test_name, expected, actual,
       CASE WHEN expected IS NOT DISTINCT FROM actual THEN 'PASS' ELSE 'FAIL' END AS result
FROM tests;
