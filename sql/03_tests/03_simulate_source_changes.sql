-- ============================================================================
-- 03_simulate_source_changes.sql            (database: abc_hub_OPERATIONAL)
--
-- Simulates one new business day (2026-06-12) in the SOURCE system so the
-- incremental behaviour of the pipeline can be demonstrated and tested.
-- Every touched row gets updated_at on 2026-06-12, i.e. later than anything
-- in the original package (max 2026-06-11 03:37), so NiFi's
-- QueryDatabaseTableRecord (max-value column updated_at) picks up ONLY these.
--
-- Scenario                                         expected pipeline behaviour
-- -----------------------------------------------  ----------------------------------------
-- 1 overdue rental 1  is returned late            new bronze version; silver updated; returns
--                                                  and late return in fact A; item available
-- 2 new rental + payment (customer 42)             new fact A / B / C activity on 2026-06-12
-- 3 customer 10 suspended                          dim_customer SCD2: version closed, new opened
-- 4 customer 11 moves to another city             dim_customer SCD2 (home_city changes)
-- 5 new customer 2001 + address + subscription    new dim_customer row, plan on fact A
-- 6 inventory item 5 moves warehouse               dim_inventory_item SCD2
-- 7 two identical new streams (in same batch)      NiFi keeps one, rejects one as DUPLICATE
-- 8 copy of an OLD stream with a new stream_id     loads to silver, flagged is_duplicate by SQL
-- 9 stream with NULL start_time                    NiFi ValidateRecord rejects it (VALIDATION)
-- 10 review with rating 0                          cleansed: rating NULL, dq_flags RATING_INVALID
--
-- Idempotent: fixed ids + ON CONFLICT DO NOTHING; re-running changes nothing.
-- Undo: re-run scripts/setup_databases.sh (reloads the original package).
-- ============================================================================
\set ON_ERROR_STOP on
BEGIN;

-- 1. The oldest still-open rental comes back today, late.
UPDATE rental r
   SET return_date = DATE '2026-06-12', status = 'Returned',
       late_fee    = 0.75 * (DATE '2026-06-12' - r.due_date),
       updated_at  = TIMESTAMP '2026-06-12 10:15:00'
 WHERE r.rental_id = (SELECT min(rental_id) FROM rental WHERE return_date IS NULL
                         AND updated_at < TIMESTAMP '2026-06-12');
UPDATE inventory_item i
   SET status = 'Available', updated_at = TIMESTAMP '2026-06-12 10:15:00'
 WHERE i.inventory_id = (SELECT inventory_id FROM rental WHERE updated_at = TIMESTAMP '2026-06-12 10:15:00')
   AND i.status <> 'Available';

-- 2. Customer 42 rents inventory item 1 today and pays for it.
INSERT INTO rental (rental_id, customer_id, inventory_id, rental_date, due_date, return_date, rental_fee, late_fee,
                    status, created_at, updated_at)
VALUES (900001, 42, 1, DATE '2026-06-12', DATE '2026-06-19', NULL, 3.49, 0, 'Active',
        TIMESTAMP '2026-06-12 11:00:00', TIMESTAMP '2026-06-12 11:00:00')
ON CONFLICT DO NOTHING;
INSERT INTO payment (payment_id, customer_id, rental_id, subscription_id, payment_method_id, amount, payment_date,
                     payment_type, status, created_at, updated_at)
VALUES (900001, 42, 900001, NULL, 1, 3.49, TIMESTAMP '2026-06-12 11:00:30', 'Rental', 'Completed',
        TIMESTAMP '2026-06-12 11:00:30', TIMESTAMP '2026-06-12 11:00:30')
ON CONFLICT DO NOTHING;

-- 3. Customer 10 is suspended (SCD2 on status).
UPDATE customer SET status = 'Suspended', updated_at = TIMESTAMP '2026-06-12 09:00:00'
 WHERE customer_id = 10 AND status <> 'Suspended';

-- 4. Customer 11 moves: home address now in a different city (SCD2 on location).
UPDATE customer_address
   SET city_id = CASE WHEN city_id = 1 THEN 2 ELSE 1 END, address_line = '12 New Harbour Road',
       updated_at = TIMESTAMP '2026-06-12 09:30:00'
 WHERE address_id = (SELECT min(address_id) FROM customer_address WHERE customer_id = 11)
   AND updated_at < TIMESTAMP '2026-06-12';

-- 5. A brand-new customer registers, subscribes and pays.
INSERT INTO customer (customer_id, customer_no, first_name, last_name, email, phone, date_of_birth, gender,
                      registration_date, status, created_at, updated_at)
VALUES (2001, 'CUS-002001', 'Nimal', 'Perera', 'nimal.perera2001@example.com', NULL, DATE '1995-04-02', 'Male',
        DATE '2026-06-12', 'active ', TIMESTAMP '2026-06-12 08:00:00', TIMESTAMP '2026-06-12 08:00:00')
ON CONFLICT DO NOTHING;
INSERT INTO customer_address (address_id, customer_id, city_id, address_line, postal_code, address_type,
                              created_at, updated_at)
VALUES (900001, 2001, 1, '7 Galle Road', '00300', 'home', TIMESTAMP '2026-06-12 08:00:00',
        TIMESTAMP '2026-06-12 08:00:00')
ON CONFLICT DO NOTHING;
INSERT INTO customer_subscription (subscription_id, customer_id, plan_id, start_date, end_date, status, auto_renew,
                                   created_at, updated_at)
VALUES (900001, 2001, 2, DATE '2026-06-12', NULL, 'Active', TRUE, TIMESTAMP '2026-06-12 08:05:00',
        TIMESTAMP '2026-06-12 08:05:00')
ON CONFLICT DO NOTHING;
INSERT INTO payment (payment_id, customer_id, rental_id, subscription_id, payment_method_id, amount, payment_date,
                     payment_type, status, created_at, updated_at)
VALUES (900002, 2001, NULL, 900001, 3, 8.99, TIMESTAMP '2026-06-12 08:05:30', 'Subscription', 'Completed',
        TIMESTAMP '2026-06-12 08:05:30', TIMESTAMP '2026-06-12 08:05:30')
ON CONFLICT DO NOTHING;

-- 6. Inventory item 5 is transferred to another warehouse (SCD2 on warehouse).
UPDATE inventory_item
   SET warehouse_id = CASE WHEN warehouse_id = 1 THEN 2 ELSE 1 END, updated_at = TIMESTAMP '2026-06-12 12:00:00'
 WHERE inventory_id = 5 AND updated_at < TIMESTAMP '2026-06-12';

-- 7. The new customer streams twice today; the second row is an exact
--    duplicate (same customer, content, device, start) under a new id.
INSERT INTO streaming_session (stream_id, customer_id, content_id, device_id, start_time, end_time, watch_duration,
                               completion_percentage, created_at, updated_at)
VALUES (900001, 2001, 2, 1, TIMESTAMP '2026-06-12 20:00:00', TIMESTAMP '2026-06-12 21:30:00', 90, 58.06,
        TIMESTAMP '2026-06-12 20:00:00', TIMESTAMP '2026-06-12 21:30:00'),
       (900002, 2001, 2, 1, TIMESTAMP '2026-06-12 20:00:00', TIMESTAMP '2026-06-12 21:30:00', 90, 58.06,
        TIMESTAMP '2026-06-12 20:00:00', TIMESTAMP '2026-06-12 21:30:00'),
       (900003, 42, 7, 2, TIMESTAMP '2026-06-12 18:00:00', TIMESTAMP '2026-06-12 18:45:00', NULL, 112.40,
        TIMESTAMP '2026-06-12 18:00:00', TIMESTAMP '2026-06-12 18:45:00')
ON CONFLICT DO NOTHING;

-- 8. A late copy of stream 1 (already loaded yesterday) under a new id:
--    arrives in a different batch from its original.
INSERT INTO streaming_session (stream_id, customer_id, content_id, device_id, start_time, end_time, watch_duration,
                               completion_percentage, created_at, updated_at)
SELECT 900004, customer_id, content_id, device_id, start_time, end_time, watch_duration, completion_percentage,
       TIMESTAMP '2026-06-12 13:00:00', TIMESTAMP '2026-06-12 13:00:00'
FROM streaming_session WHERE stream_id = 1
ON CONFLICT DO NOTHING;

-- 9. A broken event: no start_time (allowed by the OLTP schema, mandatory for analytics).
INSERT INTO streaming_session (stream_id, customer_id, content_id, device_id, start_time, end_time, watch_duration,
                               completion_percentage, created_at, updated_at)
VALUES (900005, 42, 3, 1, NULL, TIMESTAMP '2026-06-12 22:00:00', 30, 40.00,
        TIMESTAMP '2026-06-12 22:00:00', TIMESTAMP '2026-06-12 22:00:00')
ON CONFLICT DO NOTHING;

-- 10. A review with an out-of-range rating.
INSERT INTO review (review_id, customer_id, content_id, rating, review_text, review_date, created_at, updated_at)
VALUES (900001, 2001, 2, 0, 'Great start to my subscription!', TIMESTAMP '2026-06-12 21:35:00',
        TIMESTAMP '2026-06-12 21:35:00', TIMESTAMP '2026-06-12 21:35:00')
ON CONFLICT DO NOTHING;

COMMIT;

SELECT 'source rows changed on 2026-06-12' AS info, count(*) AS row_count FROM (
    SELECT updated_at FROM rental WHERE updated_at >= '2026-06-12' UNION ALL
    SELECT updated_at FROM payment WHERE updated_at >= '2026-06-12' UNION ALL
    SELECT updated_at FROM customer WHERE updated_at >= '2026-06-12' UNION ALL
    SELECT updated_at FROM customer_address WHERE updated_at >= '2026-06-12' UNION ALL
    SELECT updated_at FROM customer_subscription WHERE updated_at >= '2026-06-12' UNION ALL
    SELECT updated_at FROM inventory_item WHERE updated_at >= '2026-06-12' UNION ALL
    SELECT updated_at FROM streaming_session WHERE updated_at >= '2026-06-12' UNION ALL
    SELECT updated_at FROM review WHERE updated_at >= '2026-06-12') x;
