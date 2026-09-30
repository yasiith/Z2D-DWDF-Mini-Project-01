-- ============================================================================
-- data_quality_profiling.sql
-- Profiles the OPERATIONAL database and produces a data-quality scorecard.
-- Every finding here drives a rule in the NiFi transformation stage
-- (see nifi/flow_config.py) or in the silver -> gold SQL.
--
-- Run:  psql -d abc_hub_operational -f sql/01_profiling/data_quality_profiling.sql
-- ============================================================================

\pset pager off
\echo '=== 1. Data-quality scorecard (one row per check) ==='

WITH checks AS (
    -- ---------------- duplicates ----------------
    SELECT 'customer' AS table_name, 'DUPLICATE' AS category,
           'Same e-mail registered under two customer_ids' AS rule,
           (SELECT count(*) FROM (
                SELECT customer_id, row_number() OVER (PARTITION BY lower(trim(email)) ORDER BY customer_id) rn
                FROM customer) x WHERE rn > 1) AS rows_affected,
           'Keep lowest customer_id, route the later copy to etl.rejected_record' AS handling
    UNION ALL
    SELECT 'streaming_session', 'DUPLICATE',
           'Same customer, content, device and start_time under two stream_ids',
           (SELECT count(*) FROM (
                SELECT row_number() OVER (PARTITION BY customer_id, content_id, device_id, start_time ORDER BY stream_id) rn
                FROM streaming_session) x WHERE rn > 1),
           'Keep lowest stream_id, reject the copy'
    UNION ALL
    SELECT 'payment', 'DUPLICATE',
           'Same customer, type, amount, timestamp and rental/subscription',
           (SELECT count(*) FROM (
                SELECT row_number() OVER (PARTITION BY customer_id, payment_type, amount, payment_date,
                                                       rental_id, subscription_id ORDER BY payment_id) rn
                FROM payment) x WHERE rn > 1),
           'Keep lowest payment_id, reject the copy'
    -- ---------------- inconsistent text ----------------
    UNION ALL
    SELECT 'customer', 'INCONSISTENT_TEXT', 'status not in canonical form (Active/Inactive/Suspended)',
           (SELECT count(*) FROM customer WHERE status NOT IN ('Active', 'Inactive', 'Suspended')),
           'TRIM + title-case'
    UNION ALL
    SELECT 'customer', 'INCONSISTENT_TEXT', 'email has upper-case letters or surrounding spaces',
           (SELECT count(*) FROM customer WHERE email <> lower(trim(email))),
           'LOWER(TRIM(email))'
    UNION ALL
    SELECT 'customer_address', 'INCONSISTENT_TEXT', 'address_type not title-case',
           (SELECT count(*) FROM customer_address WHERE address_type NOT IN ('Home', 'Billing')),
           'Title-case'
    UNION ALL
    SELECT 'rental', 'INCONSISTENT_TEXT', 'status not title-case (RETURNED/OVERDUE/ACTIVE)',
           (SELECT count(*) FROM rental WHERE status NOT IN ('Returned', 'Overdue', 'Active')),
           'Title-case'
    UNION ALL
    SELECT 'support_ticket', 'INCONSISTENT_TEXT', 'priority not title-case',
           (SELECT count(*) FROM support_ticket WHERE priority NOT IN ('Low', 'Medium', 'High', 'Urgent')),
           'Title-case'
    -- ---------------- missing values ----------------
    UNION ALL
    SELECT 'customer', 'MISSING', 'gender is null', (SELECT count(*) FROM customer WHERE gender IS NULL),
           'Default to ''Unknown'''
    UNION ALL
    SELECT 'customer', 'MISSING', 'phone is null (optional field)', (SELECT count(*) FROM customer WHERE phone IS NULL),
           'Keep NULL (optional)'
    UNION ALL
    SELECT 'customer_address', 'MISSING', 'postal_code is null (optional field)',
           (SELECT count(*) FROM customer_address WHERE postal_code IS NULL), 'Keep NULL (optional)'
    UNION ALL
    SELECT 'content', 'MISSING', 'language is null',
           (SELECT count(*) FROM content WHERE language IS NULL), 'Default to ''Unknown'''
    UNION ALL
    SELECT 'content', 'NOT_APPLICABLE', 'duration_minutes null - only Books and Video Games',
           (SELECT count(*) FROM content c JOIN content_type t USING (content_type_id)
             WHERE c.duration_minutes IS NULL AND t.content_type IN ('Book', 'Video Game')),
           'Legitimate: keep NULL, duration band = ''Not applicable'''
    UNION ALL
    SELECT 'streaming_session', 'MISSING', 'watch_duration is null',
           (SELECT count(*) FROM streaming_session WHERE watch_duration IS NULL),
           'Derive from end_time - start_time (exact match on all populated rows)'
    UNION ALL
    SELECT 'review', 'MISSING', 'review_text is null (optional)',
           (SELECT count(*) FROM review WHERE review_text IS NULL), 'Keep NULL (rating still valid)'
    -- ---------------- business-rule violations ----------------
    UNION ALL
    SELECT 'streaming_session', 'BUSINESS_RULE', 'completion_percentage > 100',
           (SELECT count(*) FROM streaming_session WHERE completion_percentage > 100),
           'Cap at 100 and flag COMPLETION_CAPPED'
    UNION ALL
    SELECT 'rental', 'BUSINESS_RULE', 'late_fee < 0',
           (SELECT count(*) FROM rental WHERE late_fee < 0),
           'ABS() when the item came back late / is overdue, otherwise 0'
    UNION ALL
    SELECT 'payment', 'BUSINESS_RULE', 'amount <= 0 (sign error)',
           (SELECT count(*) FROM payment WHERE amount <= 0),
           'ABS(amount) - equals plan fee / rental charge after the fix'
    UNION ALL
    SELECT 'review', 'BUSINESS_RULE', 'rating outside 1-5',
           (SELECT count(*) FROM review WHERE rating NOT BETWEEN 1 AND 5),
           'Set rating NULL (excluded from averages) and flag'
    UNION ALL
    SELECT 'support_ticket', 'BUSINESS_RULE', 'closed_date earlier than opened_date',
           (SELECT count(*) FROM support_ticket WHERE closed_date < opened_date),
           'Set closed_date NULL (resolution time unknown) and flag'
    UNION ALL
    SELECT 'rental', 'BUSINESS_RULE', 'item rented before its purchase_date',
           (SELECT count(*) FROM rental r JOIN inventory_item i USING (inventory_id) WHERE r.rental_date < i.purchase_date),
           'Keep; inventory in-service date = LEAST(purchase_date, first rental)'
    UNION ALL
    SELECT 'rental', 'BUSINESS_RULE', 'overlapping rentals on the same physical item',
           (SELECT count(*) FROM rental a JOIN rental b
                 ON a.inventory_id = b.inventory_id AND a.rental_id < b.rental_id
                AND b.rental_date < COALESCE(a.return_date, DATE '9999-12-31')
                AND a.rental_date < COALESCE(b.return_date, DATE '9999-12-31')),
           'Keep rentals; snapshot is_rented is capped at 1 per item per day'
    -- ---------------- referential integrity (should all be 0) ----------------
    UNION ALL
    SELECT 'payment', 'REFERENTIAL', 'Rental payment without rental_id / subscription payment without subscription_id',
           (SELECT count(*) FROM payment
             WHERE (payment_type = 'Rental' AND rental_id IS NULL)
                OR (payment_type = 'Subscription' AND subscription_id IS NULL)),
           'Reject if ever > 0'
    UNION ALL
    SELECT 'audit columns', 'AUDIT', 'updated_at earlier than created_at (any table checked)',
           (SELECT count(*) FROM rental WHERE updated_at < created_at)
         + (SELECT count(*) FROM streaming_session WHERE updated_at < created_at)
         + (SELECT count(*) FROM payment WHERE updated_at < created_at),
           'None expected (package README guarantees it)'
)
SELECT table_name, category, rule, rows_affected, handling
FROM checks
ORDER BY category, table_name, rule;

\echo '=== 2. Evidence: negative payment amounts equal the expected charge once the sign is fixed ==='
SELECT p.payment_type,
       count(*)                                                            AS negative_payments,
       count(*) FILTER (WHERE p.payment_type = 'Subscription'
                          AND abs(p.amount) = sp.monthly_fee)               AS abs_matches_plan_fee,
       count(*) FILTER (WHERE p.payment_type = 'Rental'
                          AND abs(p.amount) = r.rental_fee + abs(r.late_fee)
                           OR abs(p.amount) = r.rental_fee)                 AS abs_matches_rental_charge
FROM payment p
LEFT JOIN customer_subscription cs ON cs.subscription_id = p.subscription_id
LEFT JOIN subscription_plan sp     ON sp.plan_id = cs.plan_id
LEFT JOIN rental r                 ON r.rental_id = p.rental_id
WHERE p.amount <= 0
GROUP BY p.payment_type;

\echo '=== 3. Evidence: negative late fees - late returns were charged ABS(late_fee) ==='
SELECT (r.return_date > r.due_date OR r.return_date IS NULL) AS returned_late_or_overdue,
       count(*)                                              AS negative_late_fees,
       count(*) FILTER (WHERE abs(p.amount) = r.rental_fee + abs(r.late_fee)) AS payment_includes_abs_fee,
       count(*) FILTER (WHERE abs(p.amount) = r.rental_fee)                   AS payment_excludes_fee
FROM rental r
LEFT JOIN payment p ON p.rental_id = r.rental_id AND p.payment_id = (
        SELECT min(payment_id) FROM payment WHERE rental_id = r.rental_id)
WHERE r.late_fee < 0
GROUP BY 1;

\echo '=== 4. Evidence: missing watch_duration can be derived exactly ==='
SELECT count(*) FILTER (WHERE watch_duration IS NOT NULL)                         AS populated_rows,
       count(*) FILTER (WHERE watch_duration IS NOT NULL
                          AND watch_duration = extract(epoch FROM end_time - start_time) / 60) AS populated_and_equal_to_end_minus_start
FROM streaming_session;

\echo '=== 5. Business-date coverage (drives dim_date range and snapshot window) ==='
SELECT 'streaming_session.start_time' AS business_date, min(start_time)::date AS min_date, max(start_time)::date AS max_date FROM streaming_session
UNION ALL SELECT 'rental.rental_date', min(rental_date), max(rental_date) FROM rental
UNION ALL SELECT 'rental.return_date', min(return_date), max(return_date) FROM rental
UNION ALL SELECT 'payment.payment_date', min(payment_date)::date, max(payment_date)::date FROM payment
UNION ALL SELECT 'customer.registration_date', min(registration_date), max(registration_date) FROM customer
UNION ALL SELECT 'inventory_item.purchase_date', min(purchase_date), max(purchase_date) FROM inventory_item;
