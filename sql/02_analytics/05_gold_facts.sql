-- ============================================================================
-- 05_gold_facts.sql                         (database: abc_hub_analytics)
--
-- The three pre-aggregated fact tables of Question 7.
--
-- The PRIMARY KEY of every fact is its grain. Loads delete-and-reinsert the
-- affected grain rows inside one transaction, so a repeated execution can
-- never produce a duplicate analytical record (Q8 Task 3).
--
-- Measure additivity is documented per column:
--   [A] additive       - safe to SUM across every dimension
--   [S] semi-additive  - SUM across items but not across time (snapshot flags)
--   [N] non-additive   - ratio/average: recompute from its additive parts
-- ============================================================================

-- ----------------------------------------------------------------------------
-- Scenario A - Customer Daily Activity
-- GRAIN: one row per customer per calendar day on which the customer did
--        anything (streamed, rented, returned, paid, raised a ticket,
--        reviewed or wish-listed). Days with no activity are not stored.
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS gold.fact_customer_daily_activity (
    date_key                    INT     NOT NULL REFERENCES gold.dim_date (date_key),
    customer_key                BIGINT  NOT NULL REFERENCES gold.dim_customer (customer_key),
    subscription_plan_key       INT     NOT NULL REFERENCES gold.dim_subscription_plan (subscription_plan_key),
    -- streaming
    streaming_session_count     INT           NOT NULL DEFAULT 0,  -- [A]
    streaming_minutes           INT           NOT NULL DEFAULT 0,  -- [A] total watch duration
    distinct_titles_streamed    INT           NOT NULL DEFAULT 0,  -- [N] distinct count
    completed_stream_count      INT           NOT NULL DEFAULT 0,  -- [A] completion >= 90 %
    -- physical rentals
    rental_count                INT           NOT NULL DEFAULT 0,  -- [A] rentals started
    returned_item_count         INT           NOT NULL DEFAULT 0,  -- [A] items returned
    late_return_count           INT           NOT NULL DEFAULT 0,  -- [A] returned after due date
    -- money (completed payments only; refunds shown separately)
    total_amount_spent          NUMERIC(12,2) NOT NULL DEFAULT 0,  -- [A]
    subscription_amount_spent   NUMERIC(12,2) NOT NULL DEFAULT 0,  -- [A]
    rental_amount_spent         NUMERIC(12,2) NOT NULL DEFAULT 0,  -- [A]
    refunded_amount             NUMERIC(12,2) NOT NULL DEFAULT 0,  -- [A]
    failed_payment_count        INT           NOT NULL DEFAULT 0,  -- [A]
    -- engagement & support
    support_ticket_count        INT           NOT NULL DEFAULT 0,  -- [A] tickets raised
    review_count                INT           NOT NULL DEFAULT 0,  -- [A]
    wishlist_add_count          INT           NOT NULL DEFAULT 0,  -- [A]
    -- lineage
    _run_id                     BIGINT,
    _loaded_at                  TIMESTAMP     NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT pk_fact_customer_daily_activity PRIMARY KEY (date_key, customer_key)
);
CREATE INDEX IF NOT EXISTS ix_fcda_customer ON gold.fact_customer_daily_activity (customer_key);
CREATE INDEX IF NOT EXISTS ix_fcda_plan     ON gold.fact_customer_daily_activity (subscription_plan_key);

-- ----------------------------------------------------------------------------
-- Scenario B - Content Monthly Performance
-- GRAIN: one row per content item per calendar month in which the title
--        was streamed, rented, paid for, reviewed or wish-listed.
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS gold.fact_content_monthly_performance (
    month_key                       INT     NOT NULL REFERENCES gold.dim_month (month_key),
    content_key                     BIGINT  NOT NULL REFERENCES gold.dim_content (content_key),
    -- streaming
    stream_count                    INT           NOT NULL DEFAULT 0,  -- [A]
    unique_viewer_count             INT           NOT NULL DEFAULT 0,  -- [N] distinct customers
    streaming_minutes               BIGINT        NOT NULL DEFAULT 0,  -- [A]
    completed_stream_count          INT           NOT NULL DEFAULT 0,  -- [A]
    avg_completion_pct              NUMERIC(5,2),                      -- [N]
    -- rentals
    rental_count                    INT           NOT NULL DEFAULT 0,  -- [A]
    unique_renter_count             INT           NOT NULL DEFAULT 0,  -- [N]
    -- revenue (see README revenue-attribution assumption)
    rental_revenue                  NUMERIC(12,2) NOT NULL DEFAULT 0,  -- [A] completed rental payments
    allocated_subscription_revenue  NUMERIC(12,2) NOT NULL DEFAULT 0,  -- [A] month's subscription revenue x share of watch minutes
    total_revenue                   NUMERIC(12,2) NOT NULL DEFAULT 0,  -- [A]
    -- ratings & demand
    review_count                    INT           NOT NULL DEFAULT 0,  -- [A]
    rated_review_count              INT           NOT NULL DEFAULT 0,  -- [A] reviews with a valid 1-5 rating
    rating_sum                      INT           NOT NULL DEFAULT 0,  -- [A] numerator of the average
    avg_customer_rating             NUMERIC(3,2),                      -- [N] rating_sum / rated_review_count
    wishlist_add_count              INT           NOT NULL DEFAULT 0,  -- [A]
    -- lineage
    _run_id                         BIGINT,
    _loaded_at                      TIMESTAMP     NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT pk_fact_content_monthly_performance PRIMARY KEY (month_key, content_key)
);
CREATE INDEX IF NOT EXISTS ix_fcmp_content ON gold.fact_content_monthly_performance (content_key);

-- ----------------------------------------------------------------------------
-- Scenario C - Inventory Utilisation (periodic snapshot)
-- GRAIN: one row per physical inventory item per calendar day, for every day
--        the item is in service within the snapshot window - dense, including
--        idle days, because "days available" must count days with no activity.
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS gold.fact_inventory_daily_snapshot (
    date_key                INT     NOT NULL REFERENCES gold.dim_date (date_key),
    inventory_key           BIGINT  NOT NULL REFERENCES gold.dim_inventory_item (inventory_key),
    content_key             BIGINT  NOT NULL REFERENCES gold.dim_content (content_key),
    warehouse_key           INT     NOT NULL REFERENCES gold.dim_warehouse (warehouse_key),
    rental_count            SMALLINT     NOT NULL DEFAULT 0,  -- [A] rentals starting this day
    return_count            SMALLINT     NOT NULL DEFAULT 0,  -- [A] returns this day
    days_in_service         SMALLINT     NOT NULL DEFAULT 1,  -- [A over time] always 1 per row
    days_rented             SMALLINT     NOT NULL DEFAULT 0,  -- [A over time] 1 if on loan
    days_available          SMALLINT     NOT NULL DEFAULT 0,  -- [A over time] 1 if on the shelf
    is_overdue              SMALLINT     NOT NULL DEFAULT 0,  -- [S] 1 if on loan past its due date
    utilisation_pct         NUMERIC(5,2) NOT NULL DEFAULT 0,  -- [N] 100 * days_rented / days_in_service
    _run_id                 BIGINT,
    _loaded_at              TIMESTAMP    NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT pk_fact_inventory_daily_snapshot PRIMARY KEY (date_key, inventory_key),
    CONSTRAINT ck_fids_flags CHECK (days_rented + days_available = days_in_service)
);
CREATE INDEX IF NOT EXISTS ix_fids_inventory ON gold.fact_inventory_daily_snapshot (inventory_key);
CREATE INDEX IF NOT EXISTS ix_fids_content   ON gold.fact_inventory_daily_snapshot (content_key);
CREATE INDEX IF NOT EXISTS ix_fids_warehouse ON gold.fact_inventory_daily_snapshot (warehouse_key);
