-- ============================================================================
-- 03_silver_tables.sql                      (database: abc_hub_analytics)
--
-- SILVER = cleansed, conformed, CURRENT-STATE entities.
--
--   * One row per source primary key. NiFi loads with UPSERT
--     (INSERT ... ON CONFLICT (pk) DO UPDATE), so re-running a batch
--     overwrites instead of duplicating - the load is idempotent.
--   * Values are already cleansed by the NiFi transform stage
--     (trimmed / title-cased text, sign errors fixed, capped percentages,
--     derived durations, invalid values nulled). dq_flags lists every
--     correction applied to the row, so nothing is changed silently.
--   * NOT NULL marks the fields that are mandatory for analytics; NiFi's
--     ValidateRecord rejects records missing them before they get here.
--   * is_duplicate / duplicate_of_id: set by etl.flag_silver_duplicates()
--     for duplicates that arrive in DIFFERENT NiFi batches (in-batch
--     duplicates are already removed by NiFi).
--   * _loaded_at is maintained by trigger and drives incremental gold loads.
--   * No foreign keys here: NiFi loads tables independently and in any
--     order. Referential integrity is enforced in gold (unknown members).
-- ============================================================================

-- grain: one row per country_id
CREATE TABLE IF NOT EXISTS silver.country (
    country_id      BIGINT NOT NULL,
    country_name    VARCHAR(100) NOT NULL,
    country_code    VARCHAR(5) NOT NULL,
    created_at      TIMESTAMP NOT NULL,
    updated_at      TIMESTAMP NOT NULL,
    dq_flags        VARCHAR(500),
    is_duplicate    BOOLEAN      NOT NULL DEFAULT FALSE,
    duplicate_of_id BIGINT,
    _batch_id       VARCHAR(64),
    _loaded_at      TIMESTAMP    NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT pk_silver_country PRIMARY KEY (country_id)
);
CREATE INDEX IF NOT EXISTS ix_silver_country_loaded_at ON silver.country (_loaded_at);
CREATE OR REPLACE TRIGGER trg_silver_country_loaded_at BEFORE INSERT OR UPDATE ON silver.country
    FOR EACH ROW EXECUTE FUNCTION etl.set_loaded_at();

-- grain: one row per city_id
CREATE TABLE IF NOT EXISTS silver.city (
    city_id         BIGINT NOT NULL,
    country_id      BIGINT NOT NULL,
    city_name       VARCHAR(100) NOT NULL,
    created_at      TIMESTAMP NOT NULL,
    updated_at      TIMESTAMP NOT NULL,
    dq_flags        VARCHAR(500),
    is_duplicate    BOOLEAN      NOT NULL DEFAULT FALSE,
    duplicate_of_id BIGINT,
    _batch_id       VARCHAR(64),
    _loaded_at      TIMESTAMP    NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT pk_silver_city PRIMARY KEY (city_id)
);
CREATE INDEX IF NOT EXISTS ix_silver_city_loaded_at ON silver.city (_loaded_at);
CREATE OR REPLACE TRIGGER trg_silver_city_loaded_at BEFORE INSERT OR UPDATE ON silver.city
    FOR EACH ROW EXECUTE FUNCTION etl.set_loaded_at();

-- grain: one row per customer_id  |  duplicate business key: (lower(trim(email)))
CREATE TABLE IF NOT EXISTS silver.customer (
    customer_id     BIGINT NOT NULL,
    customer_no     VARCHAR(20) NOT NULL,
    first_name      VARCHAR(100),
    last_name       VARCHAR(100),
    email           VARCHAR(150) NOT NULL,
    phone           VARCHAR(30),
    date_of_birth   DATE,
    gender          VARCHAR(20),
    registration_date DATE NOT NULL,
    status          VARCHAR(20) NOT NULL,
    created_at      TIMESTAMP NOT NULL,
    updated_at      TIMESTAMP NOT NULL,
    dq_flags        VARCHAR(500),
    is_duplicate    BOOLEAN      NOT NULL DEFAULT FALSE,
    duplicate_of_id BIGINT,
    _batch_id       VARCHAR(64),
    _loaded_at      TIMESTAMP    NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT pk_silver_customer PRIMARY KEY (customer_id)
);
CREATE INDEX IF NOT EXISTS ix_silver_customer_loaded_at ON silver.customer (_loaded_at);
CREATE OR REPLACE TRIGGER trg_silver_customer_loaded_at BEFORE INSERT OR UPDATE ON silver.customer
    FOR EACH ROW EXECUTE FUNCTION etl.set_loaded_at();

-- grain: one row per address_id
CREATE TABLE IF NOT EXISTS silver.customer_address (
    address_id      BIGINT NOT NULL,
    customer_id     BIGINT NOT NULL,
    city_id         BIGINT NOT NULL,
    address_line    VARCHAR(255),
    postal_code     VARCHAR(20),
    address_type    VARCHAR(20) NOT NULL,
    created_at      TIMESTAMP NOT NULL,
    updated_at      TIMESTAMP NOT NULL,
    dq_flags        VARCHAR(500),
    is_duplicate    BOOLEAN      NOT NULL DEFAULT FALSE,
    duplicate_of_id BIGINT,
    _batch_id       VARCHAR(64),
    _loaded_at      TIMESTAMP    NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT pk_silver_customer_address PRIMARY KEY (address_id)
);
CREATE INDEX IF NOT EXISTS ix_silver_customer_address_loaded_at ON silver.customer_address (_loaded_at);
CREATE OR REPLACE TRIGGER trg_silver_customer_address_loaded_at BEFORE INSERT OR UPDATE ON silver.customer_address
    FOR EACH ROW EXECUTE FUNCTION etl.set_loaded_at();

-- grain: one row per plan_id
CREATE TABLE IF NOT EXISTS silver.subscription_plan (
    plan_id         BIGINT NOT NULL,
    plan_name       VARCHAR(50) NOT NULL,
    monthly_fee     NUMERIC(10,2) NOT NULL,
    video_quality   VARCHAR(20),
    max_devices     INTEGER,
    created_at      TIMESTAMP NOT NULL,
    updated_at      TIMESTAMP NOT NULL,
    dq_flags        VARCHAR(500),
    is_duplicate    BOOLEAN      NOT NULL DEFAULT FALSE,
    duplicate_of_id BIGINT,
    _batch_id       VARCHAR(64),
    _loaded_at      TIMESTAMP    NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT pk_silver_subscription_plan PRIMARY KEY (plan_id)
);
CREATE INDEX IF NOT EXISTS ix_silver_subscription_plan_loaded_at ON silver.subscription_plan (_loaded_at);
CREATE OR REPLACE TRIGGER trg_silver_subscription_plan_loaded_at BEFORE INSERT OR UPDATE ON silver.subscription_plan
    FOR EACH ROW EXECUTE FUNCTION etl.set_loaded_at();

-- grain: one row per subscription_id
CREATE TABLE IF NOT EXISTS silver.customer_subscription (
    subscription_id BIGINT NOT NULL,
    customer_id     BIGINT NOT NULL,
    plan_id         BIGINT NOT NULL,
    start_date      DATE NOT NULL,
    end_date        DATE,
    status          VARCHAR(20) NOT NULL,
    auto_renew      BOOLEAN,
    created_at      TIMESTAMP NOT NULL,
    updated_at      TIMESTAMP NOT NULL,
    dq_flags        VARCHAR(500),
    is_duplicate    BOOLEAN      NOT NULL DEFAULT FALSE,
    duplicate_of_id BIGINT,
    _batch_id       VARCHAR(64),
    _loaded_at      TIMESTAMP    NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT pk_silver_customer_subscription PRIMARY KEY (subscription_id)
);
CREATE INDEX IF NOT EXISTS ix_silver_customer_subscription_loaded_at ON silver.customer_subscription (_loaded_at);
CREATE OR REPLACE TRIGGER trg_silver_customer_subscription_loaded_at BEFORE INSERT OR UPDATE ON silver.customer_subscription
    FOR EACH ROW EXECUTE FUNCTION etl.set_loaded_at();

-- grain: one row per content_type_id
CREATE TABLE IF NOT EXISTS silver.content_type (
    content_type_id BIGINT NOT NULL,
    content_type    VARCHAR(50) NOT NULL,
    created_at      TIMESTAMP NOT NULL,
    updated_at      TIMESTAMP NOT NULL,
    dq_flags        VARCHAR(500),
    is_duplicate    BOOLEAN      NOT NULL DEFAULT FALSE,
    duplicate_of_id BIGINT,
    _batch_id       VARCHAR(64),
    _loaded_at      TIMESTAMP    NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT pk_silver_content_type PRIMARY KEY (content_type_id)
);
CREATE INDEX IF NOT EXISTS ix_silver_content_type_loaded_at ON silver.content_type (_loaded_at);
CREATE OR REPLACE TRIGGER trg_silver_content_type_loaded_at BEFORE INSERT OR UPDATE ON silver.content_type
    FOR EACH ROW EXECUTE FUNCTION etl.set_loaded_at();

-- grain: one row per genre_id
CREATE TABLE IF NOT EXISTS silver.genre (
    genre_id        BIGINT NOT NULL,
    genre_name      VARCHAR(50) NOT NULL,
    created_at      TIMESTAMP NOT NULL,
    updated_at      TIMESTAMP NOT NULL,
    dq_flags        VARCHAR(500),
    is_duplicate    BOOLEAN      NOT NULL DEFAULT FALSE,
    duplicate_of_id BIGINT,
    _batch_id       VARCHAR(64),
    _loaded_at      TIMESTAMP    NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT pk_silver_genre PRIMARY KEY (genre_id)
);
CREATE INDEX IF NOT EXISTS ix_silver_genre_loaded_at ON silver.genre (_loaded_at);
CREATE OR REPLACE TRIGGER trg_silver_genre_loaded_at BEFORE INSERT OR UPDATE ON silver.genre
    FOR EACH ROW EXECUTE FUNCTION etl.set_loaded_at();

-- grain: one row per artist_id
CREATE TABLE IF NOT EXISTS silver.artist (
    artist_id       BIGINT NOT NULL,
    artist_name     VARCHAR(150) NOT NULL,
    country         VARCHAR(100),
    created_at      TIMESTAMP NOT NULL,
    updated_at      TIMESTAMP NOT NULL,
    dq_flags        VARCHAR(500),
    is_duplicate    BOOLEAN      NOT NULL DEFAULT FALSE,
    duplicate_of_id BIGINT,
    _batch_id       VARCHAR(64),
    _loaded_at      TIMESTAMP    NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT pk_silver_artist PRIMARY KEY (artist_id)
);
CREATE INDEX IF NOT EXISTS ix_silver_artist_loaded_at ON silver.artist (_loaded_at);
CREATE OR REPLACE TRIGGER trg_silver_artist_loaded_at BEFORE INSERT OR UPDATE ON silver.artist
    FOR EACH ROW EXECUTE FUNCTION etl.set_loaded_at();

-- grain: one row per content_id
CREATE TABLE IF NOT EXISTS silver.content (
    content_id      BIGINT NOT NULL,
    content_type_id BIGINT NOT NULL,
    title           VARCHAR(255) NOT NULL,
    release_date    DATE,
    duration_minutes INTEGER,
    language        VARCHAR(50),
    age_rating      VARCHAR(10),
    created_at      TIMESTAMP NOT NULL,
    updated_at      TIMESTAMP NOT NULL,
    dq_flags        VARCHAR(500),
    is_duplicate    BOOLEAN      NOT NULL DEFAULT FALSE,
    duplicate_of_id BIGINT,
    _batch_id       VARCHAR(64),
    _loaded_at      TIMESTAMP    NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT pk_silver_content PRIMARY KEY (content_id)
);
CREATE INDEX IF NOT EXISTS ix_silver_content_loaded_at ON silver.content (_loaded_at);
CREATE OR REPLACE TRIGGER trg_silver_content_loaded_at BEFORE INSERT OR UPDATE ON silver.content
    FOR EACH ROW EXECUTE FUNCTION etl.set_loaded_at();

-- grain: one row per content_id, genre_id
CREATE TABLE IF NOT EXISTS silver.content_genre (
    content_id      BIGINT NOT NULL,
    genre_id        BIGINT NOT NULL,
    created_at      TIMESTAMP NOT NULL,
    updated_at      TIMESTAMP NOT NULL,
    dq_flags        VARCHAR(500),
    is_duplicate    BOOLEAN      NOT NULL DEFAULT FALSE,
    duplicate_of_id BIGINT,
    _batch_id       VARCHAR(64),
    _loaded_at      TIMESTAMP    NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT pk_silver_content_genre PRIMARY KEY (content_id, genre_id)
);
CREATE INDEX IF NOT EXISTS ix_silver_content_genre_loaded_at ON silver.content_genre (_loaded_at);
CREATE OR REPLACE TRIGGER trg_silver_content_genre_loaded_at BEFORE INSERT OR UPDATE ON silver.content_genre
    FOR EACH ROW EXECUTE FUNCTION etl.set_loaded_at();

-- grain: one row per content_id, artist_id
CREATE TABLE IF NOT EXISTS silver.content_artist (
    content_id      BIGINT NOT NULL,
    artist_id       BIGINT NOT NULL,
    role            VARCHAR(50),
    created_at      TIMESTAMP NOT NULL,
    updated_at      TIMESTAMP NOT NULL,
    dq_flags        VARCHAR(500),
    is_duplicate    BOOLEAN      NOT NULL DEFAULT FALSE,
    duplicate_of_id BIGINT,
    _batch_id       VARCHAR(64),
    _loaded_at      TIMESTAMP    NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT pk_silver_content_artist PRIMARY KEY (content_id, artist_id)
);
CREATE INDEX IF NOT EXISTS ix_silver_content_artist_loaded_at ON silver.content_artist (_loaded_at);
CREATE OR REPLACE TRIGGER trg_silver_content_artist_loaded_at BEFORE INSERT OR UPDATE ON silver.content_artist
    FOR EACH ROW EXECUTE FUNCTION etl.set_loaded_at();

-- grain: one row per warehouse_id
CREATE TABLE IF NOT EXISTS silver.warehouse (
    warehouse_id    BIGINT NOT NULL,
    city_id         BIGINT NOT NULL,
    warehouse_name  VARCHAR(150) NOT NULL,
    created_at      TIMESTAMP NOT NULL,
    updated_at      TIMESTAMP NOT NULL,
    dq_flags        VARCHAR(500),
    is_duplicate    BOOLEAN      NOT NULL DEFAULT FALSE,
    duplicate_of_id BIGINT,
    _batch_id       VARCHAR(64),
    _loaded_at      TIMESTAMP    NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT pk_silver_warehouse PRIMARY KEY (warehouse_id)
);
CREATE INDEX IF NOT EXISTS ix_silver_warehouse_loaded_at ON silver.warehouse (_loaded_at);
CREATE OR REPLACE TRIGGER trg_silver_warehouse_loaded_at BEFORE INSERT OR UPDATE ON silver.warehouse
    FOR EACH ROW EXECUTE FUNCTION etl.set_loaded_at();

-- grain: one row per inventory_id
CREATE TABLE IF NOT EXISTS silver.inventory_item (
    inventory_id    BIGINT NOT NULL,
    content_id      BIGINT NOT NULL,
    warehouse_id    BIGINT NOT NULL,
    barcode         VARCHAR(50),
    purchase_date   DATE,
    item_condition  VARCHAR(20),
    status          VARCHAR(20) NOT NULL,
    created_at      TIMESTAMP NOT NULL,
    updated_at      TIMESTAMP NOT NULL,
    dq_flags        VARCHAR(500),
    is_duplicate    BOOLEAN      NOT NULL DEFAULT FALSE,
    duplicate_of_id BIGINT,
    _batch_id       VARCHAR(64),
    _loaded_at      TIMESTAMP    NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT pk_silver_inventory_item PRIMARY KEY (inventory_id)
);
CREATE INDEX IF NOT EXISTS ix_silver_inventory_item_loaded_at ON silver.inventory_item (_loaded_at);
CREATE OR REPLACE TRIGGER trg_silver_inventory_item_loaded_at BEFORE INSERT OR UPDATE ON silver.inventory_item
    FOR EACH ROW EXECUTE FUNCTION etl.set_loaded_at();

-- grain: one row per stream_id  |  duplicate business key: (customer_id, content_id, device_id, start_time)
CREATE TABLE IF NOT EXISTS silver.streaming_session (
    stream_id       BIGINT NOT NULL,
    customer_id     BIGINT NOT NULL,
    content_id      BIGINT NOT NULL,
    device_id       BIGINT NOT NULL,
    start_time      TIMESTAMP NOT NULL,
    end_time        TIMESTAMP NOT NULL,
    watch_duration  INTEGER,
    completion_percentage NUMERIC(5,2),
    created_at      TIMESTAMP NOT NULL,
    updated_at      TIMESTAMP NOT NULL,
    dq_flags        VARCHAR(500),
    is_duplicate    BOOLEAN      NOT NULL DEFAULT FALSE,
    duplicate_of_id BIGINT,
    _batch_id       VARCHAR(64),
    _loaded_at      TIMESTAMP    NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT pk_silver_streaming_session PRIMARY KEY (stream_id)
);
CREATE INDEX IF NOT EXISTS ix_silver_streaming_session_loaded_at ON silver.streaming_session (_loaded_at);
CREATE OR REPLACE TRIGGER trg_silver_streaming_session_loaded_at BEFORE INSERT OR UPDATE ON silver.streaming_session
    FOR EACH ROW EXECUTE FUNCTION etl.set_loaded_at();

-- grain: one row per rental_id
CREATE TABLE IF NOT EXISTS silver.rental (
    rental_id       BIGINT NOT NULL,
    customer_id     BIGINT NOT NULL,
    inventory_id    BIGINT NOT NULL,
    rental_date     DATE NOT NULL,
    due_date        DATE NOT NULL,
    return_date     DATE,
    rental_fee      NUMERIC(10,2) NOT NULL,
    late_fee        NUMERIC(10,2),
    status          VARCHAR(20) NOT NULL,
    created_at      TIMESTAMP NOT NULL,
    updated_at      TIMESTAMP NOT NULL,
    dq_flags        VARCHAR(500),
    is_duplicate    BOOLEAN      NOT NULL DEFAULT FALSE,
    duplicate_of_id BIGINT,
    _batch_id       VARCHAR(64),
    _loaded_at      TIMESTAMP    NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT pk_silver_rental PRIMARY KEY (rental_id)
);
CREATE INDEX IF NOT EXISTS ix_silver_rental_loaded_at ON silver.rental (_loaded_at);
CREATE OR REPLACE TRIGGER trg_silver_rental_loaded_at BEFORE INSERT OR UPDATE ON silver.rental
    FOR EACH ROW EXECUTE FUNCTION etl.set_loaded_at();

-- grain: one row per payment_id  |  duplicate business key: (customer_id, payment_type, amount, payment_date, rental_id, subscription_id)
CREATE TABLE IF NOT EXISTS silver.payment (
    payment_id      BIGINT NOT NULL,
    customer_id     BIGINT NOT NULL,
    rental_id       BIGINT,
    subscription_id BIGINT,
    payment_method_id BIGINT NOT NULL,
    amount          NUMERIC(10,2) NOT NULL,
    payment_date    TIMESTAMP NOT NULL,
    payment_type    VARCHAR(20) NOT NULL,
    status          VARCHAR(20) NOT NULL,
    created_at      TIMESTAMP NOT NULL,
    updated_at      TIMESTAMP NOT NULL,
    dq_flags        VARCHAR(500),
    is_duplicate    BOOLEAN      NOT NULL DEFAULT FALSE,
    duplicate_of_id BIGINT,
    _batch_id       VARCHAR(64),
    _loaded_at      TIMESTAMP    NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT pk_silver_payment PRIMARY KEY (payment_id)
);
CREATE INDEX IF NOT EXISTS ix_silver_payment_loaded_at ON silver.payment (_loaded_at);
CREATE OR REPLACE TRIGGER trg_silver_payment_loaded_at BEFORE INSERT OR UPDATE ON silver.payment
    FOR EACH ROW EXECUTE FUNCTION etl.set_loaded_at();

-- grain: one row per review_id
CREATE TABLE IF NOT EXISTS silver.review (
    review_id       BIGINT NOT NULL,
    customer_id     BIGINT NOT NULL,
    content_id      BIGINT NOT NULL,
    rating          INTEGER,
    review_text     VARCHAR(500),
    review_date     TIMESTAMP NOT NULL,
    created_at      TIMESTAMP NOT NULL,
    updated_at      TIMESTAMP NOT NULL,
    dq_flags        VARCHAR(500),
    is_duplicate    BOOLEAN      NOT NULL DEFAULT FALSE,
    duplicate_of_id BIGINT,
    _batch_id       VARCHAR(64),
    _loaded_at      TIMESTAMP    NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT pk_silver_review PRIMARY KEY (review_id)
);
CREATE INDEX IF NOT EXISTS ix_silver_review_loaded_at ON silver.review (_loaded_at);
CREATE OR REPLACE TRIGGER trg_silver_review_loaded_at BEFORE INSERT OR UPDATE ON silver.review
    FOR EACH ROW EXECUTE FUNCTION etl.set_loaded_at();

-- grain: one row per wishlist_id
CREATE TABLE IF NOT EXISTS silver.wishlist (
    wishlist_id     BIGINT NOT NULL,
    customer_id     BIGINT NOT NULL,
    content_id      BIGINT NOT NULL,
    added_date      TIMESTAMP NOT NULL,
    created_at      TIMESTAMP NOT NULL,
    updated_at      TIMESTAMP NOT NULL,
    dq_flags        VARCHAR(500),
    is_duplicate    BOOLEAN      NOT NULL DEFAULT FALSE,
    duplicate_of_id BIGINT,
    _batch_id       VARCHAR(64),
    _loaded_at      TIMESTAMP    NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT pk_silver_wishlist PRIMARY KEY (wishlist_id)
);
CREATE INDEX IF NOT EXISTS ix_silver_wishlist_loaded_at ON silver.wishlist (_loaded_at);
CREATE OR REPLACE TRIGGER trg_silver_wishlist_loaded_at BEFORE INSERT OR UPDATE ON silver.wishlist
    FOR EACH ROW EXECUTE FUNCTION etl.set_loaded_at();

-- grain: one row per ticket_id
CREATE TABLE IF NOT EXISTS silver.support_ticket (
    ticket_id       BIGINT NOT NULL,
    customer_id     BIGINT NOT NULL,
    category_id     BIGINT NOT NULL,
    opened_date     TIMESTAMP NOT NULL,
    closed_date     TIMESTAMP,
    priority        VARCHAR(20),
    status          VARCHAR(20) NOT NULL,
    created_at      TIMESTAMP NOT NULL,
    updated_at      TIMESTAMP NOT NULL,
    dq_flags        VARCHAR(500),
    is_duplicate    BOOLEAN      NOT NULL DEFAULT FALSE,
    duplicate_of_id BIGINT,
    _batch_id       VARCHAR(64),
    _loaded_at      TIMESTAMP    NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT pk_silver_support_ticket PRIMARY KEY (ticket_id)
);
CREATE INDEX IF NOT EXISTS ix_silver_support_ticket_loaded_at ON silver.support_ticket (_loaded_at);
CREATE OR REPLACE TRIGGER trg_silver_support_ticket_loaded_at BEFORE INSERT OR UPDATE ON silver.support_ticket
    FOR EACH ROW EXECUTE FUNCTION etl.set_loaded_at();


-- ----------------------------------------------------------------------------
-- Join / lookup indexes used by the silver -> gold loads (foreign-key columns
-- and business dates). Without them the gold facts fall back to nested scans.
-- ----------------------------------------------------------------------------
CREATE INDEX IF NOT EXISTS ix_silver_customer_address_customer ON silver.customer_address (customer_id);
CREATE INDEX IF NOT EXISTS ix_silver_subscription_customer     ON silver.customer_subscription (customer_id, start_date);
CREATE INDEX IF NOT EXISTS ix_silver_streaming_customer        ON silver.streaming_session (customer_id);
CREATE INDEX IF NOT EXISTS ix_silver_streaming_start           ON silver.streaming_session (start_time);
CREATE INDEX IF NOT EXISTS ix_silver_rental_customer           ON silver.rental (customer_id);
CREATE INDEX IF NOT EXISTS ix_silver_rental_inventory          ON silver.rental (inventory_id);
CREATE INDEX IF NOT EXISTS ix_silver_payment_customer          ON silver.payment (customer_id);
CREATE INDEX IF NOT EXISTS ix_silver_payment_rental            ON silver.payment (rental_id);
CREATE INDEX IF NOT EXISTS ix_silver_payment_date              ON silver.payment (payment_date);
CREATE INDEX IF NOT EXISTS ix_silver_review_customer           ON silver.review (customer_id);
CREATE INDEX IF NOT EXISTS ix_silver_wishlist_customer         ON silver.wishlist (customer_id);
CREATE INDEX IF NOT EXISTS ix_silver_ticket_customer           ON silver.support_ticket (customer_id);
CREATE INDEX IF NOT EXISTS ix_silver_inventory_content         ON silver.inventory_item (content_id);
