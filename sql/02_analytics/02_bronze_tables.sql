-- ============================================================================
-- 02_bronze_tables.sql                      (database: abc_hub_analytics)
--
-- BRONZE = raw landing zone. One table per extracted operational table.
--
--   * Columns and types mirror the source exactly; values are NOT cleansed.
--     Every column is nullable so bad source rows still land and stay
--     auditable (validation happens later, in the NiFi transform stage).
--   * Insert-only history: each extracted row VERSION is kept. The primary
--     key is the version key (source primary key, updated_at); NiFi loads
--     with INSERT_IGNORE, which PostgreSQL executes as
--     INSERT ... ON CONFLICT (<primary key>) DO NOTHING - so replaying a
--     batch never creates duplicates and the load is idempotent.
--   * _bronze_id is a surrogate row number (arrival order) for auditing.
--   * Lineage columns: _batch_id (NiFi batch), _record_source, _loaded_at.
--
-- 20 of the 26 operational tables are extracted - the ones the Question 7
-- models need. device, payment_method, support_category, courier, delivery
-- and recommendation are out of scope for these marts (see README).
--
-- Generated from the operational catalog (information_schema), then reviewed.
-- ============================================================================

-- source: abc_hub_operational.public.country  |  version key: (country_id, updated_at)
CREATE TABLE IF NOT EXISTS bronze.country (
    _bronze_id      BIGINT GENERATED ALWAYS AS IDENTITY UNIQUE,
    country_id      BIGINT,
    country_name    VARCHAR(100),
    country_code    VARCHAR(5),
    created_at      TIMESTAMP,
    updated_at      TIMESTAMP,
    _batch_id       VARCHAR(64),
    _record_source  VARCHAR(100) NOT NULL DEFAULT 'abc_hub_operational.public.country',
    _loaded_at      TIMESTAMP    NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT pk_bronze_country PRIMARY KEY (country_id, updated_at)
);

-- source: abc_hub_operational.public.city  |  version key: (city_id, updated_at)
CREATE TABLE IF NOT EXISTS bronze.city (
    _bronze_id      BIGINT GENERATED ALWAYS AS IDENTITY UNIQUE,
    city_id         BIGINT,
    country_id      BIGINT,
    city_name       VARCHAR(100),
    created_at      TIMESTAMP,
    updated_at      TIMESTAMP,
    _batch_id       VARCHAR(64),
    _record_source  VARCHAR(100) NOT NULL DEFAULT 'abc_hub_operational.public.city',
    _loaded_at      TIMESTAMP    NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT pk_bronze_city PRIMARY KEY (city_id, updated_at)
);

-- source: abc_hub_operational.public.customer  |  version key: (customer_id, updated_at)
CREATE TABLE IF NOT EXISTS bronze.customer (
    _bronze_id      BIGINT GENERATED ALWAYS AS IDENTITY UNIQUE,
    customer_id     BIGINT,
    customer_no     VARCHAR(20),
    first_name      VARCHAR(100),
    last_name       VARCHAR(100),
    email           VARCHAR(150),
    phone           VARCHAR(30),
    date_of_birth   DATE,
    gender          VARCHAR(20),
    registration_date DATE,
    status          VARCHAR(20),
    created_at      TIMESTAMP,
    updated_at      TIMESTAMP,
    _batch_id       VARCHAR(64),
    _record_source  VARCHAR(100) NOT NULL DEFAULT 'abc_hub_operational.public.customer',
    _loaded_at      TIMESTAMP    NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT pk_bronze_customer PRIMARY KEY (customer_id, updated_at)
);

-- source: abc_hub_operational.public.customer_address  |  version key: (address_id, updated_at)
CREATE TABLE IF NOT EXISTS bronze.customer_address (
    _bronze_id      BIGINT GENERATED ALWAYS AS IDENTITY UNIQUE,
    address_id      BIGINT,
    customer_id     BIGINT,
    city_id         BIGINT,
    address_line    VARCHAR(255),
    postal_code     VARCHAR(20),
    address_type    VARCHAR(20),
    created_at      TIMESTAMP,
    updated_at      TIMESTAMP,
    _batch_id       VARCHAR(64),
    _record_source  VARCHAR(100) NOT NULL DEFAULT 'abc_hub_operational.public.customer_address',
    _loaded_at      TIMESTAMP    NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT pk_bronze_customer_address PRIMARY KEY (address_id, updated_at)
);

-- source: abc_hub_operational.public.subscription_plan  |  version key: (plan_id, updated_at)
CREATE TABLE IF NOT EXISTS bronze.subscription_plan (
    _bronze_id      BIGINT GENERATED ALWAYS AS IDENTITY UNIQUE,
    plan_id         BIGINT,
    plan_name       VARCHAR(50),
    monthly_fee     NUMERIC(10,2),
    video_quality   VARCHAR(20),
    max_devices     INTEGER,
    created_at      TIMESTAMP,
    updated_at      TIMESTAMP,
    _batch_id       VARCHAR(64),
    _record_source  VARCHAR(100) NOT NULL DEFAULT 'abc_hub_operational.public.subscription_plan',
    _loaded_at      TIMESTAMP    NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT pk_bronze_subscription_plan PRIMARY KEY (plan_id, updated_at)
);

-- source: abc_hub_operational.public.customer_subscription  |  version key: (subscription_id, updated_at)
CREATE TABLE IF NOT EXISTS bronze.customer_subscription (
    _bronze_id      BIGINT GENERATED ALWAYS AS IDENTITY UNIQUE,
    subscription_id BIGINT,
    customer_id     BIGINT,
    plan_id         BIGINT,
    start_date      DATE,
    end_date        DATE,
    status          VARCHAR(20),
    auto_renew      BOOLEAN,
    created_at      TIMESTAMP,
    updated_at      TIMESTAMP,
    _batch_id       VARCHAR(64),
    _record_source  VARCHAR(100) NOT NULL DEFAULT 'abc_hub_operational.public.customer_subscription',
    _loaded_at      TIMESTAMP    NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT pk_bronze_customer_subscription PRIMARY KEY (subscription_id, updated_at)
);

-- source: abc_hub_operational.public.content_type  |  version key: (content_type_id, updated_at)
CREATE TABLE IF NOT EXISTS bronze.content_type (
    _bronze_id      BIGINT GENERATED ALWAYS AS IDENTITY UNIQUE,
    content_type_id BIGINT,
    content_type    VARCHAR(50),
    created_at      TIMESTAMP,
    updated_at      TIMESTAMP,
    _batch_id       VARCHAR(64),
    _record_source  VARCHAR(100) NOT NULL DEFAULT 'abc_hub_operational.public.content_type',
    _loaded_at      TIMESTAMP    NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT pk_bronze_content_type PRIMARY KEY (content_type_id, updated_at)
);

-- source: abc_hub_operational.public.genre  |  version key: (genre_id, updated_at)
CREATE TABLE IF NOT EXISTS bronze.genre (
    _bronze_id      BIGINT GENERATED ALWAYS AS IDENTITY UNIQUE,
    genre_id        BIGINT,
    genre_name      VARCHAR(50),
    created_at      TIMESTAMP,
    updated_at      TIMESTAMP,
    _batch_id       VARCHAR(64),
    _record_source  VARCHAR(100) NOT NULL DEFAULT 'abc_hub_operational.public.genre',
    _loaded_at      TIMESTAMP    NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT pk_bronze_genre PRIMARY KEY (genre_id, updated_at)
);

-- source: abc_hub_operational.public.artist  |  version key: (artist_id, updated_at)
CREATE TABLE IF NOT EXISTS bronze.artist (
    _bronze_id      BIGINT GENERATED ALWAYS AS IDENTITY UNIQUE,
    artist_id       BIGINT,
    artist_name     VARCHAR(150),
    country         VARCHAR(100),
    created_at      TIMESTAMP,
    updated_at      TIMESTAMP,
    _batch_id       VARCHAR(64),
    _record_source  VARCHAR(100) NOT NULL DEFAULT 'abc_hub_operational.public.artist',
    _loaded_at      TIMESTAMP    NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT pk_bronze_artist PRIMARY KEY (artist_id, updated_at)
);

-- source: abc_hub_operational.public.content  |  version key: (content_id, updated_at)
CREATE TABLE IF NOT EXISTS bronze.content (
    _bronze_id      BIGINT GENERATED ALWAYS AS IDENTITY UNIQUE,
    content_id      BIGINT,
    content_type_id BIGINT,
    title           VARCHAR(255),
    release_date    DATE,
    duration_minutes INTEGER,
    language        VARCHAR(50),
    age_rating      VARCHAR(10),
    created_at      TIMESTAMP,
    updated_at      TIMESTAMP,
    _batch_id       VARCHAR(64),
    _record_source  VARCHAR(100) NOT NULL DEFAULT 'abc_hub_operational.public.content',
    _loaded_at      TIMESTAMP    NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT pk_bronze_content PRIMARY KEY (content_id, updated_at)
);

-- source: abc_hub_operational.public.content_genre  |  version key: (content_id, genre_id, updated_at)
CREATE TABLE IF NOT EXISTS bronze.content_genre (
    _bronze_id      BIGINT GENERATED ALWAYS AS IDENTITY UNIQUE,
    content_id      BIGINT,
    genre_id        BIGINT,
    created_at      TIMESTAMP,
    updated_at      TIMESTAMP,
    _batch_id       VARCHAR(64),
    _record_source  VARCHAR(100) NOT NULL DEFAULT 'abc_hub_operational.public.content_genre',
    _loaded_at      TIMESTAMP    NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT pk_bronze_content_genre PRIMARY KEY (content_id, genre_id, updated_at)
);

-- source: abc_hub_operational.public.content_artist  |  version key: (content_id, artist_id, updated_at)
CREATE TABLE IF NOT EXISTS bronze.content_artist (
    _bronze_id      BIGINT GENERATED ALWAYS AS IDENTITY UNIQUE,
    content_id      BIGINT,
    artist_id       BIGINT,
    role            VARCHAR(50),
    created_at      TIMESTAMP,
    updated_at      TIMESTAMP,
    _batch_id       VARCHAR(64),
    _record_source  VARCHAR(100) NOT NULL DEFAULT 'abc_hub_operational.public.content_artist',
    _loaded_at      TIMESTAMP    NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT pk_bronze_content_artist PRIMARY KEY (content_id, artist_id, updated_at)
);

-- source: abc_hub_operational.public.warehouse  |  version key: (warehouse_id, updated_at)
CREATE TABLE IF NOT EXISTS bronze.warehouse (
    _bronze_id      BIGINT GENERATED ALWAYS AS IDENTITY UNIQUE,
    warehouse_id    BIGINT,
    city_id         BIGINT,
    warehouse_name  VARCHAR(150),
    created_at      TIMESTAMP,
    updated_at      TIMESTAMP,
    _batch_id       VARCHAR(64),
    _record_source  VARCHAR(100) NOT NULL DEFAULT 'abc_hub_operational.public.warehouse',
    _loaded_at      TIMESTAMP    NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT pk_bronze_warehouse PRIMARY KEY (warehouse_id, updated_at)
);

-- source: abc_hub_operational.public.inventory_item  |  version key: (inventory_id, updated_at)
CREATE TABLE IF NOT EXISTS bronze.inventory_item (
    _bronze_id      BIGINT GENERATED ALWAYS AS IDENTITY UNIQUE,
    inventory_id    BIGINT,
    content_id      BIGINT,
    warehouse_id    BIGINT,
    barcode         VARCHAR(50),
    purchase_date   DATE,
    item_condition  VARCHAR(20),
    status          VARCHAR(20),
    created_at      TIMESTAMP,
    updated_at      TIMESTAMP,
    _batch_id       VARCHAR(64),
    _record_source  VARCHAR(100) NOT NULL DEFAULT 'abc_hub_operational.public.inventory_item',
    _loaded_at      TIMESTAMP    NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT pk_bronze_inventory_item PRIMARY KEY (inventory_id, updated_at)
);

-- source: abc_hub_operational.public.streaming_session  |  version key: (stream_id, updated_at)
CREATE TABLE IF NOT EXISTS bronze.streaming_session (
    _bronze_id      BIGINT GENERATED ALWAYS AS IDENTITY UNIQUE,
    stream_id       BIGINT,
    customer_id     BIGINT,
    content_id      BIGINT,
    device_id       BIGINT,
    start_time      TIMESTAMP,
    end_time        TIMESTAMP,
    watch_duration  INTEGER,
    completion_percentage NUMERIC(5,2),
    created_at      TIMESTAMP,
    updated_at      TIMESTAMP,
    _batch_id       VARCHAR(64),
    _record_source  VARCHAR(100) NOT NULL DEFAULT 'abc_hub_operational.public.streaming_session',
    _loaded_at      TIMESTAMP    NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT pk_bronze_streaming_session PRIMARY KEY (stream_id, updated_at)
);

-- source: abc_hub_operational.public.rental  |  version key: (rental_id, updated_at)
CREATE TABLE IF NOT EXISTS bronze.rental (
    _bronze_id      BIGINT GENERATED ALWAYS AS IDENTITY UNIQUE,
    rental_id       BIGINT,
    customer_id     BIGINT,
    inventory_id    BIGINT,
    rental_date     DATE,
    due_date        DATE,
    return_date     DATE,
    rental_fee      NUMERIC(10,2),
    late_fee        NUMERIC(10,2),
    status          VARCHAR(20),
    created_at      TIMESTAMP,
    updated_at      TIMESTAMP,
    _batch_id       VARCHAR(64),
    _record_source  VARCHAR(100) NOT NULL DEFAULT 'abc_hub_operational.public.rental',
    _loaded_at      TIMESTAMP    NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT pk_bronze_rental PRIMARY KEY (rental_id, updated_at)
);

-- source: abc_hub_operational.public.payment  |  version key: (payment_id, updated_at)
CREATE TABLE IF NOT EXISTS bronze.payment (
    _bronze_id      BIGINT GENERATED ALWAYS AS IDENTITY UNIQUE,
    payment_id      BIGINT,
    customer_id     BIGINT,
    rental_id       BIGINT,
    subscription_id BIGINT,
    payment_method_id BIGINT,
    amount          NUMERIC(10,2),
    payment_date    TIMESTAMP,
    payment_type    VARCHAR(20),
    status          VARCHAR(20),
    created_at      TIMESTAMP,
    updated_at      TIMESTAMP,
    _batch_id       VARCHAR(64),
    _record_source  VARCHAR(100) NOT NULL DEFAULT 'abc_hub_operational.public.payment',
    _loaded_at      TIMESTAMP    NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT pk_bronze_payment PRIMARY KEY (payment_id, updated_at)
);

-- source: abc_hub_operational.public.review  |  version key: (review_id, updated_at)
CREATE TABLE IF NOT EXISTS bronze.review (
    _bronze_id      BIGINT GENERATED ALWAYS AS IDENTITY UNIQUE,
    review_id       BIGINT,
    customer_id     BIGINT,
    content_id      BIGINT,
    rating          INTEGER,
    review_text     VARCHAR(500),
    review_date     TIMESTAMP,
    created_at      TIMESTAMP,
    updated_at      TIMESTAMP,
    _batch_id       VARCHAR(64),
    _record_source  VARCHAR(100) NOT NULL DEFAULT 'abc_hub_operational.public.review',
    _loaded_at      TIMESTAMP    NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT pk_bronze_review PRIMARY KEY (review_id, updated_at)
);

-- source: abc_hub_operational.public.wishlist  |  version key: (wishlist_id, updated_at)
CREATE TABLE IF NOT EXISTS bronze.wishlist (
    _bronze_id      BIGINT GENERATED ALWAYS AS IDENTITY UNIQUE,
    wishlist_id     BIGINT,
    customer_id     BIGINT,
    content_id      BIGINT,
    added_date      TIMESTAMP,
    created_at      TIMESTAMP,
    updated_at      TIMESTAMP,
    _batch_id       VARCHAR(64),
    _record_source  VARCHAR(100) NOT NULL DEFAULT 'abc_hub_operational.public.wishlist',
    _loaded_at      TIMESTAMP    NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT pk_bronze_wishlist PRIMARY KEY (wishlist_id, updated_at)
);

-- source: abc_hub_operational.public.support_ticket  |  version key: (ticket_id, updated_at)
CREATE TABLE IF NOT EXISTS bronze.support_ticket (
    _bronze_id      BIGINT GENERATED ALWAYS AS IDENTITY UNIQUE,
    ticket_id       BIGINT,
    customer_id     BIGINT,
    category_id     BIGINT,
    opened_date     TIMESTAMP,
    closed_date     TIMESTAMP,
    priority        VARCHAR(20),
    status          VARCHAR(20),
    created_at      TIMESTAMP,
    updated_at      TIMESTAMP,
    _batch_id       VARCHAR(64),
    _record_source  VARCHAR(100) NOT NULL DEFAULT 'abc_hub_operational.public.support_ticket',
    _loaded_at      TIMESTAMP    NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT pk_bronze_support_ticket PRIMARY KEY (ticket_id, updated_at)
);

