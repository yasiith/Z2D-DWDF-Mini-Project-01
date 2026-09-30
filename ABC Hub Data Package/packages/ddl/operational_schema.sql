-- ============================================================================
-- ABC Hub Operational Database
-- Mini Project 01 - Zuu Crew Machine Learning Academy
-- Schema generated to match the provided ER diagram exactly.
-- ============================================================================

-- Run this script against an empty database, e.g.:
--   createdb abc_hub_operational
--   psql -d abc_hub_operational -f operational_schema.sql

DROP SCHEMA IF EXISTS public CASCADE;
CREATE SCHEMA public;

-- ----------------------------------------------------------------------------
-- Lookup / reference tables
-- ----------------------------------------------------------------------------

CREATE TABLE country (
    country_id      BIGINT PRIMARY KEY,
    country_name    VARCHAR(100) NOT NULL,
    country_code    VARCHAR(5)   NOT NULL,
    created_at      TIMESTAMP NOT NULL,
    updated_at      TIMESTAMP NOT NULL
);

CREATE TABLE city (
    city_id         BIGINT PRIMARY KEY,
    country_id      BIGINT NOT NULL REFERENCES country(country_id),
    city_name       VARCHAR(100) NOT NULL,
    created_at      TIMESTAMP NOT NULL,
    updated_at      TIMESTAMP NOT NULL
);

CREATE TABLE content_type (
    content_type_id BIGINT PRIMARY KEY,
    content_type    VARCHAR(50) NOT NULL,
    created_at      TIMESTAMP NOT NULL,
    updated_at      TIMESTAMP NOT NULL
);

CREATE TABLE genre (
    genre_id        BIGINT PRIMARY KEY,
    genre_name      VARCHAR(50) NOT NULL,
    created_at      TIMESTAMP NOT NULL,
    updated_at      TIMESTAMP NOT NULL
);

CREATE TABLE artist (
    artist_id       BIGINT PRIMARY KEY,
    artist_name     VARCHAR(150) NOT NULL,
    country         VARCHAR(100),
    created_at      TIMESTAMP NOT NULL,
    updated_at      TIMESTAMP NOT NULL
);

CREATE TABLE device (
    device_id       BIGINT PRIMARY KEY,
    device_name     VARCHAR(50) NOT NULL,
    created_at      TIMESTAMP NOT NULL,
    updated_at      TIMESTAMP NOT NULL
);

CREATE TABLE payment_method (
    payment_method_id BIGINT PRIMARY KEY,
    method_name       VARCHAR(50) NOT NULL,
    created_at      TIMESTAMP NOT NULL,
    updated_at      TIMESTAMP NOT NULL
);

CREATE TABLE support_category (
    category_id     BIGINT PRIMARY KEY,
    category_name   VARCHAR(100) NOT NULL,
    created_at      TIMESTAMP NOT NULL,
    updated_at      TIMESTAMP NOT NULL
);

CREATE TABLE courier (
    courier_id      BIGINT PRIMARY KEY,
    courier_name    VARCHAR(100) NOT NULL,
    created_at      TIMESTAMP NOT NULL,
    updated_at      TIMESTAMP NOT NULL
);

CREATE TABLE warehouse (
    warehouse_id    BIGINT PRIMARY KEY,
    city_id         BIGINT NOT NULL REFERENCES city(city_id),
    warehouse_name  VARCHAR(150) NOT NULL,
    created_at      TIMESTAMP NOT NULL,
    updated_at      TIMESTAMP NOT NULL
);

CREATE TABLE subscription_plan (
    plan_id         BIGINT PRIMARY KEY,
    plan_name       VARCHAR(50) NOT NULL,
    monthly_fee     DECIMAL(10,2) NOT NULL,
    video_quality   VARCHAR(20),
    max_devices     INT,
    created_at      TIMESTAMP NOT NULL,
    updated_at      TIMESTAMP NOT NULL
);

-- ----------------------------------------------------------------------------
-- Customer
-- ----------------------------------------------------------------------------

CREATE TABLE customer (
    customer_id       BIGINT PRIMARY KEY,
    customer_no       VARCHAR(20) NOT NULL,
    first_name        VARCHAR(100),
    last_name         VARCHAR(100),
    email             VARCHAR(150),
    phone             VARCHAR(30),
    date_of_birth     DATE,
    gender            VARCHAR(20),
    registration_date DATE,
    status            VARCHAR(20),
    created_at      TIMESTAMP NOT NULL,
    updated_at      TIMESTAMP NOT NULL
);

CREATE TABLE customer_address (
    address_id      BIGINT PRIMARY KEY,
    customer_id     BIGINT NOT NULL REFERENCES customer(customer_id),
    city_id         BIGINT NOT NULL REFERENCES city(city_id),
    address_line    VARCHAR(255),
    postal_code     VARCHAR(20),
    address_type    VARCHAR(20),
    created_at      TIMESTAMP NOT NULL,
    updated_at      TIMESTAMP NOT NULL
);

CREATE TABLE customer_subscription (
    subscription_id BIGINT PRIMARY KEY,
    customer_id     BIGINT NOT NULL REFERENCES customer(customer_id),
    plan_id         BIGINT NOT NULL REFERENCES subscription_plan(plan_id),
    start_date      DATE,
    end_date        DATE,
    status          VARCHAR(20),
    auto_renew      BOOLEAN,
    created_at      TIMESTAMP NOT NULL,
    updated_at      TIMESTAMP NOT NULL
);

-- ----------------------------------------------------------------------------
-- Content
-- ----------------------------------------------------------------------------

CREATE TABLE content (
    content_id       BIGINT PRIMARY KEY,
    content_type_id  BIGINT NOT NULL REFERENCES content_type(content_type_id),
    title            VARCHAR(255) NOT NULL,
    release_date     DATE,
    duration_minutes INT,
    language         VARCHAR(50),
    age_rating       VARCHAR(10),
    created_at      TIMESTAMP NOT NULL,
    updated_at      TIMESTAMP NOT NULL
);

CREATE TABLE content_genre (
    content_id      BIGINT NOT NULL REFERENCES content(content_id),
    genre_id        BIGINT NOT NULL REFERENCES genre(genre_id),
    created_at      TIMESTAMP NOT NULL,
    updated_at      TIMESTAMP NOT NULL,
    PRIMARY KEY (content_id, genre_id)
);

CREATE TABLE content_artist (
    content_id      BIGINT NOT NULL REFERENCES content(content_id),
    artist_id       BIGINT NOT NULL REFERENCES artist(artist_id),
    role            VARCHAR(50),
    created_at      TIMESTAMP NOT NULL,
    updated_at      TIMESTAMP NOT NULL,
    PRIMARY KEY (content_id, artist_id)
);

-- ----------------------------------------------------------------------------
-- Streaming
-- ----------------------------------------------------------------------------

CREATE TABLE streaming_session (
    stream_id            BIGINT PRIMARY KEY,
    customer_id          BIGINT NOT NULL REFERENCES customer(customer_id),
    content_id           BIGINT NOT NULL REFERENCES content(content_id),
    device_id            BIGINT NOT NULL REFERENCES device(device_id),
    start_time           TIMESTAMP,
    end_time             TIMESTAMP,
    watch_duration       INT,
    completion_percentage DECIMAL(5,2),
    created_at      TIMESTAMP NOT NULL,
    updated_at      TIMESTAMP NOT NULL
);

-- ----------------------------------------------------------------------------
-- Physical inventory and rentals
-- ----------------------------------------------------------------------------

CREATE TABLE inventory_item (
    inventory_id    BIGINT PRIMARY KEY,
    content_id      BIGINT NOT NULL REFERENCES content(content_id),
    warehouse_id    BIGINT NOT NULL REFERENCES warehouse(warehouse_id),
    barcode         VARCHAR(50),
    purchase_date   DATE,
    item_condition  VARCHAR(20),
    status          VARCHAR(20),
    created_at      TIMESTAMP NOT NULL,
    updated_at      TIMESTAMP NOT NULL
);

CREATE TABLE rental (
    rental_id       BIGINT PRIMARY KEY,
    customer_id     BIGINT NOT NULL REFERENCES customer(customer_id),
    inventory_id    BIGINT NOT NULL REFERENCES inventory_item(inventory_id),
    rental_date     DATE,
    due_date        DATE,
    return_date     DATE,
    rental_fee      DECIMAL(10,2),
    late_fee        DECIMAL(10,2),
    status          VARCHAR(20),
    created_at      TIMESTAMP NOT NULL,
    updated_at      TIMESTAMP NOT NULL
);

CREATE TABLE delivery (
    delivery_id     BIGINT PRIMARY KEY,
    rental_id       BIGINT NOT NULL REFERENCES rental(rental_id),
    courier_id      BIGINT NOT NULL REFERENCES courier(courier_id),
    dispatch_date   TIMESTAMP,
    delivered_date  TIMESTAMP,
    returned_date   TIMESTAMP,
    delivery_status VARCHAR(20),
    created_at      TIMESTAMP NOT NULL,
    updated_at      TIMESTAMP NOT NULL
);

-- ----------------------------------------------------------------------------
-- Payments
-- ----------------------------------------------------------------------------

CREATE TABLE payment (
    payment_id        BIGINT PRIMARY KEY,
    customer_id       BIGINT NOT NULL REFERENCES customer(customer_id),
    rental_id         BIGINT REFERENCES rental(rental_id),
    subscription_id   BIGINT REFERENCES customer_subscription(subscription_id),
    payment_method_id BIGINT NOT NULL REFERENCES payment_method(payment_method_id),
    amount            DECIMAL(10,2),
    payment_date      TIMESTAMP,
    payment_type      VARCHAR(20),
    status            VARCHAR(20),
    created_at      TIMESTAMP NOT NULL,
    updated_at      TIMESTAMP NOT NULL
);

-- ----------------------------------------------------------------------------
-- Engagement: reviews, wishlist, support, recommendations
-- ----------------------------------------------------------------------------

CREATE TABLE review (
    review_id       BIGINT PRIMARY KEY,
    customer_id     BIGINT NOT NULL REFERENCES customer(customer_id),
    content_id      BIGINT NOT NULL REFERENCES content(content_id),
    rating          INT,
    review_text     VARCHAR(500),
    review_date     TIMESTAMP,
    created_at      TIMESTAMP NOT NULL,
    updated_at      TIMESTAMP NOT NULL
);

CREATE TABLE wishlist (
    wishlist_id     BIGINT PRIMARY KEY,
    customer_id     BIGINT NOT NULL REFERENCES customer(customer_id),
    content_id      BIGINT NOT NULL REFERENCES content(content_id),
    added_date      TIMESTAMP,
    created_at      TIMESTAMP NOT NULL,
    updated_at      TIMESTAMP NOT NULL
);

CREATE TABLE support_ticket (
    ticket_id       BIGINT PRIMARY KEY,
    customer_id     BIGINT NOT NULL REFERENCES customer(customer_id),
    category_id     BIGINT NOT NULL REFERENCES support_category(category_id),
    opened_date     TIMESTAMP,
    closed_date     TIMESTAMP,
    priority        VARCHAR(20),
    status          VARCHAR(20),
    created_at      TIMESTAMP NOT NULL,
    updated_at      TIMESTAMP NOT NULL
);

CREATE TABLE recommendation (
    recommendation_id   BIGINT PRIMARY KEY,
    customer_id         BIGINT NOT NULL REFERENCES customer(customer_id),
    content_id          BIGINT NOT NULL REFERENCES content(content_id),
    recommendation_date TIMESTAMP,
    algorithm_version   VARCHAR(20),
    clicked             BOOLEAN,
    created_at      TIMESTAMP NOT NULL,
    updated_at      TIMESTAMP NOT NULL
);

-- ----------------------------------------------------------------------------
-- Helpful indexes (FK columns frequently joined/filtered by the ETL pipeline)
-- ----------------------------------------------------------------------------

CREATE INDEX idx_streaming_session_customer ON streaming_session(customer_id);
CREATE INDEX idx_streaming_session_content  ON streaming_session(content_id);
CREATE INDEX idx_streaming_session_start    ON streaming_session(start_time);
CREATE INDEX idx_rental_customer            ON rental(customer_id);
CREATE INDEX idx_rental_inventory           ON rental(inventory_id);
CREATE INDEX idx_payment_customer           ON payment(customer_id);
CREATE INDEX idx_payment_date               ON payment(payment_date);
CREATE INDEX idx_review_content             ON review(content_id);
CREATE INDEX idx_inventory_item_content     ON inventory_item(content_id);
