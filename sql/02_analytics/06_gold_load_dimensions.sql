-- ============================================================================
-- 06_gold_load_dimensions.sql               (database: abc_hub_analytics)
--
-- Silver -> gold, part 1: run preparation, dimension loads, finalisation.
-- Called in this order by NiFi process group "05 Build Gold - Star Schema":
--
--   etl.gold_prepare(run_id, full_refresh)   change set + calendar
--   etl.load_dim_subscription_plan(run_id)   SCD1
--   etl.load_dim_content(run_id)             SCD1
--   etl.load_dim_warehouse(run_id)           SCD1
--   etl.load_dim_customer(run_id)            SCD2
--   etl.load_dim_inventory_item(run_id)      SCD2
--   ... fact loads (07_gold_load_facts.sql) ...
--   etl.gold_finalize(run_id)                advance watermark
--
-- Every function is one transaction (a PostgreSQL function call is atomic),
-- logs itself to etl.run_step_log and returns one summary row that NiFi
-- writes to the FlowFile. Re-running any step with unchanged silver data
-- changes nothing: SCD1 upserts only touch rows whose values differ, SCD2
-- only opens a version when the tracked-attribute hash changes.
-- ============================================================================

-- ----------------------------------------------------------------------------
-- Run window and change set
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS etl.gold_run_window (
    run_id                  BIGINT    PRIMARY KEY REFERENCES etl.pipeline_run (run_id),
    full_refresh            BOOLEAN   NOT NULL,
    watermark_from          TIMESTAMP NOT NULL,   -- silver rows with _loaded_at >  from ...
    watermark_to            TIMESTAMP NOT NULL,   -- ... and <= to are processed by this run
    snapshot_start          DATE,                 -- inventory snapshot window
    snapshot_end            DATE,
    previous_snapshot_end   DATE                  -- end date of the last successful run
);

-- Business keys whose gold rows must be rebuilt by this run.
CREATE TABLE IF NOT EXISTS etl.gold_change_set (
    run_id      BIGINT      NOT NULL REFERENCES etl.pipeline_run (run_id),
    entity      VARCHAR(20) NOT NULL CHECK (entity IN ('customer', 'month', 'inventory')),
    key_id      BIGINT      NOT NULL,             -- customer_id, month_key (YYYYMM) or inventory_id
    PRIMARY KEY (run_id, entity, key_id)
);

-- ----------------------------------------------------------------------------
-- Cross-batch duplicate detection in silver.
-- NiFi removes duplicates that arrive in the SAME batch; this catches a copy
-- that arrives in a later batch than its original. The first record (lowest
-- id) of each business-key group survives; the rest are flagged and ignored
-- by every gold load. Only rows whose flag actually changes are updated.
-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION etl.flag_silver_duplicates()
RETURNS BIGINT
LANGUAGE plpgsql AS $$
DECLARE n BIGINT := 0; k BIGINT;
BEGIN
    WITH ranked AS (
        SELECT customer_id AS id,
               first_value(customer_id) OVER w AS first_id,
               row_number() OVER w AS rn
        FROM silver.customer
        WINDOW w AS (PARTITION BY lower(trim(email)) ORDER BY customer_id))
    UPDATE silver.customer s
       SET is_duplicate = (r.rn > 1), duplicate_of_id = CASE WHEN r.rn > 1 THEN r.first_id END
      FROM ranked r
     WHERE s.customer_id = r.id AND s.is_duplicate IS DISTINCT FROM (r.rn > 1);
    GET DIAGNOSTICS k = ROW_COUNT; n := n + k;

    WITH ranked AS (
        SELECT stream_id AS id,
               first_value(stream_id) OVER w AS first_id,
               row_number() OVER w AS rn
        FROM silver.streaming_session
        WINDOW w AS (PARTITION BY customer_id, content_id, device_id, start_time ORDER BY stream_id))
    UPDATE silver.streaming_session s
       SET is_duplicate = (r.rn > 1), duplicate_of_id = CASE WHEN r.rn > 1 THEN r.first_id END
      FROM ranked r
     WHERE s.stream_id = r.id AND s.is_duplicate IS DISTINCT FROM (r.rn > 1);
    GET DIAGNOSTICS k = ROW_COUNT; n := n + k;

    WITH ranked AS (
        SELECT payment_id AS id,
               first_value(payment_id) OVER w AS first_id,
               row_number() OVER w AS rn
        FROM silver.payment
        WINDOW w AS (PARTITION BY customer_id, payment_type, amount, payment_date, rental_id, subscription_id
                     ORDER BY payment_id))
    UPDATE silver.payment s
       SET is_duplicate = (r.rn > 1), duplicate_of_id = CASE WHEN r.rn > 1 THEN r.first_id END
      FROM ranked r
     WHERE s.payment_id = r.id AND s.is_duplicate IS DISTINCT FROM (r.rn > 1);
    GET DIAGNOSTICS k = ROW_COUNT; n := n + k;
    RETURN n;
END $$;

-- ----------------------------------------------------------------------------
-- Calendar dimensions: idempotent, extended to cover every business date.
-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION etl.ensure_calendar(p_from DATE, p_to DATE)
RETURNS BIGINT
LANGUAGE plpgsql AS $$
DECLARE n BIGINT; m BIGINT;
BEGIN
    INSERT INTO gold.dim_date (date_key, full_date, day_of_month, day_of_week, day_name, is_weekend, iso_week,
                               month_key, month_number, month_name, quarter_number, year_number, year_month_label)
    SELECT to_char(d, 'YYYYMMDD')::INT, d, extract(day FROM d), extract(isodow FROM d), trim(to_char(d, 'Day')),
           extract(isodow FROM d) IN (6, 7), extract(week FROM d), to_char(d, 'YYYYMM')::INT,
           extract(month FROM d), trim(to_char(d, 'Month')), extract(quarter FROM d), extract(year FROM d),
           to_char(d, 'YYYY-MM')
    FROM generate_series(p_from, p_to, INTERVAL '1 day') AS g(ts), LATERAL (SELECT g.ts::date AS d) x
    ON CONFLICT (date_key) DO NOTHING;
    GET DIAGNOSTICS n = ROW_COUNT;

    INSERT INTO gold.dim_month (month_key, month_start_date, month_end_date, days_in_month, month_number,
                                month_name, quarter_number, year_number, year_month_label)
    SELECT month_key, min(full_date), max(full_date), count(*), min(month_number), min(month_name),
           min(quarter_number), min(year_number), min(year_month_label)
    FROM gold.dim_date
    WHERE date_key > 0
    GROUP BY month_key
    ON CONFLICT (month_key) DO NOTHING;
    GET DIAGNOSTICS m = ROW_COUNT;
    RETURN n + m;
END $$;

-- ----------------------------------------------------------------------------
-- etl.gold_prepare - step 1 of every gold build
-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION etl.gold_prepare(p_run_id BIGINT, p_full_refresh BOOLEAN DEFAULT FALSE)
RETURNS TABLE (step VARCHAR, rows_inserted BIGINT, rows_updated BIGINT, rows_deleted BIGINT)
LANGUAGE plpgsql AS $$
DECLARE
    v_started   TIMESTAMP := clock_timestamp();
    v_dups      BIGINT;
    v_cal       BIGINT;
    v_from      TIMESTAMP;
    v_to        TIMESTAMP;
    v_prev_end  DATE;
    v_snap_from DATE;
    v_snap_to   DATE;
    v_first     BOOLEAN;
    v_keys      BIGINT;
BEGIN
    v_dups := etl.flag_silver_duplicates();

    -- The change window: everything loaded into silver since the last successful build.
    SELECT watermark_value INTO v_from FROM etl.watermark WHERE watermark_name = 'gold_build';
    v_first := v_from IS NULL;
    IF p_full_refresh OR v_first THEN
        v_from := TIMESTAMP '-infinity';
    END IF;
    SELECT max(t) INTO v_to FROM (
        SELECT max(_loaded_at) t FROM silver.customer UNION ALL
        SELECT max(_loaded_at) FROM silver.customer_address UNION ALL
        SELECT max(_loaded_at) FROM silver.customer_subscription UNION ALL
        SELECT max(_loaded_at) FROM silver.content UNION ALL
        SELECT max(_loaded_at) FROM silver.inventory_item UNION ALL
        SELECT max(_loaded_at) FROM silver.streaming_session UNION ALL
        SELECT max(_loaded_at) FROM silver.rental UNION ALL
        SELECT max(_loaded_at) FROM silver.payment UNION ALL
        SELECT max(_loaded_at) FROM silver.review UNION ALL
        SELECT max(_loaded_at) FROM silver.wishlist UNION ALL
        SELECT max(_loaded_at) FROM silver.support_ticket) x;
    v_to := COALESCE(v_to, v_from);

    -- Snapshot window for Scenario C: first to last business date seen in silver.
    SELECT min(d), max(d) INTO v_snap_from, v_snap_to FROM (
        SELECT min(rental_date) d FROM silver.rental UNION ALL
        SELECT max(GREATEST(rental_date, return_date)) FROM silver.rental UNION ALL
        SELECT min(start_time)::date FROM silver.streaming_session UNION ALL
        SELECT max(start_time)::date FROM silver.streaming_session UNION ALL
        SELECT max(payment_date)::date FROM silver.payment) x;

    SELECT w.snapshot_end INTO v_prev_end
      FROM etl.gold_run_window w JOIN etl.pipeline_run r USING (run_id)
     WHERE r.status = 'SUCCEEDED'
     ORDER BY w.run_id DESC LIMIT 1;

    INSERT INTO etl.gold_run_window (run_id, full_refresh, watermark_from, watermark_to,
                                     snapshot_start, snapshot_end, previous_snapshot_end)
    VALUES (p_run_id, p_full_refresh OR v_first, v_from, v_to, v_snap_from, v_snap_to,
            CASE WHEN p_full_refresh OR v_first THEN NULL ELSE v_prev_end END)
    ON CONFLICT (run_id) DO UPDATE
       SET full_refresh = EXCLUDED.full_refresh, watermark_from = EXCLUDED.watermark_from,
           watermark_to = EXCLUDED.watermark_to, snapshot_start = EXCLUDED.snapshot_start,
           snapshot_end = EXCLUDED.snapshot_end, previous_snapshot_end = EXCLUDED.previous_snapshot_end;

    -- Calendar covering every date any dimension or fact can reference.
    v_cal := etl.ensure_calendar(
        LEAST(DATE '2018-01-01',
              (SELECT min(purchase_date) FROM silver.inventory_item),
              (SELECT min(registration_date) FROM silver.customer)),
        GREATEST(DATE '2030-12-31', v_snap_to + 365));

    -- Change set. Customers: any activity or profile change in the window.
    DELETE FROM etl.gold_change_set WHERE run_id = p_run_id;
    INSERT INTO etl.gold_change_set (run_id, entity, key_id)
    SELECT DISTINCT p_run_id, 'customer', customer_id FROM (
        SELECT customer_id FROM silver.customer              WHERE _loaded_at > v_from AND _loaded_at <= v_to UNION
        SELECT customer_id FROM silver.customer_address      WHERE _loaded_at > v_from AND _loaded_at <= v_to UNION
        SELECT customer_id FROM silver.customer_subscription WHERE _loaded_at > v_from AND _loaded_at <= v_to UNION
        SELECT customer_id FROM silver.streaming_session     WHERE _loaded_at > v_from AND _loaded_at <= v_to UNION
        SELECT customer_id FROM silver.rental                WHERE _loaded_at > v_from AND _loaded_at <= v_to UNION
        SELECT customer_id FROM silver.payment               WHERE _loaded_at > v_from AND _loaded_at <= v_to UNION
        SELECT customer_id FROM silver.support_ticket        WHERE _loaded_at > v_from AND _loaded_at <= v_to UNION
        SELECT customer_id FROM silver.review                WHERE _loaded_at > v_from AND _loaded_at <= v_to UNION
        SELECT customer_id FROM silver.wishlist              WHERE _loaded_at > v_from AND _loaded_at <= v_to) c;

    -- Months: every month in which a changed event happened (whole month is re-aggregated).
    INSERT INTO etl.gold_change_set (run_id, entity, key_id)
    SELECT DISTINCT p_run_id, 'month', to_char(d, 'YYYYMM')::BIGINT FROM (
        SELECT start_time::date d FROM silver.streaming_session WHERE _loaded_at > v_from AND _loaded_at <= v_to UNION
        SELECT rental_date        FROM silver.rental            WHERE _loaded_at > v_from AND _loaded_at <= v_to UNION
        SELECT payment_date::date FROM silver.payment           WHERE _loaded_at > v_from AND _loaded_at <= v_to UNION
        SELECT review_date::date  FROM silver.review            WHERE _loaded_at > v_from AND _loaded_at <= v_to UNION
        SELECT added_date::date   FROM silver.wishlist          WHERE _loaded_at > v_from AND _loaded_at <= v_to) m
    WHERE d IS NOT NULL;

    -- Inventory items: item changes or any rental of the item changed.
    INSERT INTO etl.gold_change_set (run_id, entity, key_id)
    SELECT DISTINCT p_run_id, 'inventory', inventory_id FROM (
        SELECT inventory_id FROM silver.inventory_item WHERE _loaded_at > v_from AND _loaded_at <= v_to UNION
        SELECT inventory_id FROM silver.rental         WHERE _loaded_at > v_from AND _loaded_at <= v_to) i;
    SELECT count(*) INTO v_keys FROM etl.gold_change_set WHERE run_id = p_run_id;

    PERFORM etl.log_step(p_run_id, 'gold_prepare', v_started, v_cal, v_dups, 0,
        format('window (%s, %s]; %s changed keys; snapshot %s..%s; full_refresh=%s',
               v_from, v_to, v_keys, v_snap_from, v_snap_to, p_full_refresh OR v_first));
    RETURN QUERY SELECT 'gold_prepare'::VARCHAR, v_cal, v_dups, 0::BIGINT;
END $$;

-- ----------------------------------------------------------------------------
-- dim_subscription_plan - SCD1
-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION etl.load_dim_subscription_plan(p_run_id BIGINT)
RETURNS TABLE (step VARCHAR, rows_inserted BIGINT, rows_updated BIGINT, rows_deleted BIGINT)
LANGUAGE plpgsql AS $$
DECLARE v_started TIMESTAMP := clock_timestamp(); v_ins BIGINT; v_upd BIGINT;
BEGIN
    WITH src AS (
        SELECT plan_id, plan_name, monthly_fee, video_quality, max_devices,
               dense_rank() OVER (ORDER BY monthly_fee)::SMALLINT AS plan_tier
        FROM silver.subscription_plan),
    up AS (
        INSERT INTO gold.dim_subscription_plan AS d (plan_id, plan_name, monthly_fee, video_quality, max_devices,
                                                     plan_tier, _run_id)
        SELECT plan_id, plan_name, monthly_fee, video_quality, max_devices, plan_tier, p_run_id FROM src
        ON CONFLICT (plan_id) DO UPDATE
           SET plan_name = EXCLUDED.plan_name, monthly_fee = EXCLUDED.monthly_fee,
               video_quality = EXCLUDED.video_quality, max_devices = EXCLUDED.max_devices,
               plan_tier = EXCLUDED.plan_tier, _run_id = EXCLUDED._run_id, _loaded_at = clock_timestamp()
         WHERE (d.plan_name, d.monthly_fee, d.video_quality, d.max_devices, d.plan_tier)
               IS DISTINCT FROM (EXCLUDED.plan_name, EXCLUDED.monthly_fee, EXCLUDED.video_quality,
                                 EXCLUDED.max_devices, EXCLUDED.plan_tier)
        RETURNING (xmax = 0) AS inserted)
    SELECT count(*) FILTER (WHERE inserted), count(*) FILTER (WHERE NOT inserted) INTO v_ins, v_upd FROM up;
    PERFORM etl.log_step(p_run_id, 'load_dim_subscription_plan', v_started, v_ins, v_upd, 0);
    RETURN QUERY SELECT 'load_dim_subscription_plan'::VARCHAR, v_ins, v_upd, 0::BIGINT;
END $$;

-- ----------------------------------------------------------------------------
-- dim_content - SCD1. Assumptions (documented in the README):
--   * primary genre  = the genre with the lowest genre_id (no primary flag in source)
--   * primary artist = by role priority Director > Lead Actor > Performer >
--                      Author > Composer > Writer > Producer > Supporting Actor
--   * streamable / rentable by content type, as stated in the data package
-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION etl.load_dim_content(p_run_id BIGINT)
RETURNS TABLE (step VARCHAR, rows_inserted BIGINT, rows_updated BIGINT, rows_deleted BIGINT)
LANGUAGE plpgsql AS $$
DECLARE v_started TIMESTAMP := clock_timestamp(); v_ins BIGINT; v_upd BIGINT;
BEGIN
    WITH genres AS (
        SELECT cg.content_id,
               (array_agg(g.genre_name ORDER BY g.genre_id))[1]        AS primary_genre,
               string_agg(g.genre_name, ', ' ORDER BY g.genre_name)   AS genre_list
        FROM silver.content_genre cg JOIN silver.genre g USING (genre_id)
        GROUP BY cg.content_id),
    artists AS (
        SELECT DISTINCT ON (ca.content_id) ca.content_id, a.artist_name, ca.role
        FROM silver.content_artist ca JOIN silver.artist a USING (artist_id)
        ORDER BY ca.content_id,
                 array_position(ARRAY['Director', 'Lead Actor', 'Performer', 'Author', 'Composer', 'Writer',
                                      'Producer', 'Supporting Actor'], ca.role) NULLS LAST,
                 ca.artist_id),
    src AS (
        SELECT c.content_id, c.title, ct.content_type,
               ct.content_type IN ('Movie', 'TV Series', 'Documentary', 'Music') AS is_streamable,
               ct.content_type IN ('Movie', 'Music', 'Book', 'Video Game')       AS is_rentable,
               COALESCE(g.primary_genre, 'Unclassified') AS primary_genre,
               COALESCE(g.genre_list, 'Unclassified')    AS genre_list,
               COALESCE(a.artist_name, 'Not credited')   AS primary_artist,
               COALESCE(a.role, 'Not credited')          AS primary_artist_role,
               c.language, COALESCE(c.age_rating, 'Unrated') AS age_rating, c.release_date,
               extract(year FROM c.release_date)::SMALLINT AS release_year, c.duration_minutes,
               CASE WHEN c.duration_minutes IS NULL THEN 'Not applicable'
                    WHEN c.duration_minutes < 30  THEN 'Under 30 min'
                    WHEN c.duration_minutes < 90  THEN '30-89 min'
                    WHEN c.duration_minutes < 150 THEN '90-149 min'
                    ELSE '150+ min' END AS duration_band
        FROM silver.content c
        JOIN silver.content_type ct USING (content_type_id)
        LEFT JOIN genres g  USING (content_id)
        LEFT JOIN artists a USING (content_id)),
    up AS (
        INSERT INTO gold.dim_content AS d (content_id, title, content_type, is_streamable, is_rentable, availability,
               primary_genre, genre_list, primary_artist, primary_artist_role, language, age_rating, release_date,
               release_year, duration_minutes, duration_band, _run_id)
        SELECT content_id, title, content_type, is_streamable, is_rentable,
               CASE WHEN is_streamable AND is_rentable THEN 'Streaming & Rental'
                    WHEN is_streamable THEN 'Streaming only' ELSE 'Rental only' END,
               primary_genre, genre_list, primary_artist, primary_artist_role, language, age_rating, release_date,
               release_year, duration_minutes, duration_band, p_run_id
        FROM src
        ON CONFLICT (content_id) DO UPDATE
           SET title = EXCLUDED.title, content_type = EXCLUDED.content_type, is_streamable = EXCLUDED.is_streamable,
               is_rentable = EXCLUDED.is_rentable, availability = EXCLUDED.availability,
               primary_genre = EXCLUDED.primary_genre, genre_list = EXCLUDED.genre_list,
               primary_artist = EXCLUDED.primary_artist, primary_artist_role = EXCLUDED.primary_artist_role,
               language = EXCLUDED.language, age_rating = EXCLUDED.age_rating, release_date = EXCLUDED.release_date,
               release_year = EXCLUDED.release_year, duration_minutes = EXCLUDED.duration_minutes,
               duration_band = EXCLUDED.duration_band, _run_id = EXCLUDED._run_id, _loaded_at = clock_timestamp()
         WHERE (d.title, d.content_type, d.primary_genre, d.genre_list, d.primary_artist, d.primary_artist_role,
                d.language, d.age_rating, d.release_date, d.duration_minutes)
               IS DISTINCT FROM
               (EXCLUDED.title, EXCLUDED.content_type, EXCLUDED.primary_genre, EXCLUDED.genre_list,
                EXCLUDED.primary_artist, EXCLUDED.primary_artist_role, EXCLUDED.language, EXCLUDED.age_rating,
                EXCLUDED.release_date, EXCLUDED.duration_minutes)
        RETURNING (xmax = 0) AS inserted)
    SELECT count(*) FILTER (WHERE inserted), count(*) FILTER (WHERE NOT inserted) INTO v_ins, v_upd FROM up;
    PERFORM etl.log_step(p_run_id, 'load_dim_content', v_started, v_ins, v_upd, 0);
    RETURN QUERY SELECT 'load_dim_content'::VARCHAR, v_ins, v_upd, 0::BIGINT;
END $$;

-- ----------------------------------------------------------------------------
-- dim_warehouse - SCD1 (location flattened in)
-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION etl.load_dim_warehouse(p_run_id BIGINT)
RETURNS TABLE (step VARCHAR, rows_inserted BIGINT, rows_updated BIGINT, rows_deleted BIGINT)
LANGUAGE plpgsql AS $$
DECLARE v_started TIMESTAMP := clock_timestamp(); v_ins BIGINT; v_upd BIGINT;
BEGIN
    WITH up AS (
        INSERT INTO gold.dim_warehouse AS d (warehouse_id, warehouse_name, city, country, country_code, _run_id)
        SELECT w.warehouse_id, w.warehouse_name, COALESCE(ci.city_name, 'Unknown'),
               COALESCE(co.country_name, 'Unknown'), COALESCE(co.country_code, '??'), p_run_id
        FROM silver.warehouse w
        LEFT JOIN silver.city ci    ON ci.city_id = w.city_id
        LEFT JOIN silver.country co ON co.country_id = ci.country_id
        ON CONFLICT (warehouse_id) DO UPDATE
           SET warehouse_name = EXCLUDED.warehouse_name, city = EXCLUDED.city, country = EXCLUDED.country,
               country_code = EXCLUDED.country_code, _run_id = EXCLUDED._run_id, _loaded_at = clock_timestamp()
         WHERE (d.warehouse_name, d.city, d.country, d.country_code)
               IS DISTINCT FROM (EXCLUDED.warehouse_name, EXCLUDED.city, EXCLUDED.country, EXCLUDED.country_code)
        RETURNING (xmax = 0) AS inserted)
    SELECT count(*) FILTER (WHERE inserted), count(*) FILTER (WHERE NOT inserted) INTO v_ins, v_upd FROM up;
    PERFORM etl.log_step(p_run_id, 'load_dim_warehouse', v_started, v_ins, v_upd, 0);
    RETURN QUERY SELECT 'load_dim_warehouse'::VARCHAR, v_ins, v_upd, 0::BIGINT;
END $$;

-- ----------------------------------------------------------------------------
-- dim_customer - SCD2
--   Type 2 : customer_status, home_city, home_country, home_country_code
--   Type 1 : name, e-mail, phone, gender, date of birth, age band, registration
--   A change dated the same day as the current version overwrites it in place
--   (no zero-length versions). The first version of every customer starts at
--   1900-01-01 because no earlier history exists in the source.
-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION etl.load_dim_customer(p_run_id BIGINT)
RETURNS TABLE (step VARCHAR, rows_inserted BIGINT, rows_updated BIGINT, rows_deleted BIGINT)
LANGUAGE plpgsql AS $$
DECLARE v_started TIMESTAMP := clock_timestamp(); v_ins BIGINT := 0; v_upd BIGINT := 0; k BIGINT;
BEGIN
    CREATE TEMP TABLE tmp_customer_src ON COMMIT DROP AS
    WITH home AS (
        SELECT DISTINCT ON (a.customer_id) a.customer_id, a.city_id, a.updated_at
        FROM silver.customer_address a
        ORDER BY a.customer_id, (a.address_type = 'Home') DESC, a.updated_at DESC, a.address_id DESC)
    SELECT c.customer_id, c.customer_no,
           trim(concat_ws(' ', c.first_name, c.last_name)) AS full_name,
           c.email, c.phone, c.gender, c.date_of_birth,
           CASE WHEN c.date_of_birth IS NULL OR c.registration_date IS NULL THEN 'Unknown'
                ELSE CASE WHEN age < 18 THEN 'Under 18' WHEN age < 25 THEN '18-24' WHEN age < 35 THEN '25-34'
                          WHEN age < 45 THEN '35-44' WHEN age < 55 THEN '45-54' WHEN age < 65 THEN '55-64'
                          ELSE '65+' END END AS age_band,
           c.registration_date, to_char(c.registration_date, 'YYYY-MM') AS registration_month,
           c.status AS customer_status,
           COALESCE(ci.city_name, 'Unknown') AS home_city,
           COALESCE(co.country_name, 'Unknown') AS home_country,
           COALESCE(co.country_code, '??') AS home_country_code,
           GREATEST(c.updated_at, h.updated_at)::date AS change_date
    FROM silver.customer c
    LEFT JOIN home h            ON h.customer_id = c.customer_id
    LEFT JOIN silver.city ci    ON ci.city_id = h.city_id
    LEFT JOIN silver.country co ON co.country_id = ci.country_id
    CROSS JOIN LATERAL (SELECT extract(year FROM age(c.registration_date, c.date_of_birth))::INT AS age) a
    WHERE NOT c.is_duplicate;
    ALTER TABLE tmp_customer_src ADD COLUMN scd2_hash CHAR(32);
    UPDATE tmp_customer_src
       SET scd2_hash = md5(concat_ws('|', customer_status, home_city, home_country, home_country_code));

    -- 1. Type 1 attributes: overwrite on every version.
    UPDATE gold.dim_customer d
       SET customer_no = s.customer_no, full_name = s.full_name, email = s.email, phone = s.phone,
           gender = s.gender, date_of_birth = s.date_of_birth, age_band = s.age_band,
           registration_date = s.registration_date, registration_month = s.registration_month,
           _run_id = p_run_id, _loaded_at = clock_timestamp()
      FROM tmp_customer_src s
     WHERE d.customer_id = s.customer_id
       AND (d.customer_no, d.full_name, d.email, d.phone, d.gender, d.date_of_birth, d.age_band, d.registration_date)
           IS DISTINCT FROM
           (s.customer_no, s.full_name, s.email, s.phone, s.gender, s.date_of_birth, s.age_band, s.registration_date);
    GET DIAGNOSTICS k = ROW_COUNT; v_upd := v_upd + k;

    -- 2. SCD2 change dated on the current version's start day: overwrite in place.
    UPDATE gold.dim_customer d
       SET customer_status = s.customer_status, home_city = s.home_city, home_country = s.home_country,
           home_country_code = s.home_country_code, scd2_hash = s.scd2_hash,
           _run_id = p_run_id, _loaded_at = clock_timestamp()
      FROM tmp_customer_src s
     WHERE d.customer_id = s.customer_id AND d.is_current
       AND d.scd2_hash <> s.scd2_hash AND s.change_date <= d.effective_from;
    GET DIAGNOSTICS k = ROW_COUNT; v_upd := v_upd + k;

    -- 3. SCD2 change on a later day: close the current version ...
    CREATE TEMP TABLE tmp_customer_changed ON COMMIT DROP AS
    SELECT s.* FROM tmp_customer_src s
    JOIN gold.dim_customer d ON d.customer_id = s.customer_id AND d.is_current
    WHERE d.scd2_hash <> s.scd2_hash AND s.change_date > d.effective_from;

    UPDATE gold.dim_customer d
       SET effective_to = s.change_date - 1, is_current = FALSE, _run_id = p_run_id, _loaded_at = clock_timestamp()
      FROM tmp_customer_changed s
     WHERE d.customer_id = s.customer_id AND d.is_current;
    GET DIAGNOSTICS k = ROW_COUNT; v_upd := v_upd + k;

    -- ... and open the new version (plus the first version of brand-new customers).
    INSERT INTO gold.dim_customer (customer_id, customer_no, full_name, email, phone, gender, date_of_birth, age_band,
                                   registration_date, registration_month, customer_status, home_city, home_country,
                                   home_country_code, scd2_hash, effective_from, _run_id)
    SELECT s.customer_id, s.customer_no, s.full_name, s.email, s.phone, s.gender, s.date_of_birth, s.age_band,
           s.registration_date, s.registration_month, s.customer_status, s.home_city, s.home_country,
           s.home_country_code, s.scd2_hash,
           CASE WHEN ch.customer_id IS NULL THEN DATE '1900-01-01' ELSE s.change_date END, p_run_id
    FROM tmp_customer_src s
    LEFT JOIN tmp_customer_changed ch ON ch.customer_id = s.customer_id
    WHERE ch.customer_id IS NOT NULL
       OR NOT EXISTS (SELECT 1 FROM gold.dim_customer d WHERE d.customer_id = s.customer_id);
    GET DIAGNOSTICS v_ins = ROW_COUNT;

    PERFORM etl.log_step(p_run_id, 'load_dim_customer', v_started, v_ins, v_upd, 0);
    RETURN QUERY SELECT 'load_dim_customer'::VARCHAR, v_ins, v_upd, 0::BIGINT;
END $$;

-- ----------------------------------------------------------------------------
-- dim_inventory_item - SCD2
--   Type 2 : item_status, item_condition, warehouse_id
--   Type 1 : barcode, content, purchase date, in-service date
--   in_service_date = LEAST(purchase_date, first rental) - 338 rentals in the
--   source pre-date the recorded purchase date (profiling finding).
-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION etl.load_dim_inventory_item(p_run_id BIGINT)
RETURNS TABLE (step VARCHAR, rows_inserted BIGINT, rows_updated BIGINT, rows_deleted BIGINT)
LANGUAGE plpgsql AS $$
DECLARE v_started TIMESTAMP := clock_timestamp(); v_ins BIGINT := 0; v_upd BIGINT := 0; k BIGINT;
BEGIN
    CREATE TEMP TABLE tmp_inventory_src ON COMMIT DROP AS
    SELECT i.inventory_id, COALESCE(i.barcode, 'NO-BARCODE-' || i.inventory_id) AS barcode, i.content_id,
           i.purchase_date,
           LEAST(i.purchase_date, (SELECT min(r.rental_date) FROM silver.rental r
                                    WHERE r.inventory_id = i.inventory_id)) AS in_service_date,
           COALESCE(i.item_condition, 'Unknown') AS item_condition, i.status AS item_status, i.warehouse_id,
           md5(concat_ws('|', i.status, i.item_condition, i.warehouse_id)) AS scd2_hash,
           i.updated_at::date AS change_date
    FROM silver.inventory_item i;

    UPDATE gold.dim_inventory_item d
       SET barcode = s.barcode, content_id = s.content_id, purchase_date = s.purchase_date,
           in_service_date = s.in_service_date, _run_id = p_run_id, _loaded_at = clock_timestamp()
      FROM tmp_inventory_src s
     WHERE d.inventory_id = s.inventory_id
       AND (d.barcode, d.content_id, d.purchase_date, d.in_service_date)
           IS DISTINCT FROM (s.barcode, s.content_id, s.purchase_date, s.in_service_date);
    GET DIAGNOSTICS k = ROW_COUNT; v_upd := v_upd + k;

    UPDATE gold.dim_inventory_item d
       SET item_status = s.item_status, item_condition = s.item_condition, warehouse_id = s.warehouse_id,
           scd2_hash = s.scd2_hash, _run_id = p_run_id, _loaded_at = clock_timestamp()
      FROM tmp_inventory_src s
     WHERE d.inventory_id = s.inventory_id AND d.is_current
       AND d.scd2_hash <> s.scd2_hash AND s.change_date <= d.effective_from;
    GET DIAGNOSTICS k = ROW_COUNT; v_upd := v_upd + k;

    CREATE TEMP TABLE tmp_inventory_changed ON COMMIT DROP AS
    SELECT s.* FROM tmp_inventory_src s
    JOIN gold.dim_inventory_item d ON d.inventory_id = s.inventory_id AND d.is_current
    WHERE d.scd2_hash <> s.scd2_hash AND s.change_date > d.effective_from;

    UPDATE gold.dim_inventory_item d
       SET effective_to = s.change_date - 1, is_current = FALSE, _run_id = p_run_id, _loaded_at = clock_timestamp()
      FROM tmp_inventory_changed s
     WHERE d.inventory_id = s.inventory_id AND d.is_current;
    GET DIAGNOSTICS k = ROW_COUNT; v_upd := v_upd + k;

    INSERT INTO gold.dim_inventory_item (inventory_id, barcode, content_id, purchase_date, in_service_date,
                                         item_condition, item_status, warehouse_id, scd2_hash, effective_from, _run_id)
    SELECT s.inventory_id, s.barcode, s.content_id, s.purchase_date, s.in_service_date, s.item_condition,
           s.item_status, s.warehouse_id, s.scd2_hash,
           CASE WHEN ch.inventory_id IS NULL THEN DATE '1900-01-01' ELSE s.change_date END, p_run_id
    FROM tmp_inventory_src s
    LEFT JOIN tmp_inventory_changed ch ON ch.inventory_id = s.inventory_id
    WHERE ch.inventory_id IS NOT NULL
       OR NOT EXISTS (SELECT 1 FROM gold.dim_inventory_item d WHERE d.inventory_id = s.inventory_id);
    GET DIAGNOSTICS v_ins = ROW_COUNT;

    PERFORM etl.log_step(p_run_id, 'load_dim_inventory_item', v_started, v_ins, v_upd, 0);
    RETURN QUERY SELECT 'load_dim_inventory_item'::VARCHAR, v_ins, v_upd, 0::BIGINT;
END $$;

-- ----------------------------------------------------------------------------
-- etl.gold_finalize - last step: advance the watermark, refresh statistics.
-- Only reached when every previous step succeeded (NiFi stops the chain on
-- failure), so a failed run is simply re-processed by the next run.
-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION etl.gold_finalize(p_run_id BIGINT)
RETURNS TABLE (step VARCHAR, rows_inserted BIGINT, rows_updated BIGINT, rows_deleted BIGINT)
LANGUAGE plpgsql AS $$
DECLARE v_started TIMESTAMP := clock_timestamp(); v_to TIMESTAMP;
BEGIN
    SELECT watermark_to INTO v_to FROM etl.gold_run_window WHERE run_id = p_run_id;
    INSERT INTO etl.watermark (watermark_name, watermark_value)
    VALUES ('gold_build', v_to)
    ON CONFLICT (watermark_name) DO UPDATE
       SET watermark_value = EXCLUDED.watermark_value, updated_at = clock_timestamp();
    DELETE FROM etl.gold_change_set WHERE run_id < p_run_id - 50;   -- keep recent change sets for audit
    ANALYZE gold.fact_customer_daily_activity;
    ANALYZE gold.fact_content_monthly_performance;
    ANALYZE gold.fact_inventory_daily_snapshot;
    PERFORM etl.log_step(p_run_id, 'gold_finalize', v_started, 0, 1, 0, format('watermark -> %s', v_to));
    RETURN QUERY SELECT 'gold_finalize'::VARCHAR, 0::BIGINT, 1::BIGINT, 0::BIGINT;
END $$;
