-- ============================================================================
-- 02_load_operational_data.sql
-- Loads the 26 ABC Hub CSV files into the operational database.
--
-- Why not a plain "\copy <table> FROM <csv>"?
--   Several integer columns were exported by a dataframe tool as floats
--   ("1.0", "72.0"): payment.rental_id, payment.subscription_id,
--   content.duration_minutes and streaming_session.watch_duration.
--   PostgreSQL rejects '1.0' for BIGINT/INT, so a direct \copy fails.
--
-- Approach (lossless, source values untouched):
--   1. Build a throw-away schema "_load" with an all-TEXT copy of every table.
--   2. \copy each CSV into its text table.
--   3. INSERT into the real table, casting integer columns via NUMERIC
--      ('1.0'::numeric::bigint = 1). Every other column uses a plain cast.
--   4. Drop "_load".
--
-- Run AFTER the provided DDL (which drops and recreates schema public):
--   psql -d abc_hub_operational -f "ABC Hub Data Package/packages/ddl/operational_schema.sql"
--   psql -d abc_hub_operational -v datadir="ABC Hub Data Package/packages/data" \
--        -f sql/00_setup/02_load_operational_data.sql
-- ============================================================================

\set ON_ERROR_STOP on

\if :{?datadir}
\else
  \set datadir 'ABC Hub Data Package/packages/data'
\endif

-- Tables in foreign-key order (parents before children), per the package README.
DROP SCHEMA IF EXISTS _load CASCADE;
CREATE SCHEMA _load;

CREATE TABLE _load.load_order (seq INT PRIMARY KEY, table_name TEXT NOT NULL);
INSERT INTO _load.load_order VALUES
 ( 1,'country'),( 2,'city'),( 3,'content_type'),( 4,'genre'),( 5,'artist'),
 ( 6,'device'),( 7,'payment_method'),( 8,'support_category'),( 9,'courier'),
 (10,'warehouse'),(11,'subscription_plan'),(12,'customer'),(13,'customer_address'),
 (14,'customer_subscription'),(15,'content'),(16,'content_genre'),(17,'content_artist'),
 (18,'inventory_item'),(19,'streaming_session'),(20,'rental'),(21,'delivery'),
 (22,'payment'),(23,'review'),(24,'wishlist'),(25,'support_ticket'),(26,'recommendation');

-- Step 1: all-TEXT landing copies with identical column order.
DO $$
DECLARE r RECORD; cols TEXT;
BEGIN
  FOR r IN SELECT table_name FROM _load.load_order ORDER BY seq LOOP
    SELECT string_agg(format('%I TEXT', column_name), ', ' ORDER BY ordinal_position)
      INTO cols
      FROM information_schema.columns
     WHERE table_schema = 'public' AND table_name = r.table_name;
    EXECUTE format('CREATE TABLE _load.%I (%s)', r.table_name, cols);
  END LOOP;
END $$;

-- Step 2: bulk-load the CSVs (psql client-side \copy; paths relative to datadir).
\cd :datadir
\copy _load.country               FROM 'country.csv'               WITH (FORMAT csv, HEADER true)
\copy _load.city                  FROM 'city.csv'                  WITH (FORMAT csv, HEADER true)
\copy _load.content_type          FROM 'content_type.csv'          WITH (FORMAT csv, HEADER true)
\copy _load.genre                 FROM 'genre.csv'                 WITH (FORMAT csv, HEADER true)
\copy _load.artist                FROM 'artist.csv'                WITH (FORMAT csv, HEADER true)
\copy _load.device                FROM 'device.csv'                WITH (FORMAT csv, HEADER true)
\copy _load.payment_method        FROM 'payment_method.csv'        WITH (FORMAT csv, HEADER true)
\copy _load.support_category      FROM 'support_category.csv'      WITH (FORMAT csv, HEADER true)
\copy _load.courier               FROM 'courier.csv'               WITH (FORMAT csv, HEADER true)
\copy _load.warehouse             FROM 'warehouse.csv'             WITH (FORMAT csv, HEADER true)
\copy _load.subscription_plan     FROM 'subscription_plan.csv'     WITH (FORMAT csv, HEADER true)
\copy _load.customer              FROM 'customer.csv'              WITH (FORMAT csv, HEADER true)
\copy _load.customer_address      FROM 'customer_address.csv'      WITH (FORMAT csv, HEADER true)
\copy _load.customer_subscription FROM 'customer_subscription.csv' WITH (FORMAT csv, HEADER true)
\copy _load.content               FROM 'content.csv'               WITH (FORMAT csv, HEADER true)
\copy _load.content_genre         FROM 'content_genre.csv'         WITH (FORMAT csv, HEADER true)
\copy _load.content_artist        FROM 'content_artist.csv'        WITH (FORMAT csv, HEADER true)
\copy _load.inventory_item        FROM 'inventory_item.csv'        WITH (FORMAT csv, HEADER true)
\copy _load.streaming_session     FROM 'streaming_session.csv'     WITH (FORMAT csv, HEADER true)
\copy _load.rental                FROM 'rental.csv'                WITH (FORMAT csv, HEADER true)
\copy _load.delivery              FROM 'delivery.csv'              WITH (FORMAT csv, HEADER true)
\copy _load.payment               FROM 'payment.csv'               WITH (FORMAT csv, HEADER true)
\copy _load.review                FROM 'review.csv'                WITH (FORMAT csv, HEADER true)
\copy _load.wishlist              FROM 'wishlist.csv'              WITH (FORMAT csv, HEADER true)
\copy _load.support_ticket        FROM 'support_ticket.csv'        WITH (FORMAT csv, HEADER true)
\copy _load.recommendation        FROM 'recommendation.csv'        WITH (FORMAT csv, HEADER true)

-- Step 3: typed insert into the real tables, parents first.
DO $$
DECLARE r RECORD; col_list TEXT; sel_list TEXT; n BIGINT;
BEGIN
  FOR r IN SELECT table_name FROM _load.load_order ORDER BY seq LOOP
    SELECT string_agg(format('%I', c.column_name), ', ' ORDER BY c.ordinal_position),
           string_agg(
             CASE WHEN c.data_type IN ('bigint', 'integer', 'smallint')
                  THEN format('%I::numeric::%s', c.column_name, c.data_type)
                  ELSE format('%I::%s', c.column_name,
                              pg_catalog.format_type(a.atttypid, a.atttypmod))
             END, ', ' ORDER BY c.ordinal_position)
      INTO col_list, sel_list
      FROM information_schema.columns c
      JOIN pg_catalog.pg_attribute a
        ON a.attrelid = format('public.%I', c.table_name)::regclass
       AND a.attname  = c.column_name
     WHERE c.table_schema = 'public' AND c.table_name = r.table_name;

    EXECUTE format('TRUNCATE public.%I CASCADE', r.table_name);
    EXECUTE format('INSERT INTO public.%I (%s) SELECT %s FROM _load.%I',
                   r.table_name, col_list, sel_list, r.table_name);
    GET DIAGNOSTICS n = ROW_COUNT;
    RAISE NOTICE 'loaded %-22s % rows', r.table_name, n;
  END LOOP;
END $$;

-- Step 4: clean up and refresh planner statistics.
DROP SCHEMA _load CASCADE;
ANALYZE;

-- Row-count check against the package README (expected total: 98,700).
SELECT relname AS table_name, n_live_tup AS row_count
FROM pg_stat_user_tables
WHERE schemaname = 'public'
ORDER BY relname;
