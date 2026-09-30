# ABC Hub Operational Database — Mini Project 01

This is the operational (source) database for ABC Hub, the entertainment
platform used throughout Mini Project 01: a combined streaming and
physical media rental service. Use this data as the source for Question 8
(the Apache NiFi ETL pipeline into PostgreSQL).

## Contents

- `ddl/operational_schema.sql` — run this against an empty PostgreSQL
  database to create all 26 tables, with primary keys, foreign keys, and a
  few helpful indexes.
- `data/*.csv` — 26 CSV files, 98,700 rows total, column order matching the
  schema exactly.

## Update: audit columns added

Every table now includes `created_at` and `updated_at` timestamp columns.
These reflect when each record was created and last touched in the
system, following the standard audit-column pattern used across the
program's datasets. If you already loaded an earlier version of this
data, re-download and reload from this package — every other column and
value is unchanged, only the two new columns were added.

## Loading the database

1. Create a database:
   ```
   createdb abc_hub_operational
   ```
2. Create the schema:
   ```
   psql -d abc_hub_operational -f ddl/operational_schema.sql
   ```
3. Load each table in this order (parent tables before child tables, so
   foreign keys resolve correctly):

   ```
   country, city, content_type, genre, artist, device, payment_method,
   support_category, courier, warehouse, subscription_plan,
   customer, customer_address, customer_subscription,
   content, content_genre, content_artist,
   inventory_item, streaming_session, rental, delivery,
   payment, review, wishlist, support_ticket, recommendation
   ```

   For each table:
   ```
   psql -d abc_hub_operational -c "\copy <table_name> FROM 'data/<table_name>.csv' WITH (FORMAT csv, HEADER true)"
   ```

## A note on data quality

This is real-world-style operational data: most of it is clean, but you will
encounter some missing optional fields, inconsistent text casing, a handful
of duplicate-looking records, and a few values that break expected business
rules (for example, a watch completion percentage slightly above 100%, or a
late fee that is negative). Part of Question 8, Task 2 is to detect and
handle exactly this kind of issue in your NiFi transformation stage — treat
it as you would real operational data, not as a mistake in the dataset.

The new `created_at` / `updated_at` columns are **not** part of that
exercise: they are complete and internally consistent on every row, and
`updated_at` is never earlier than `created_at`.

## Assumptions baked into this dataset

- The data covers roughly 12 months of operational history.
- Content types are split into streamable (Movie, TV Series, Documentary,
  Music) and rentable (Movie, Music, Book, Video Game); Movies and Music
  appear in both streaming and physical-rental form, matching the scenario.
- Subscription payments are generated monthly for each active subscription
  period; rental payments are generated once per rental.

## Scale

| Table | Rows | Table | Rows |
|---|---|---|---|
| country | 15 | inventory_item | 1,785 |
| city | 44 | streaming_session | 48,060 |
| content_type | 6 | rental | 6,500 |
| genre | 12 | delivery | 6,500 |
| artist | 250 | payment | 16,892 |
| device | 6 | review | 3,500 |
| payment_method | 5 | wishlist | 2,800 |
| support_category | 8 | support_ticket | 1,400 |
| courier | 5 | recommendation | 4,500 |
| warehouse | 10 | customer | 1,008 |
| subscription_plan | 4 | customer_address | 1,112 |
| content | 600 | customer_subscription | 1,215 |
| content_genre | 1,032 | | |
| content_artist | 1,431 | | |

**Total: 98,700 rows.**

## Revenue attribution note (relevant to Question 7, Scenario B)

`payment` rows are typed `Subscription` or `Rental`. Subscription payments
are not tied to a single content item (a subscription covers unlimited
streaming), so a reasonable approach for "Content Monthly Performance"
revenue is to use rental payments as the direct revenue driver per
title, and treat streaming as a usage/engagement metric rather than a
directly attributed revenue figure (or apply a simple per-stream
licensing-cost assumption if you want a revenue proxy for streamed
content — state this as an assumption in your report, as the brief
itself expects).
