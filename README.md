# ABC Hub: Modern Data Platform (Z2D DWDF Mini Project 01)

An end-to-end analytical platform for **ABC Hub**, an entertainment company that offers both digital streaming and physical media rentals. An **Apache NiFi** ETL pipeline moves data from the operational PostgreSQL database into an analytical PostgreSQL database organised in **bronze, silver and gold** layers, where three star schemas answer the business questions of the project brief:

* **Customer Daily Activity**: one row per customer per day
* **Content Monthly Performance**: one row per content item per month
* **Inventory Utilisation**: one row per physical inventory item per day

The full answers to Questions 1 to 8 (architecture, data modelling, star schemas and the NiFi implementation) are in [`REPORT.pdf`](REPORT.pdf).

---

## 1. Project structure

```
.
├── README.md                              this file
├── REPORT.pdf                             report answering Questions 1-8
├── ZuuCrew-Project-Submission-Form_Mini Project 01.pdf
│
├── ABC Hub Data Package/packages/         provided source system
│   ├── ddl/operational_schema.sql         operational schema (26 tables)
│   ├── data/*.csv                         26 CSV files, 98,700 rows
│   └── README.md                          data package notes
│
├── sql/
│   ├── 00_setup/
│   │   ├── 01_create_databases.sql        creates abc_hub_operational + abc_hub_analytics
│   │   └── 02_load_operational_data.sql   loads the 26 CSVs into the operational database
│   ├── 01_profiling/
│   │   └── data_quality_profiling.sql     data-quality scorecard of the source data
│   ├── 02_analytics/                      analytical database, run in file order
│   │   ├── 01_schemas_and_etl_control.sql schemas + ETL control tables and functions
│   │   ├── 02_bronze_tables.sql           bronze: raw, insert-only history (20 tables)
│   │   ├── 03_silver_tables.sql           silver: cleansed current state (20 tables)
│   │   ├── 04_gold_dimensions.sql         gold: 7 dimensions (SCD Type 1 and Type 2)
│   │   ├── 05_gold_facts.sql              gold: 3 fact tables
│   │   ├── 06_gold_load_dimensions.sql    change detection + dimension load functions
│   │   ├── 07_gold_load_facts.sql         incremental, idempotent fact load functions
│   │   ├── 08_gold_views.sql              analyst views and KPI view
│   │   └── 09_security_roles.sql          role-based access control
│   ├── 03_tests/
│   │   ├── 01_data_validation_tests.sql   44 PASS/FAIL checks
│   │   ├── 02_fact_checksums.sql          fingerprints used by the idempotency test
│   │   ├── 03_simulate_source_changes.sql simulates one new business day in the source
│   │   └── 04_incremental_checks.sql      verifies the incremental load (22 checks)
│   └── 04_sample_queries/
│       └── analytical_queries.sql         business questions answered from gold
│
├── nifi/
│   ├── ABC_Hub_ETL_Pipeline.json          exported NiFi flow definition (import this)
│   ├── build_flow.py                      builds the flow in NiFi through the REST API and exports it
│   ├── flow_config.py                     per-table metadata: columns, mandatory fields, cleansing rules
│   ├── nifi_api.py                        small NiFi REST client used by the scripts
│   └── run_pipeline.py                    runs the pipeline on demand ("Run Once")
│
├── scripts/
│   ├── setup_databases.sh                 builds both databases in one command
│   ├── run_tests.sh                       runs the 44 validation checks
│   ├── test_idempotency.sh                proves repeated runs change nothing
│   ├── run_full_demo.sh                   everything from scratch: databases, flow, load, tests, screenshots
│   ├── capture_evidence.sh                runs the pipeline and captures evidence screenshots (into docs/screenshots/)
│   └── capture_screenshots.mjs            headless-Chrome capture of the NiFi UI
│
└── screenshots/                           NiFi workflow, successful run, populated tables, query results
```

### Analytical database layout (`abc_hub_analytics`)

| Schema | Purpose |
|---|---|
| `etl` | Control plane: pipeline runs, step log, load audit, rejected records, watermarks |
| `bronze` | Raw copy of every extracted row version (insert-only, for audit and replay) |
| `silver` | Validated, cleansed and de-duplicated data, one row per source key |
| `gold` | Star schemas (dimensions and facts) plus analyst views |

## 2. Software versions used

| Software | Version | Used for |
|---|---|---|
| Windows | 11 | development machine |
| PostgreSQL | 18.4 | operational and analytical databases (any PostgreSQL 14+ should work) |
| Apache NiFi | 2.12.0 | ETL pipeline (the flow imports into NiFi 2.x) |
| Java (Eclipse Temurin JDK) | 21.0.11 | required by NiFi 2.x |
| PostgreSQL JDBC driver | 42.7.13 | NiFi database connections |
| Python | 3.11.9 | flow builder and runner scripts (standard library only) |
| Git Bash | 2.52 | runs the `.sh` scripts on Windows |
| Node.js | 24.16 | only for capturing screenshots (no packages needed) |
| Google Chrome | current | only for capturing screenshots |

## 3. Installation steps

1. **Install PostgreSQL** and make sure `psql` is on your `PATH`.
2. **Install Java 21** (NiFi 2.x needs it).
3. **Download Apache NiFi 2.x** (`nifi-2.x.y-bin.zip`) from <https://nifi.apache.org/download/>, check its SHA-512 checksum and unzip it.
4. **Download the PostgreSQL JDBC driver** (`postgresql-42.7.x.jar`) from Maven Central and note its full path. You will need it in step 3 of section 5.
5. **Choose how to sign in to NiFi:**
   * Default: NiFi starts on HTTPS with a generated single-user login. The username and password are printed in `logs/nifi-app.log` on the first start.
   * Local development only: in `conf/nifi.properties` set `nifi.web.http.host=127.0.0.1` and `nifi.web.http.port=8080`, and leave the `nifi.web.https.*`, `nifi.security.keystore*`, `nifi.security.truststore*` and `nifi.security.user.*` properties empty. NiFi then runs on plain HTTP, reachable only from your own computer. The helper scripts in `nifi/` expect this setup (`http://127.0.0.1:8080`).
6. **Start NiFi** from its folder:

```bash
bin/nifi.cmd start
```

   (use `bin/nifi.sh start` on Linux/macOS), then open <http://127.0.0.1:8080/nifi>. The first start takes one to two minutes.

## 4. Database configuration

### Password (never stored in the project)

Create a **pgpass** file so that `psql`, the scripts and the NiFi JDBC driver can connect without a password in any file of this project:

* Windows: `%APPDATA%\postgresql\pgpass.conf`
* Linux/macOS: `~/.pgpass` (then run `chmod 600 ~/.pgpass`)

It contains one line:

```
localhost:5432:*:postgres:<your password>
```

### Create and load the databases

From the project folder (Git Bash on Windows):

```bash
./scripts/setup_databases.sh
```

This takes about 5 seconds and:

1. creates `abc_hub_operational` and `abc_hub_analytics`;
2. creates the operational schema and loads the 26 CSV files (98,700 rows);
3. creates the `etl`, `bronze`, `silver` and `gold` schemas, tables, load functions, views and roles in `abc_hub_analytics`.

Other options:

```bash
./scripts/setup_databases.sh --analytics-only
```

```bash
./scripts/setup_databases.sh --reset-analytics
```

`--analytics-only` re-applies the analytical scripts; `--reset-analytics` drops and recreates the analytical database (rebuild the NiFi flow afterwards so its extract watermarks reset too).

### Connection settings

| Setting | Default | Change with |
|---|---|---|
| Host / port | `localhost:5432` | `PGHOST`, `PGPORT` environment variables |
| User | `postgres` | `PGUSER` environment variable |
| Source database | `abc_hub_operational` | NiFi parameter `source.jdbc.url` |
| Target database | `abc_hub_analytics` | NiFi parameter `target.jdbc.url` |

### Access roles

`sql/02_analytics/09_security_roles.sql` creates four group roles without login rights:

| Role | Access |
|---|---|
| `abc_etl` | read/write on all layers (the pipeline) |
| `abc_analyst` | gold analyst views only, no personal data |
| `abc_data_scientist` | silver and gold |
| `abc_auditor` | read-only on everything, including bronze and the ETL audit tables |

Create your own login users (with your own passwords) and grant them one of these roles.

## 5. Executing the NiFi workflow

### Option A: import the flow in the NiFi UI

1. On the NiFi canvas, drag a **Process Group** onto the canvas, choose **Import from file** (the upload icon) and select `nifi/ABC_Hub_ETL_Pipeline.json`.
2. Open the top-right menu (☰), choose **Parameter Contexts**, edit **ABC Hub ETL Parameters** and check the values:

   | Parameter | Value |
   |---|---|
   | `jdbc.driver.path` | full path of `postgresql-42.7.x.jar` on your machine |
   | `db.user` | `postgres` (or your ETL user) |
   | `db.password` | your password, or leave it **empty** to use the pgpass file |
   | `source.jdbc.url` | `jdbc:postgresql://localhost:5432/abc_hub_operational` |
   | `target.jdbc.url` | `jdbc:postgresql://localhost:5432/abc_hub_analytics` |
   | `log.directory` | folder for rejected and failed records (default `./logs/abc_hub_etl` inside the NiFi folder) |
   | `gold.full.refresh` | `false` (incremental) or `true` (rebuild all gold facts) |

3. Right-click the group and choose **Enable all controller services**.
4. Right-click the group and choose **Start**.
5. The pipeline then runs on its schedule:

   | Processors | Schedule |
   |---|---|
   | Reference data extracts (country, city, plans, genres, ...) | daily at 00:30 |
   | Transactional extracts (customers, streams, rentals, payments, ...) | every hour at :05 |
   | Gold build (dimensions, then facts) | every hour at :30 |

   To run it immediately: open group **01 Extract**, right-click each `Extract ... (incremental)` processor and choose **Run Once**. When the queues are empty, open group **05 Build Gold**, right-click **Trigger Gold Build** and choose **Run Once**.

### Option B: scripted (NiFi on `http://127.0.0.1:8080`)

Build the flow in NiFi, start it and re-export the flow definition:

```bash
python nifi/build_flow.py
```

Run the whole pipeline once (extract, bronze, silver, then gold):

```bash
python nifi/run_pipeline.py
```

Or do everything from scratch in one command (databases, flow, initial load, tests, screenshots):

```bash
./scripts/run_full_demo.sh
```

### What the workflow does

| Process group | What happens |
|---|---|
| 01 Extract | `QueryDatabaseTableRecord` per table reads only rows whose `updated_at` is newer than the stored watermark |
| 02 Land Raw (Bronze) | every extracted row version is appended to `bronze.<table>` with its batch id |
| 03 Validate & Cleanse | `ValidateRecord` rejects records missing mandatory fields; `QueryRecord` standardises, fixes and de-duplicates |
| 04 Load Silver | cleansed records are upserted into `silver.<table>` |
| 05 Build Gold | PostgreSQL functions load 5 dimensions first, then 3 facts, only for data that changed |
| 06 Error Handling | rejected records go to JSON files and `etl.rejected_record`; failures go to files and the NiFi error log |

### Checking the result

```bash
./scripts/run_tests.sh
```

```bash
./scripts/test_idempotency.sh
```

```bash
psql -d abc_hub_analytics -f sql/04_sample_queries/analytical_queries.sql
```

Useful tables: `etl.pipeline_run` (run history), `etl.run_step_log` (rows per step), `etl.load_audit` (batches per layer) and `etl.rejected_record` (rejected records with the reason).

To test incremental loading, simulate a new business day in the source and run the pipeline again:

```bash
psql -d abc_hub_operational -f sql/03_tests/03_simulate_source_changes.sql
```

```bash
python nifi/run_pipeline.py
```

```bash
psql -d abc_hub_analytics -f sql/03_tests/04_incremental_checks.sql
```

### Results from my run

| Measure | Value |
|---|---|
| Operational source rows | 98,700 |
| Bronze rows (20 tables) | 87,676 |
| Silver rows | 87,583 (93 duplicates rejected and logged) |
| `fact_customer_daily_activity` | 68,337 rows |
| `fact_content_monthly_performance` | 7,099 rows |
| `fact_inventory_daily_snapshot` | 637,674 rows |
| Initial load through NiFi | about 47 seconds (12 s to silver, 34 s gold build) |
| Validation checks | 44 of 44 pass |
| Repeated run | 0 rows changed |

## 6. Assumptions made during implementation

**Data and loading**

1. `updated_at` reliably marks every change in the source (guaranteed by the data package), so it is used as the incremental watermark.
2. Only the 20 tables needed by the Question 7 models are extracted. `device`, `payment_method`, `support_category`, `courier`, `delivery` and `recommendation` are out of scope and can be added with one entry in `nifi/flow_config.py`.
3. Integer columns exported as `"1.0"` in the CSVs are loaded through a text staging area and cast, without changing any value.

**Data-quality rules** (each one is backed by the profiling in `sql/01_profiling/`)

4. Duplicates: the same e-mail for customers, the same customer/content/device/start time for streams, and the same customer/type/amount/time/reference for payments. The record with the lowest id is kept; the others are rejected and logged.
5. Text is standardised (for example `ACTIVE`, ` Active ` and `active` become `Active`), e-mails are lower-cased, and a missing gender or language becomes `Unknown`.
6. A missing watch duration is calculated from end time minus start time; completion above 100% is capped at 100.
7. Negative payment amounts are sign errors, so `ABS()` is used. A negative late fee becomes `ABS()` when the item was returned late or is overdue, and 0 otherwise.
8. A rating of 0 (the scale is 1 to 5) is set to empty and left out of averages; a ticket closed before it was opened gets an empty close date.
9. Missing durations for Books and Video Games are correct (not applicable) and are kept empty.

**Business definitions**

10. Revenue means completed payments; refunds and failed payments are reported separately.
11. Revenue per title = rental payments for that title + the month's subscription revenue shared by each title's share of streaming minutes (subscriptions are not tied to one title).
12. A completed stream is one watched to at least 90%.
13. Inventory utilisation = days rented divided by days in service.

**Modelling**

14. Customer status and home location keep history (SCD Type 2); name, e-mail and phone are overwritten (Type 1). The same applies to inventory status, condition and warehouse. The first version of every record starts on 1900-01-01 because the source has no earlier history.
15. The primary genre is the genre with the lowest id, and the primary artist is chosen by role (Director, then Lead Actor, Performer, Author and so on), because the source has no "primary" flag.
16. A physical copy is in service from its purchase date or its first rental, whichever is earlier (338 rentals happen before the recorded purchase date). A retired copy leaves service after its last return.
17. Overlapping rentals of the same copy (a source data issue) count as one rented day, so utilisation never exceeds 100%.
18. The inventory snapshot covers the business dates present in the data (June 2025 to June 2026); June 2026 is a partial month.

**Environment**

19. The development NiFi ran on local-only HTTP (127.0.0.1) and read the database password from the pgpass file, so no credential is stored in the flow definition or in this repository.
