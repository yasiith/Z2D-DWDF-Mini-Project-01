-- ============================================================================
-- 01_schemas_and_etl_control.sql            (database: abc_hub_analytics)
--
-- Medallion layout:
--   etl     control plane: pipeline runs, step log, load audit, rejected
--           records, watermarks
--   bronze  raw, insert-only history of every extracted row version
--   silver  cleansed, conformed, current-state copy of each source entity
--   gold    business-facing star schemas (Question 7)
--
-- Idempotent: safe to re-run.
-- ============================================================================

CREATE SCHEMA IF NOT EXISTS etl;
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

COMMENT ON SCHEMA etl    IS 'ETL control plane: run log, step log, load audit, rejected records, watermarks';
COMMENT ON SCHEMA bronze IS 'Raw landing zone: insert-only history of every extracted source row version';
COMMENT ON SCHEMA silver IS 'Cleansed and conformed current-state entities (one row per source key)';
COMMENT ON SCHEMA gold   IS 'Business-facing dimensional model: dimensions, facts and analyst views';

-- ----------------------------------------------------------------------------
-- Pipeline runs: one row per execution of a pipeline (e.g. the gold build).
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS etl.pipeline_run (
    run_id          BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    pipeline_name   VARCHAR(100) NOT NULL,
    triggered_by    VARCHAR(100) NOT NULL DEFAULT 'nifi',
    full_refresh    BOOLEAN      NOT NULL DEFAULT FALSE,
    status          VARCHAR(20)  NOT NULL DEFAULT 'RUNNING'
                    CHECK (status IN ('RUNNING', 'SUCCEEDED', 'FAILED')),
    started_at      TIMESTAMP    NOT NULL DEFAULT clock_timestamp(),
    finished_at     TIMESTAMP,
    error_message   TEXT
);

-- One row per step inside a run (dimension load, fact load, ...).
CREATE TABLE IF NOT EXISTS etl.run_step_log (
    step_log_id     BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    run_id          BIGINT       NOT NULL REFERENCES etl.pipeline_run (run_id),
    step_name       VARCHAR(100) NOT NULL,
    rows_inserted   BIGINT       NOT NULL DEFAULT 0,
    rows_updated    BIGINT       NOT NULL DEFAULT 0,
    rows_deleted    BIGINT       NOT NULL DEFAULT 0,
    started_at      TIMESTAMP    NOT NULL,
    finished_at     TIMESTAMP    NOT NULL DEFAULT clock_timestamp(),
    message         TEXT
);

-- Load audit written by NiFi after every successful bronze / silver load.
CREATE TABLE IF NOT EXISTS etl.load_audit (
    audit_id        BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    batch_id        VARCHAR(64)  NOT NULL,
    source_table    VARCHAR(100) NOT NULL,
    target_layer    VARCHAR(20)  NOT NULL CHECK (target_layer IN ('bronze', 'silver')),
    record_count    BIGINT       NOT NULL,
    flowfile_uuid   VARCHAR(64),
    loaded_at       TIMESTAMP    NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX IF NOT EXISTS ix_load_audit_table ON etl.load_audit (source_table, loaded_at);

-- Records NiFi refused to load: failed validation, in-batch duplicates,
-- or processing errors. One row per distinct rejected source record, payload
-- kept as JSON so it can be inspected and replayed. If the same record is
-- rejected again (e.g. a replayed extract), occurrence_count is increased
-- instead of adding a duplicate row - the audit log is idempotent too.
CREATE TABLE IF NOT EXISTS etl.rejected_record (
    reject_id        BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    batch_id         VARCHAR(64),
    source_table     VARCHAR(100) NOT NULL,
    reject_reason    VARCHAR(30)  NOT NULL
                     CHECK (reject_reason IN ('VALIDATION', 'DUPLICATE', 'PROCESSING_ERROR')),
    reject_details   TEXT,
    record_payload   JSONB        NOT NULL,
    payload_hash     CHAR(32)     GENERATED ALWAYS AS (md5(record_payload::TEXT)) STORED,
    flowfile_uuid    VARCHAR(64),
    rejected_at      TIMESTAMP    NOT NULL DEFAULT clock_timestamp(),
    occurrence_count INT          NOT NULL DEFAULT 1,
    last_rejected_at TIMESTAMP    NOT NULL DEFAULT clock_timestamp(),
    last_batch_id    VARCHAR(64)
);
CREATE INDEX IF NOT EXISTS ix_rejected_record_table ON etl.rejected_record (source_table, reject_reason);
CREATE UNIQUE INDEX IF NOT EXISTS ux_rejected_record_once
    ON etl.rejected_record (source_table, reject_reason, payload_hash);

-- High-water marks for SQL-side incremental processing (silver -> gold).
-- (NiFi keeps its own per-table extraction state for operational -> bronze.)
CREATE TABLE IF NOT EXISTS etl.watermark (
    watermark_name  VARCHAR(100) PRIMARY KEY,
    watermark_value TIMESTAMP    NOT NULL,
    updated_at      TIMESTAMP    NOT NULL DEFAULT clock_timestamp()
);

-- ----------------------------------------------------------------------------
-- Run-management functions (called from NiFi with ExecuteSQLRecord)
-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION etl.start_run(p_pipeline VARCHAR, p_full_refresh BOOLEAN DEFAULT FALSE,
                                         p_triggered_by VARCHAR DEFAULT 'nifi')
RETURNS TABLE (run_id BIGINT, full_refresh BOOLEAN)
LANGUAGE plpgsql AS $$
BEGIN
    RETURN QUERY
    INSERT INTO etl.pipeline_run (pipeline_name, full_refresh, triggered_by)
    VALUES (p_pipeline, p_full_refresh, p_triggered_by)
    RETURNING pipeline_run.run_id, pipeline_run.full_refresh;
END $$;

CREATE OR REPLACE FUNCTION etl.finish_run(p_run_id BIGINT)
RETURNS TABLE (run_id BIGINT, status VARCHAR, duration_seconds NUMERIC, steps BIGINT)
LANGUAGE plpgsql AS $$
BEGIN
    UPDATE etl.pipeline_run r
       SET status = 'SUCCEEDED', finished_at = clock_timestamp()
     WHERE r.run_id = p_run_id;
    RETURN QUERY
    SELECT r.run_id, r.status,
           round(extract(epoch FROM r.finished_at - r.started_at)::numeric, 2),
           (SELECT count(*) FROM etl.run_step_log s WHERE s.run_id = r.run_id)
      FROM etl.pipeline_run r WHERE r.run_id = p_run_id;
END $$;

CREATE OR REPLACE FUNCTION etl.fail_run(p_run_id BIGINT, p_error TEXT)
RETURNS TABLE (run_id BIGINT, status VARCHAR)
LANGUAGE plpgsql AS $$
BEGIN
    RETURN QUERY
    UPDATE etl.pipeline_run r
       SET status = 'FAILED', finished_at = clock_timestamp(), error_message = p_error
     WHERE r.run_id = p_run_id
    RETURNING r.run_id, r.status;
END $$;

CREATE OR REPLACE FUNCTION etl.log_step(p_run_id BIGINT, p_step VARCHAR, p_started TIMESTAMP,
                                        p_ins BIGINT, p_upd BIGINT, p_del BIGINT, p_msg TEXT DEFAULT NULL)
RETURNS VOID
LANGUAGE sql AS $$
    INSERT INTO etl.run_step_log (run_id, step_name, started_at, rows_inserted, rows_updated, rows_deleted, message)
    VALUES (p_run_id, p_step, p_started, p_ins, p_upd, p_del, p_msg);
$$;

-- Records NiFi rejects: expands a JSON array of records into one row each
-- (a record already logged only gets its occurrence counter increased).
CREATE OR REPLACE FUNCTION etl.log_rejected_records(p_batch_id VARCHAR, p_table VARCHAR, p_reason VARCHAR,
                                                    p_details TEXT, p_payload JSONB, p_flowfile VARCHAR)
RETURNS TABLE (rejected_count BIGINT)
LANGUAGE plpgsql AS $$
DECLARE n BIGINT;
BEGIN
    INSERT INTO etl.rejected_record AS r (batch_id, source_table, reject_reason, reject_details, record_payload,
                                          flowfile_uuid, last_batch_id)
    SELECT p_batch_id, p_table, p_reason, p_details, rec, p_flowfile, p_batch_id
      FROM jsonb_array_elements(CASE jsonb_typeof(p_payload) WHEN 'array' THEN p_payload
                                     ELSE jsonb_build_array(p_payload) END) AS rec
    ON CONFLICT (source_table, reject_reason, payload_hash) DO UPDATE
       SET occurrence_count = r.occurrence_count + 1,
           last_rejected_at = clock_timestamp(),
           last_batch_id    = EXCLUDED.last_batch_id;
    GET DIAGNOSTICS n = ROW_COUNT;
    RETURN QUERY SELECT n;
END $$;

-- Shared trigger: stamps silver rows with the time their DATA last changed.
-- The gold build uses _loaded_at to find what changed since its last run.
-- A re-delivered but identical row (e.g. a replayed NiFi batch) keeps its
-- original stamp and batch id, so repeated executions cause no gold work.
CREATE OR REPLACE FUNCTION etl.set_loaded_at()
RETURNS TRIGGER
LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'UPDATE'
       AND (to_jsonb(NEW) - '_loaded_at' - '_batch_id')
         = (to_jsonb(OLD) - '_loaded_at' - '_batch_id') THEN
        NEW._loaded_at := OLD._loaded_at;
        NEW._batch_id  := OLD._batch_id;
    ELSE
        NEW._loaded_at := clock_timestamp();
    END IF;
    RETURN NEW;
END $$;
