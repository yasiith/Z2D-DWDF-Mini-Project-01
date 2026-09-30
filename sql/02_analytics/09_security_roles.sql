-- ============================================================================
-- 09_security_roles.sql                     (database: abc_hub_analytics)
--
-- Role-based access control (least privilege). These are NOLOGIN group
-- roles: real users / service accounts are created separately (with their
-- own passwords, never stored in this repository) and granted membership:
--     CREATE ROLE jane LOGIN PASSWORD '...' IN ROLE abc_analyst;
--
--   abc_etl             pipeline service account: writes bronze/silver/gold/etl
--   abc_analyst         business analysts & BI tools: gold VIEWS only (no PII)
--   abc_data_scientist  data science: silver + gold (history for ML features)
--   abc_auditor         audit / compliance: read everything incl. bronze and
--                       the ETL audit trail, write nothing
-- ============================================================================

DO $$
DECLARE r TEXT;
BEGIN
    FOREACH r IN ARRAY ARRAY['abc_etl', 'abc_analyst', 'abc_data_scientist', 'abc_auditor'] LOOP
        IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = r) THEN
            EXECUTE format('CREATE ROLE %I NOLOGIN', r);
        END IF;
    END LOOP;
END $$;

-- Nobody gets anything by default.
REVOKE ALL ON SCHEMA etl, bronze, silver, gold FROM PUBLIC;

-- ETL service: full DML on all layers, may execute the load functions.
GRANT USAGE ON SCHEMA etl, bronze, silver, gold TO abc_etl;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA etl, bronze, silver, gold TO abc_etl;
GRANT USAGE ON ALL SEQUENCES IN SCHEMA etl, bronze, silver, gold TO abc_etl;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA etl TO abc_etl;

-- Analysts: the curated views only - no raw tables, no customer PII.
GRANT USAGE ON SCHEMA gold TO abc_analyst;
GRANT SELECT ON gold.vw_customer_daily_activity, gold.vw_content_monthly_performance,
                gold.vw_inventory_daily_utilisation, gold.vw_warehouse_utilisation_monthly,
                gold.vw_monthly_business_kpis
             TO abc_analyst;

-- Data scientists: cleansed history (silver) and the star schema (gold).
GRANT USAGE ON SCHEMA silver, gold TO abc_data_scientist;
GRANT SELECT ON ALL TABLES IN SCHEMA silver, gold TO abc_data_scientist;

-- Auditors: read-only on every layer plus the ETL audit trail.
GRANT USAGE ON SCHEMA etl, bronze, silver, gold TO abc_auditor;
GRANT SELECT ON ALL TABLES IN SCHEMA etl, bronze, silver, gold TO abc_auditor;

-- Tables created later by the owner inherit the same rules.
ALTER DEFAULT PRIVILEGES IN SCHEMA silver, gold GRANT SELECT ON TABLES TO abc_data_scientist;
ALTER DEFAULT PRIVILEGES IN SCHEMA etl, bronze, silver, gold GRANT SELECT ON TABLES TO abc_auditor;
ALTER DEFAULT PRIVILEGES IN SCHEMA etl, bronze, silver, gold
    GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO abc_etl;
