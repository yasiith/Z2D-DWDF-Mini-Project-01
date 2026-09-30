-- ============================================================================
-- 01_create_databases.sql
-- Creates the two PostgreSQL databases used by the project.
--
--   abc_hub_operational : the OLTP source system (provided DDL + CSVs)
--   abc_hub_analytics   : the analytical platform (bronze / silver / gold)
--
-- Run as a superuser (e.g. postgres) while connected to any database:
--   psql -U postgres -h localhost -d postgres -f sql/00_setup/01_create_databases.sql
--
-- Idempotent: \gexec only issues CREATE DATABASE when the database is missing.
-- ============================================================================

SELECT 'CREATE DATABASE abc_hub_operational ENCODING ''UTF8'''
WHERE NOT EXISTS (SELECT 1 FROM pg_database WHERE datname = 'abc_hub_operational')
\gexec

SELECT 'CREATE DATABASE abc_hub_analytics ENCODING ''UTF8'''
WHERE NOT EXISTS (SELECT 1 FROM pg_database WHERE datname = 'abc_hub_analytics')
\gexec

SELECT datname AS database_name
FROM pg_database
WHERE datname IN ('abc_hub_operational', 'abc_hub_analytics')
ORDER BY datname;
