-- sql/snowflake_setup.sql
-- Run once in your Snowflake account as a user with CREATE TABLE privileges
-- on the target database/schema.
--
-- Creates 4 tables for the example "customers" + "customer_devices" schema:
--   CUSTOMERS                ← current state of public.customers
--   CUSTOMERS_HISTORY        ← full audit trail of all CDC events
--   CUSTOMER_DEVICES         ← current state of public.customer_devices
--   CUSTOMER_DEVICES_HISTORY ← full audit trail
--
-- Column naming: PG camelCase → Snowflake UPPER_SNAKE_CASE (see _pg_col_to_sf
-- in src/snowflake_sink.py). JSONB columns → VARIANT.
-- Clustering: current-state tables by their partition/lookup key;
--             history tables by _CDC_TS (time-series access pattern).
-- ──────────────────────────────────────────────────────────────────────────

USE DATABASE ANALYTICS_DEV;
USE SCHEMA PUBLIC;

-- ─────────────────────────────────────────────────────────────────────────
-- 1. CUSTOMERS  (current state, one row per customer)
-- ─────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS ANALYTICS_DEV.PUBLIC.CUSTOMERS (
    ID           NUMBER(20,0)  NOT NULL,
    TENANT_ID    VARCHAR       NOT NULL,
    EMAIL        VARCHAR,
    STATUS       VARCHAR,
    PLAN_ID      VARCHAR,
    METADATA     VARIANT,                          -- JSONB
    TAGS         VARIANT,                          -- JSONB
    CREATED_AT   TIMESTAMP_NTZ,
    UPDATED_AT   TIMESTAMP_NTZ,
    _LOADED_AT   TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)
CLUSTER BY (TENANT_ID);

-- ─────────────────────────────────────────────────────────────────────────
-- 2. CUSTOMERS_HISTORY  (append-only audit trail)
--
--  _CDC_OP   I = INSERT,  U = UPDATE,  D = DELETE
--  _CDC_LSN  PostgreSQL WAL Log Sequence Number — globally unique, monotonic
--  _CDC_TS   Transaction commit timestamp in PostgreSQL (UTC)
-- ─────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS ANALYTICS_DEV.PUBLIC.CUSTOMERS_HISTORY (
    ID           NUMBER(20,0),
    TENANT_ID    VARCHAR,
    EMAIL        VARCHAR,
    STATUS       VARCHAR,
    PLAN_ID      VARCHAR,
    METADATA     VARIANT,
    TAGS         VARIANT,
    CREATED_AT   TIMESTAMP_NTZ,
    UPDATED_AT   TIMESTAMP_NTZ,
    -- CDC audit columns
    _CDC_OP      CHAR(1)       NOT NULL,
    _CDC_LSN     NUMBER(20,0)  NOT NULL,
    _CDC_TS      TIMESTAMP_NTZ NOT NULL,
    _LOADED_AT   TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)
CLUSTER BY (_CDC_TS);

-- ─────────────────────────────────────────────────────────────────────────
-- 3. CUSTOMER_DEVICES  (current state — one-to-many child of CUSTOMERS)
-- ─────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS ANALYTICS_DEV.PUBLIC.CUSTOMER_DEVICES (
    ID           NUMBER(20,0)  NOT NULL,
    CUSTOMER_ID  NUMBER(20,0)  NOT NULL,
    TENANT_ID    VARCHAR       NOT NULL,
    DEVICE_ID    VARCHAR       NOT NULL,
    LABEL        VARCHAR,
    CREATED_AT   TIMESTAMP_NTZ,
    _LOADED_AT   TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)
CLUSTER BY (ID);

-- ─────────────────────────────────────────────────────────────────────────
-- 4. CUSTOMER_DEVICES_HISTORY  (append-only audit trail)
-- ─────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS ANALYTICS_DEV.PUBLIC.CUSTOMER_DEVICES_HISTORY (
    ID           NUMBER(20,0),
    CUSTOMER_ID  NUMBER(20,0),
    TENANT_ID    VARCHAR,
    DEVICE_ID    VARCHAR,
    LABEL        VARCHAR,
    CREATED_AT   TIMESTAMP_NTZ,
    _CDC_OP      CHAR(1)       NOT NULL,
    _CDC_LSN     NUMBER(20,0)  NOT NULL,
    _CDC_TS      TIMESTAMP_NTZ NOT NULL,
    _LOADED_AT   TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)
CLUSTER BY (_CDC_TS);

-- ─────────────────────────────────────────────────────────────────────────
-- 5. Grants — run as ACCOUNTADMIN or SYSADMIN.
--    Replace <ETL_ROLE> with the role your CDC service authenticates as.
-- ─────────────────────────────────────────────────────────────────────────
-- GRANT USAGE ON DATABASE ANALYTICS_DEV                       TO ROLE <ETL_ROLE>;
-- GRANT USAGE ON SCHEMA   ANALYTICS_DEV.PUBLIC                TO ROLE <ETL_ROLE>;
-- GRANT SELECT, INSERT, UPDATE, DELETE
--   ON TABLE ANALYTICS_DEV.PUBLIC.CUSTOMERS                   TO ROLE <ETL_ROLE>;
-- GRANT SELECT, INSERT
--   ON TABLE ANALYTICS_DEV.PUBLIC.CUSTOMERS_HISTORY           TO ROLE <ETL_ROLE>;
-- GRANT SELECT, INSERT, UPDATE, DELETE
--   ON TABLE ANALYTICS_DEV.PUBLIC.CUSTOMER_DEVICES            TO ROLE <ETL_ROLE>;
-- GRANT SELECT, INSERT
--   ON TABLE ANALYTICS_DEV.PUBLIC.CUSTOMER_DEVICES_HISTORY    TO ROLE <ETL_ROLE>;

-- ─────────────────────────────────────────────────────────────────────────
-- 6. Staleness monitoring query (run ad-hoc, or wrap in a Snowflake Task)
-- ─────────────────────────────────────────────────────────────────────────
-- SELECT
--   MAX(_CDC_TS)                                              AS latest_pg_change,
--   DATEDIFF('minute', MAX(_CDC_TS), CURRENT_TIMESTAMP())      AS lag_minutes,
--   COUNT(*)                                                   AS total_history_rows
-- FROM ANALYTICS_DEV.PUBLIC.CUSTOMERS_HISTORY;
