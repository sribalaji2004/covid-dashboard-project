-- ============================================================
-- SCRIPT 1: SCHEMA SETUP & RAW DATA LOADING
-- COVID-19 End-to-End Analytics Project
-- Compatible with: PostgreSQL 13+ / MySQL 8+ / SQLite 3.35+
-- ============================================================

-- ── DROP TABLES (clean slate) ───────────────────────────────
DROP TABLE IF EXISTS covid_clean;
DROP TABLE IF EXISTS vaccination_clean;
DROP TABLE IF EXISTS country_ref;
DROP TABLE IF EXISTS covid_raw;
DROP TABLE IF EXISTS vaccination_raw;

-- ── RAW STAGING TABLES ──────────────────────────────────────
-- These mirror the CSV columns exactly — no transformation yet

CREATE TABLE covid_raw (
    date              TEXT,
    country           TEXT,
    province_state    TEXT,
    confirmed         TEXT,   -- stored as TEXT to catch dirty values
    deaths            TEXT,
    recovered         TEXT,
    active            TEXT,
    new_cases         TEXT,
    new_deaths        TEXT,
    population        TEXT,
    continent         TEXT
);

CREATE TABLE vaccination_raw (
    date                      TEXT,
    country                   TEXT,
    total_vaccinations        TEXT,
    people_vaccinated         TEXT,
    people_fully_vaccinated   TEXT,
    boosters                  TEXT,
    daily_vaccinations        TEXT,
    vaccines_used             TEXT
);

CREATE TABLE country_ref (
    country                 TEXT PRIMARY KEY,
    continent               TEXT,
    region                  TEXT,
    income_group            TEXT,
    population_2020         INTEGER,
    population_density      REAL,
    median_age              REAL,
    gdp_per_capita_usd      REAL,
    healthcare_index        REAL
);

-- ── LOAD RAW DATA ────────────────────────────────────────────
-- PostgreSQL:
--   \COPY covid_raw FROM 'data/covid_raw.csv' WITH (FORMAT csv, HEADER true, NULL '');
--   \COPY vaccination_raw FROM 'data/vaccination_data.csv' WITH (FORMAT csv, HEADER true, NULL '');
--   \COPY country_ref FROM 'data/country_reference.csv' WITH (FORMAT csv, HEADER true, NULL '');

-- SQLite (via CLI):
--   .mode csv
--   .headers on
--   .import data/covid_raw.csv covid_raw
--   .import data/vaccination_data.csv vaccination_raw
--   .import data/country_reference.csv country_ref

-- MySQL (using LOAD DATA — adjust path):
--   LOAD DATA LOCAL INFILE 'data/covid_raw.csv'
--     INTO TABLE covid_raw
--     FIELDS TERMINATED BY ',' ENCLOSED BY '"'
--     LINES TERMINATED BY '\n'
--     IGNORE 1 ROWS;

-- ── QUICK SANITY CHECK ───────────────────────────────────────
SELECT 'covid_raw'       AS tbl, COUNT(*) AS row_count FROM covid_raw
UNION ALL
SELECT 'vaccination_raw' AS tbl, COUNT(*) AS row_count FROM vaccination_raw
UNION ALL
SELECT 'country_ref'     AS tbl, COUNT(*) AS row_count FROM country_ref;

-- Expected output:
--   covid_raw        | ~160 rows
--   vaccination_raw  | ~45  rows
--   country_ref      | 20   rows
