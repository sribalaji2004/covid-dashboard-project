-- ============================================================
-- SCRIPT 2: DATA CLEANING & TRANSFORMATION
-- COVID-19 End-to-End Analytics Project
-- ============================================================

-- ── STEP 1: INSPECT RAW DATA QUALITY ────────────────────────

-- 1a. Check for NULL / empty critical fields
SELECT
    COUNT(*)                                           AS total_rows,
    SUM(CASE WHEN date      IS NULL OR date      = '' THEN 1 ELSE 0 END) AS null_date,
    SUM(CASE WHEN country   IS NULL OR country   = '' THEN 1 ELSE 0 END) AS null_country,
    SUM(CASE WHEN confirmed IS NULL OR confirmed = '' THEN 1 ELSE 0 END) AS null_confirmed,
    SUM(CASE WHEN deaths    IS NULL OR deaths    = '' THEN 1 ELSE 0 END) AS null_deaths,
    SUM(CASE WHEN continent IS NULL OR continent = '' THEN 1 ELSE 0 END) AS null_continent
FROM covid_raw;

-- 1b. Spot non-numeric values in numeric columns
SELECT date, country, confirmed, deaths
FROM covid_raw
WHERE confirmed NOT GLOB '[0-9]*'   -- SQLite syntax
   OR deaths    NOT GLOB '[0-9]*';  -- Replace with SIMILAR TO '\d+' in PostgreSQL

-- 1c. Check date format consistency
SELECT DISTINCT
    LENGTH(date) AS date_len,
    SUBSTR(date, 5, 1) AS separator,
    COUNT(*) AS cnt
FROM covid_raw
GROUP BY 1, 2;

-- 1d. Check for negative or impossible values
SELECT date, country, confirmed, deaths, new_cases, new_deaths
FROM covid_raw
WHERE CAST(confirmed AS INTEGER) < 0
   OR CAST(deaths    AS INTEGER) < 0
   OR CAST(deaths    AS INTEGER) > CAST(confirmed AS INTEGER);

-- 1e. Duplicate check (same date + country + province)
SELECT date, country, province_state, COUNT(*) AS dupes
FROM covid_raw
GROUP BY date, country, province_state
HAVING COUNT(*) > 1;


-- ── STEP 2: BUILD THE CLEAN TABLE ───────────────────────────

DROP TABLE IF EXISTS covid_clean;

CREATE TABLE covid_clean AS
WITH deduped AS (
    -- Remove exact duplicates, keep first occurrence
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY date, country, province_state
               ORDER BY confirmed DESC   -- keep row with higher confirmed if dupes differ
           ) AS rn
    FROM covid_raw
),
typed AS (
    -- Cast all columns to proper types and standardise date format
    SELECT
        -- Normalise date to YYYY-MM-DD
        CASE
            WHEN LENGTH(date) = 10 THEN date        -- already YYYY-MM-DD
            ELSE NULL                                -- flag unknown formats
        END AS report_date,

        TRIM(country)                                        AS country,
        CASE
            WHEN province_state IS NULL OR province_state = ''
            THEN 'All'
            ELSE TRIM(province_state)
        END                                                  AS province_state,

        -- Numeric fields: coerce empty strings → 0
        COALESCE(NULLIF(TRIM(confirmed), ''), '0')::INTEGER  AS confirmed,
        COALESCE(NULLIF(TRIM(deaths),    ''), '0')::INTEGER  AS deaths,
        COALESCE(NULLIF(TRIM(recovered), ''), '0')::INTEGER  AS recovered,
        COALESCE(NULLIF(TRIM(new_cases), ''), '0')::INTEGER  AS new_cases,
        COALESCE(NULLIF(TRIM(new_deaths),''), '0')::INTEGER  AS new_deaths,
        COALESCE(NULLIF(TRIM(population),''), '0')::BIGINT   AS population,

        TRIM(continent)                                      AS continent
    FROM deduped
    WHERE rn = 1
),
aggregated AS (
    -- Roll up province/state rows to country level (for countries split by region)
    SELECT
        report_date,
        country,
        continent,
        -- We keep province_state; Tableau can roll up dynamically
        province_state,
        SUM(confirmed)  AS confirmed,
        SUM(deaths)     AS deaths,
        SUM(recovered)  AS recovered,
        SUM(new_cases)  AS new_cases,
        SUM(new_deaths) AS new_deaths,
        MAX(population) AS population      -- population repeated per province; take MAX
    FROM typed
    WHERE report_date IS NOT NULL          -- drop rows with bad dates
    GROUP BY report_date, country, continent, province_state
)
SELECT
    a.*,
    -- Derived calculated columns
    CASE
        WHEN a.confirmed = 0 THEN 0.0
        ELSE ROUND(a.deaths * 100.0 / a.confirmed, 4)
    END                                             AS case_fatality_rate,

    CASE
        WHEN a.confirmed = 0 THEN 0.0
        ELSE ROUND(a.recovered * 100.0 / a.confirmed, 4)
    END                                             AS recovery_rate,

    CASE
        WHEN a.population = 0 THEN 0.0
        ELSE ROUND(a.confirmed * 1000000.0 / a.population, 2)
    END                                             AS cases_per_million,

    CASE
        WHEN a.population = 0 THEN 0.0
        ELSE ROUND(a.deaths * 1000000.0 / a.population, 2)
    END                                             AS deaths_per_million,

    -- Active cases estimate (confirmed - deaths - recovered)
    GREATEST(0, a.confirmed - a.deaths - a.recovered) AS active_estimated
FROM aggregated a;


-- ── STEP 3: CLEAN VACCINATION TABLE ─────────────────────────

DROP TABLE IF EXISTS vaccination_clean;

CREATE TABLE vaccination_clean AS
SELECT
    date::DATE                                                      AS vax_date,
    TRIM(country)                                                   AS country,
    COALESCE(NULLIF(total_vaccinations,      ''), '0')::BIGINT      AS total_vaccinations,
    COALESCE(NULLIF(people_vaccinated,       ''), '0')::BIGINT      AS people_vaccinated,
    COALESCE(NULLIF(people_fully_vaccinated, ''), '0')::BIGINT      AS people_fully_vaccinated,
    COALESCE(NULLIF(boosters,                ''), '0')::BIGINT      AS boosters,
    COALESCE(NULLIF(daily_vaccinations,      ''), '0')::INTEGER     AS daily_vaccinations,
    vaccines_used
FROM vaccination_raw;


-- ── STEP 4: VALIDATION CHECKS ────────────────────────────────

-- 4a. Row counts before and after
SELECT 'raw'   AS stage, COUNT(*) AS rows FROM covid_raw
UNION ALL
SELECT 'clean' AS stage, COUNT(*) AS rows FROM covid_clean;

-- 4b. Ensure no deaths > confirmed
SELECT COUNT(*) AS impossible_rows
FROM covid_clean
WHERE deaths > confirmed;

-- 4c. Date range
SELECT MIN(report_date) AS earliest, MAX(report_date) AS latest FROM covid_clean;

-- 4d. Countries covered
SELECT COUNT(DISTINCT country) AS n_countries FROM covid_clean;

-- 4e. Sample cleaned rows
SELECT * FROM covid_clean ORDER BY report_date, country LIMIT 20;
