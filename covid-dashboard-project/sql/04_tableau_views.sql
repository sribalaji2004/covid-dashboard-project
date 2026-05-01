-- ============================================================
-- SCRIPT 4: FINAL VIEWS FOR TABLEAU
-- Export each view as a CSV → load into Tableau
-- ============================================================

-- ── VIEW 1: Country Snapshot (KPI cards + map) ──────────────
CREATE OR REPLACE VIEW vw_country_snapshot AS
WITH stats AS (
    SELECT
        country,
        continent,
        MAX(confirmed)          AS total_confirmed,
        MAX(deaths)             AS total_deaths,
        MAX(recovered)          AS total_recovered,
        MAX(cases_per_million)  AS cases_per_million,
        MAX(deaths_per_million) AS deaths_per_million,
        MAX(case_fatality_rate) AS cfr,
        MAX(recovery_rate)      AS recovery_rate,
        MAX(active_estimated)   AS active_cases
    FROM covid_clean
    GROUP BY country, continent
),
vax AS (
    SELECT v.country,
           v.people_fully_vaccinated,
           r.population_2020,
           ROUND(v.people_fully_vaccinated * 100.0 / r.population_2020, 1) AS fully_vax_pct
    FROM vaccination_clean v
    JOIN (SELECT country, MAX(vax_date) md FROM vaccination_clean GROUP BY country) mv
         ON v.country = mv.country AND v.vax_date = mv.md
    JOIN country_ref r ON v.country = r.country
)
SELECT
    s.country,
    s.continent,
    r.region,
    r.income_group,
    r.population_2020,
    r.median_age,
    r.gdp_per_capita_usd,
    r.healthcare_index,
    s.total_confirmed,
    s.total_deaths,
    s.total_recovered,
    s.active_cases,
    ROUND(s.cfr, 3)              AS cfr_pct,
    ROUND(s.recovery_rate, 1)    AS recovery_rate_pct,
    ROUND(s.cases_per_million, 0)AS cases_per_million,
    ROUND(s.deaths_per_million,0) AS deaths_per_million,
    COALESCE(v.fully_vax_pct, 0) AS fully_vaccinated_pct
FROM stats s
JOIN country_ref r ON s.country = r.country
LEFT JOIN vax v    ON s.country = v.country
ORDER BY s.total_confirmed DESC;


-- ── VIEW 2: Global Time Series (trend lines) ─────────────────
CREATE OR REPLACE VIEW vw_global_timeseries AS
SELECT
    report_date,
    SUM(new_cases)         AS daily_new_cases,
    SUM(new_deaths)        AS daily_new_deaths,
    SUM(confirmed)         AS total_confirmed,   -- running max per country then sum
    SUM(deaths)            AS total_deaths
FROM (
    SELECT report_date, country,
           new_cases, new_deaths,
           MAX(confirmed) AS confirmed,
           MAX(deaths)    AS deaths
    FROM covid_clean
    GROUP BY report_date, country
) agg
GROUP BY report_date
ORDER BY report_date;


-- ── VIEW 3: Country Time Series (line chart by country) ──────
CREATE OR REPLACE VIEW vw_country_timeseries AS
SELECT
    report_date,
    country,
    continent,
    confirmed,
    deaths,
    new_cases,
    new_deaths,
    case_fatality_rate,
    cases_per_million,
    deaths_per_million,
    active_estimated
FROM covid_clean
WHERE province_state IN ('All')
   OR province_state IS NULL
ORDER BY country, report_date;


-- ── VIEW 4: Vaccination Progress ─────────────────────────────
CREATE OR REPLACE VIEW vw_vaccination_progress AS
SELECT
    v.vax_date,
    v.country,
    r.continent,
    r.region,
    r.income_group,
    r.population_2020,
    v.total_vaccinations,
    v.people_vaccinated,
    v.people_fully_vaccinated,
    v.boosters,
    v.daily_vaccinations,
    ROUND(v.people_vaccinated       * 100.0 / r.population_2020, 2) AS pct_at_least_one_dose,
    ROUND(v.people_fully_vaccinated * 100.0 / r.population_2020, 2) AS pct_fully_vaccinated,
    ROUND(v.boosters                * 100.0 / r.population_2020, 2) AS pct_boosted,
    v.vaccines_used
FROM vaccination_clean v
JOIN country_ref r ON v.country = r.country
ORDER BY v.country, v.vax_date;


-- ── VIEW 5: Scatter — Healthcare vs. Outcomes ────────────────
CREATE OR REPLACE VIEW vw_healthcare_scatter AS
SELECT
    c.country,
    r.continent,
    r.income_group,
    r.healthcare_index,
    r.gdp_per_capita_usd,
    r.median_age,
    r.population_density,
    MAX(c.deaths_per_million)    AS deaths_per_million,
    MAX(c.cases_per_million)     AS cases_per_million,
    MAX(c.case_fatality_rate)    AS cfr,
    COALESCE(v.fully_vax_pct, 0) AS fully_vaccinated_pct
FROM covid_clean c
JOIN country_ref r ON c.country = r.country
LEFT JOIN (
    SELECT v2.country,
           ROUND(v2.people_fully_vaccinated * 100.0 / r2.population_2020, 1) AS fully_vax_pct
    FROM vaccination_clean v2
    JOIN (SELECT country, MAX(vax_date) md FROM vaccination_clean GROUP BY country) mv
         ON v2.country = mv.country AND v2.vax_date = mv.md
    JOIN country_ref r2 ON v2.country = r2.country
) v ON c.country = v.country
GROUP BY c.country, r.continent, r.income_group, r.healthcare_index,
         r.gdp_per_capita_usd, r.median_age, r.population_density, v.fully_vax_pct
ORDER BY deaths_per_million DESC;


-- ── EXPORT INSTRUCTIONS ──────────────────────────────────────
-- Run these in psql to export CSVs for Tableau:
--
--   \COPY (SELECT * FROM vw_country_snapshot)     TO 'tableau_prep/01_country_snapshot.csv'     CSV HEADER;
--   \COPY (SELECT * FROM vw_global_timeseries)    TO 'tableau_prep/02_global_timeseries.csv'    CSV HEADER;
--   \COPY (SELECT * FROM vw_country_timeseries)   TO 'tableau_prep/03_country_timeseries.csv'   CSV HEADER;
--   \COPY (SELECT * FROM vw_vaccination_progress) TO 'tableau_prep/04_vaccination_progress.csv' CSV HEADER;
--   \COPY (SELECT * FROM vw_healthcare_scatter)   TO 'tableau_prep/05_healthcare_scatter.csv'   CSV HEADER;
--
-- Or in SQLite:
--   .output tableau_prep/01_country_snapshot.csv
--   SELECT * FROM vw_country_snapshot;
--   .output stdout
