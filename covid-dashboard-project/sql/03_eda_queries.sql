-- ============================================================
-- SCRIPT 3: EXPLORATORY DATA ANALYSIS (EDA)
-- COVID-19 End-to-End Analytics Project
-- All queries run on: covid_clean, vaccination_clean, country_ref
-- ============================================================


-- ════════════════════════════════════════════════════════════
-- SECTION A: GLOBAL SUMMARY STATISTICS
-- ════════════════════════════════════════════════════════════

-- A1. All-time global totals (latest snapshot per country)
WITH latest AS (
    SELECT country, MAX(report_date) AS max_date
    FROM covid_clean
    GROUP BY country
),
snapshot AS (
    SELECT c.*
    FROM covid_clean c
    JOIN latest l ON c.country = l.country AND c.report_date = l.max_date
    WHERE c.province_state = 'All'
       OR c.province_state IS NULL
)
SELECT
    COUNT(DISTINCT country)             AS countries_affected,
    SUM(confirmed)                      AS global_confirmed,
    SUM(deaths)                         AS global_deaths,
    SUM(recovered)                      AS global_recovered,
    ROUND(SUM(deaths)*100.0/SUM(confirmed), 3) AS global_cfr_pct
FROM snapshot;


-- A2. Monthly global new cases trend
SELECT
    DATE_TRUNC('month', report_date)    AS month,        -- PostgreSQL
    -- strftime('%Y-%m', report_date)   AS month,        -- SQLite
    SUM(new_cases)                      AS monthly_new_cases,
    SUM(new_deaths)                     AS monthly_new_deaths
FROM covid_clean
WHERE province_state IN ('All', 'Hubei', 'New York', 'Washington',
                         'California', 'New Jersey', 'Texas', 'Florida',
                         'Illinois', 'Guangdong', 'Beijing', 'Shanghai')
   OR province_state IS NULL
GROUP BY 1
ORDER BY 1;


-- A3. Pandemic waves — identify peak months per continent
WITH monthly_continent AS (
    SELECT
        continent,
        DATE_TRUNC('month', report_date) AS month,
        SUM(new_cases)                   AS new_cases
    FROM covid_clean
    GROUP BY 1, 2
),
ranked AS (
    SELECT *,
           RANK() OVER (PARTITION BY continent ORDER BY new_cases DESC) AS peak_rank
    FROM monthly_continent
)
SELECT continent, month, new_cases
FROM ranked
WHERE peak_rank <= 3
ORDER BY continent, peak_rank;


-- ════════════════════════════════════════════════════════════
-- SECTION B: COUNTRY-LEVEL ANALYSIS
-- ════════════════════════════════════════════════════════════

-- B1. Top 10 countries by total confirmed cases (latest data)
WITH latest_country AS (
    SELECT
        country,
        continent,
        MAX(confirmed)         AS total_confirmed,
        MAX(deaths)            AS total_deaths,
        MAX(cases_per_million) AS cases_per_million,
        MAX(deaths_per_million)AS deaths_per_million,
        MAX(case_fatality_rate)AS cfr
    FROM covid_clean
    GROUP BY country, continent
)
SELECT
    RANK() OVER (ORDER BY total_confirmed DESC) AS rank,
    country,
    continent,
    total_confirmed,
    total_deaths,
    ROUND(cfr, 2)           AS cfr_pct,
    ROUND(cases_per_million, 0) AS cases_per_million
FROM latest_country
ORDER BY total_confirmed DESC
LIMIT 10;


-- B2. Worst case fatality rate — among countries with 100k+ cases
WITH country_stats AS (
    SELECT
        country,
        continent,
        MAX(confirmed) AS confirmed,
        MAX(deaths)    AS deaths,
        MAX(case_fatality_rate) AS cfr
    FROM covid_clean
    GROUP BY country, continent
)
SELECT country, continent, confirmed, deaths, ROUND(cfr, 3) AS cfr_pct
FROM country_stats
WHERE confirmed >= 100000
ORDER BY cfr DESC
LIMIT 15;


-- B3. Cases per million (population-adjusted) — fairest comparison
WITH latest AS (
    SELECT country, continent, MAX(cases_per_million) AS cpm, MAX(deaths_per_million) AS dpm
    FROM covid_clean
    GROUP BY country, continent
)
SELECT
    country,
    continent,
    ROUND(cpm, 0)  AS cases_per_million,
    ROUND(dpm, 0)  AS deaths_per_million
FROM latest
ORDER BY cpm DESC
LIMIT 15;


-- B4. Country growth momentum — compare Q1 vs Q2 2022
WITH half AS (
    SELECT
        country,
        SUM(CASE WHEN report_date BETWEEN '2022-01-01' AND '2022-03-31' THEN new_cases ELSE 0 END) AS q1_2022,
        SUM(CASE WHEN report_date BETWEEN '2022-04-01' AND '2022-06-30' THEN new_cases ELSE 0 END) AS q2_2022
    FROM covid_clean
    GROUP BY country
)
SELECT
    country,
    q1_2022,
    q2_2022,
    q2_2022 - q1_2022                            AS change,
    CASE WHEN q1_2022 = 0 THEN NULL
         ELSE ROUND((q2_2022 - q1_2022)*100.0 / q1_2022, 1)
    END                                           AS pct_change
FROM half
WHERE q1_2022 > 0
ORDER BY pct_change DESC
LIMIT 15;


-- ════════════════════════════════════════════════════════════
-- SECTION C: VACCINATION ANALYSIS
-- ════════════════════════════════════════════════════════════

-- C1. Latest vaccination coverage by country
WITH latest_vax AS (
    SELECT country, MAX(vax_date) AS max_date
    FROM vaccination_clean
    GROUP BY country
)
SELECT
    v.country,
    v.total_vaccinations,
    v.people_vaccinated,
    v.people_fully_vaccinated,
    v.boosters,
    r.population_2020,
    ROUND(v.people_fully_vaccinated * 100.0 / r.population_2020, 1) AS fully_vax_pct,
    ROUND(v.people_vaccinated       * 100.0 / r.population_2020, 1) AS at_least_one_dose_pct
FROM vaccination_clean v
JOIN latest_vax lv       ON v.country = lv.country AND v.vax_date = lv.max_date
JOIN country_ref r       ON v.country = r.country
ORDER BY fully_vax_pct DESC;


-- C2. Vaccination vs. Case Fatality Rate correlation
-- (Join latest vax coverage with latest CFR per country)
WITH latest_vax AS (
    SELECT v.country,
           ROUND(v.people_fully_vaccinated * 100.0 / r.population_2020, 1) AS fully_vax_pct
    FROM vaccination_clean v
    JOIN (SELECT country, MAX(vax_date) md FROM vaccination_clean GROUP BY country) mv
         ON v.country = mv.country AND v.vax_date = mv.md
    JOIN country_ref r ON v.country = r.country
),
latest_cfr AS (
    SELECT country, MAX(case_fatality_rate) AS cfr, MAX(confirmed) AS confirmed
    FROM covid_clean
    GROUP BY country
)
SELECT
    c.country,
    c.confirmed,
    ROUND(c.cfr, 3)       AS cfr_pct,
    v.fully_vax_pct
FROM latest_cfr c
JOIN latest_vax v ON c.country = v.country
ORDER BY c.confirmed DESC;


-- C3. Vaccination rollout speed — days to reach 50% coverage
WITH vax_progress AS (
    SELECT
        v.country,
        v.vax_date,
        ROUND(v.people_vaccinated * 100.0 / r.population_2020, 2) AS pct_one_dose,
        MIN(v.vax_date) OVER (PARTITION BY v.country) AS first_vax_date
    FROM vaccination_clean v
    JOIN country_ref r ON v.country = r.country
)
SELECT DISTINCT
    country,
    first_vax_date,
    MIN(vax_date) FILTER (WHERE pct_one_dose >= 50) OVER (PARTITION BY country)
                                                AS date_reached_50pct,
    (MIN(vax_date) FILTER (WHERE pct_one_dose >= 50) OVER (PARTITION BY country))
        - first_vax_date                       AS days_to_50pct
FROM vax_progress
ORDER BY days_to_50pct NULLS LAST;


-- ════════════════════════════════════════════════════════════
-- SECTION D: CONTINENT & INCOME GROUP ANALYSIS
-- ════════════════════════════════════════════════════════════

-- D1. Totals by continent
SELECT
    c.continent,
    COUNT(DISTINCT c.country)           AS countries,
    MAX(c.confirmed)                    AS total_confirmed,
    MAX(c.deaths)                       AS total_deaths,
    ROUND(AVG(c.case_fatality_rate), 3) AS avg_cfr,
    ROUND(AVG(c.cases_per_million), 0)  AS avg_cases_per_million
FROM covid_clean c
GROUP BY c.continent
ORDER BY total_confirmed DESC;


-- D2. Impact by income group (via country_ref join)
SELECT
    r.income_group,
    COUNT(DISTINCT c.country)           AS countries,
    SUM(DISTINCT c.confirmed)           AS total_confirmed,
    ROUND(AVG(c.case_fatality_rate), 3) AS avg_cfr,
    ROUND(AVG(r.healthcare_index), 1)   AS avg_healthcare_index
FROM covid_clean c
JOIN country_ref r ON c.country = r.country
GROUP BY r.income_group
ORDER BY total_confirmed DESC;


-- D3. Healthcare index vs. Deaths per million (scatter data for Tableau)
SELECT
    c.country,
    r.continent,
    r.income_group,
    r.healthcare_index,
    r.gdp_per_capita_usd,
    MAX(c.deaths_per_million)           AS deaths_per_million,
    MAX(c.cases_per_million)            AS cases_per_million,
    MAX(c.case_fatality_rate)           AS cfr
FROM covid_clean c
JOIN country_ref r ON c.country = r.country
GROUP BY c.country, r.continent, r.income_group, r.healthcare_index, r.gdp_per_capita_usd
ORDER BY deaths_per_million DESC;


-- ════════════════════════════════════════════════════════════
-- SECTION E: TIME SERIES FOR TABLEAU DASHBOARD
-- ════════════════════════════════════════════════════════════

-- E1. Daily global running totals (for area chart)
SELECT
    report_date,
    SUM(new_cases)         AS daily_new_cases,
    SUM(new_deaths)        AS daily_new_deaths,
    SUM(SUM(new_cases)) OVER (ORDER BY report_date) AS cumulative_cases,
    SUM(SUM(new_deaths)) OVER (ORDER BY report_date) AS cumulative_deaths
FROM covid_clean
GROUP BY report_date
ORDER BY report_date;


-- E2. Country time-series (for animated map / line chart in Tableau)
SELECT
    report_date,
    country,
    continent,
    confirmed,
    deaths,
    new_cases,
    new_deaths,
    cases_per_million,
    deaths_per_million,
    case_fatality_rate,
    recovery_rate
FROM covid_clean
WHERE province_state = 'All'
   OR province_state IS NULL
ORDER BY country, report_date;


-- E3. Vaccination timeline joined with case data (for Tableau dual-axis)
SELECT
    v.vax_date                  AS report_date,
    v.country,
    v.total_vaccinations,
    v.people_fully_vaccinated,
    v.daily_vaccinations,
    ROUND(v.people_fully_vaccinated * 100.0 / r.population_2020, 2) AS fully_vax_pct,
    c.new_cases,
    c.new_deaths,
    c.case_fatality_rate
FROM vaccination_clean v
JOIN country_ref r    ON v.country = r.country
LEFT JOIN covid_clean c ON v.country = c.country
                       AND v.vax_date = c.report_date
                       AND (c.province_state = 'All' OR c.province_state IS NULL)
ORDER BY v.country, v.vax_date;
