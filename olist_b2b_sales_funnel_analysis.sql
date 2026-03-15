-- =====================================================
-- From Lead to Seller: Olist B2B Sales Funnel Analysis
-- =====================================================
-- Dataset : Marketing Funnel by Olist (Kaggle)
-- Tables  : marketing_qualified_leads (8,000 rows)
--           closed_deals (842 rows)
-- Period  : Jun 2017 to Jun 2018 (MQL capture window)
-- Tool    : PostgreSQL 18 + pgAdmin 4
-- =====================================================


-- =====================================================
-- SECTION 0: SCHEMA AND TABLE SETUP
-- Safe to run repeatedly, creates only if not exists
-- =====================================================

CREATE SCHEMA IF NOT EXISTS olist_marketing;
SET search_path = olist_marketing;

CREATE TABLE IF NOT EXISTS marketing_qualified_leads (
    mql_id             TEXT,
    first_contact_date DATE,
    landing_page_id    TEXT,
    origin             TEXT
);

CREATE TABLE IF NOT EXISTS closed_deals (
    mql_id                        TEXT,
    seller_id                     TEXT,
    sdr_id                        TEXT,
    sr_id                         TEXT,
    won_date                      TIMESTAMP,
    business_segment              TEXT,
    lead_type                     TEXT,
    lead_behaviour_profile        TEXT,
    has_company                   TEXT,
    has_gtin                      TEXT,
    average_stock                 TEXT,
    business_type                 TEXT,
    declared_product_catalog_size TEXT,
    declared_monthly_revenue      NUMERIC
);


-- =====================================================
-- DATA LOADING
-- TRUNCATE empties tables first so no duplicates
-- so that when we COPY it reloads fresh data every run
-- place the datasets in C:/pgdata/ and update paths if needed
-- =====================================================

TRUNCATE TABLE olist_marketing.marketing_qualified_leads;
TRUNCATE TABLE olist_marketing.closed_deals;

COPY olist_marketing.marketing_qualified_leads(
    mql_id, first_contact_date, landing_page_id, origin)
FROM 'C:/pgdata/olist_marketing_qualified_leads_dataset.csv'
WITH (FORMAT csv, HEADER true, DELIMITER ',');

COPY olist_marketing.closed_deals(
    mql_id, seller_id, sdr_id, sr_id, won_date, business_segment,
    lead_type, lead_behaviour_profile, has_company, has_gtin,
    average_stock, business_type, declared_product_catalog_size,
    declared_monthly_revenue)
FROM 'C:/pgdata/olist_closed_deals_dataset.csv'
WITH (FORMAT csv, HEADER true, DELIMITER ',');


-- =====================================================
-- SECTION A: DATA EXPLORATION
-- =====================================================

-- Row counts, verify data loaded correctly
SELECT COUNT(*) FROM olist_marketing.marketing_qualified_leads;
SELECT COUNT(*) FROM olist_marketing.closed_deals;

-- Quick look at both tables
-- Further exploration: use Excel filters to check for data entry issues
SELECT * FROM olist_marketing.marketing_qualified_leads LIMIT 5;
SELECT * FROM olist_marketing.closed_deals LIMIT 5;

-- Null check for MQL
SELECT
    COUNT(*)                             AS total,
    COUNT(*) - COUNT(mql_id)             AS null_mql_id,
    COUNT(*) - COUNT(first_contact_date) AS null_first_contact,
    COUNT(*) - COUNT(landing_page_id)    AS null_landing_page,
    COUNT(*) - COUNT(origin)             AS null_origin
FROM olist_marketing.marketing_qualified_leads;

-- Null check for closed_deals
SELECT
    COUNT(*)                                        AS total,
    COUNT(*) - COUNT(business_segment)              AS null_segment,
    COUNT(*) - COUNT(lead_type)                     AS null_lead_type,
    COUNT(*) - COUNT(lead_behaviour_profile)        AS null_behaviour,
    COUNT(*) - COUNT(has_company)                   AS null_company,
    COUNT(*) - COUNT(has_gtin)                      AS null_gtin,
    COUNT(*) - COUNT(average_stock)                 AS null_avg_stock,
    COUNT(*) - COUNT(business_type)                 AS null_business_type,
    COUNT(*) - COUNT(declared_product_catalog_size) AS null_catalog_size,
    COUNT(*) - COUNT(declared_monthly_revenue)      AS null_revenue
FROM olist_marketing.closed_deals;

-- Checking dirty values in lead_behaviour_profile
SELECT lead_behaviour_profile, COUNT(*) AS total
FROM olist_marketing.closed_deals
GROUP BY lead_behaviour_profile
ORDER BY total DESC;

-- Checking dirty values in business_type
SELECT business_type, COUNT(*) AS total
FROM olist_marketing.closed_deals
GROUP BY business_type
ORDER BY total DESC;

-- Checking dirty values in business_segment
SELECT business_segment, COUNT(*) AS total
FROM olist_marketing.closed_deals
GROUP BY business_segment
ORDER BY total DESC;

-- Origin distribution in MQL
SELECT origin, COUNT(*) AS total
FROM olist_marketing.marketing_qualified_leads
GROUP BY origin
ORDER BY total DESC;


-- =====================================================
-- SECTION B: DATA CLEANING
-- =====================================================

-- COLUMNS EXCLUDED FROM ANALYSIS
-- has_company              : 92.5% null (779/842), insufficient data for analysis
-- has_gtin                 : 92.4% null (778/842), insufficient data for analysis
-- average_stock            : 92.2% null (776/842) + dirty entries (dates, ranges), unusable
-- declared_product_catalog_size : 91.8% null (773/842), insufficient data for analysis
-- These columns are retained in the raw table but excluded from the cleaned temp tables

-- CLEANING DECISIONS
-- mql.origin               : 60 nulls, replaced with 'unknown'
-- cd.lead_behaviour_profile: 177 nulls, replaced with 'unknown'
-- cd.business_type         : 10 nulls, replaced with 'unknown'
-- cd.business_segment      : 1 null, replaced with 'unknown'
--                            'jewerly' typo, corrected to 'jewelry'
-- cd.lead_type             : 6 nulls, replaced with 'unknown'
-- cd.declared_monthly_revenue : 0 nulls, no cleaning needed

-- MQL clean table
DROP TABLE IF EXISTS cleaned_marketing_qualified_leads;
CREATE TEMP TABLE cleaned_marketing_qualified_leads AS
SELECT
    mql_id,
    first_contact_date,
    landing_page_id,
    COALESCE(origin, 'unknown') AS origin
FROM olist_marketing.marketing_qualified_leads;

-- Closed deals clean table
DROP TABLE IF EXISTS cleaned_closed_deals;
CREATE TEMP TABLE cleaned_closed_deals AS
SELECT
    mql_id,
    seller_id,
    sdr_id,
    sr_id,
    won_date,
    REPLACE(COALESCE(business_segment, 'unknown'), 'jewerly', 'jewelry') AS business_segment,
    COALESCE(lead_type, 'unknown')              AS lead_type,
    COALESCE(lead_behaviour_profile, 'unknown') AS lead_behaviour_profile,
    COALESCE(business_type, 'unknown')          AS business_type,
    declared_monthly_revenue
FROM olist_marketing.closed_deals;

-- Verify cleaned MQL
SELECT origin, COUNT(*) AS total
FROM cleaned_marketing_qualified_leads
GROUP BY origin
ORDER BY total DESC;

-- Verify cleaned closed_deals
SELECT business_segment, COUNT(*) AS total
FROM cleaned_closed_deals
GROUP BY business_segment
ORDER BY total DESC;

SELECT lead_behaviour_profile, COUNT(*) AS total
FROM cleaned_closed_deals
GROUP BY lead_behaviour_profile
ORDER BY total DESC;

SELECT business_type, COUNT(*) AS total
FROM cleaned_closed_deals
GROUP BY business_type
ORDER BY total DESC;

SELECT lead_type, COUNT(*) AS total
FROM cleaned_closed_deals
GROUP BY lead_type
ORDER BY total DESC;

-- Date integrity check: won_date should never be before first_contact_date
-- A deal cannot close before the lead was first contacted
SELECT COUNT(*) AS impossible_dates
FROM cleaned_closed_deals AS cd
JOIN cleaned_marketing_qualified_leads AS mql
    ON cd.mql_id = mql.mql_id
WHERE cd.won_date < mql.first_contact_date::TIMESTAMP;

-- Inspect the record
SELECT
    mql.mql_id,
    mql.first_contact_date,
    cd.won_date,
    cd.business_segment,
    cd.lead_type
FROM cleaned_closed_deals AS cd
JOIN cleaned_marketing_qualified_leads AS mql
    ON cd.mql_id = mql.mql_id
WHERE cd.won_date < mql.first_contact_date::TIMESTAMP;

-- DATA QUALITY NOTE
-- 1 record found where won_date < first_contact_date (mql_id: b91cf8812365f50ff4bda4bcd6206b05)
-- won_date: 2018-03-06, first_contact_date: 2018-03-08, 2 day discrepancy
-- Likely a data entry error, retained in dataset as impact is negligible (0.1% of records)
-- Excluded only from time-to-close calculations to avoid negative duration values


-- =====================================================
-- SECTION C: FUNNEL OVERVIEW
-- =====================================================

-- C1. What is the overall conversion rate? (MQLs to closed deals)
SELECT
    (SELECT COUNT(*) FROM cleaned_marketing_qualified_leads) AS total_leads,
    (SELECT COUNT(*) FROM cleaned_closed_deals)              AS total_closed,
    ROUND(
        (SELECT COUNT(*) FROM cleaned_closed_deals) * 100.0
        / (SELECT COUNT(*) FROM cleaned_marketing_qualified_leads)
    , 2) AS conversion_rate_pct;

-- C2. How many MQLs came in per month?
-- GROUP BY and ORDER BY use DATE_TRUNC not the alias to preserve chronological sort order
SELECT
    TO_CHAR(DATE_TRUNC('month', first_contact_date), 'Mon YYYY') AS lead_month,
    COUNT(*) AS lead_count
FROM cleaned_marketing_qualified_leads
GROUP BY DATE_TRUNC('month', first_contact_date)
ORDER BY DATE_TRUNC('month', first_contact_date);

-- C3. How many deals were closed per month?
SELECT
    TO_CHAR(DATE_TRUNC('month', won_date), 'Mon YYYY') AS won_month,
    COUNT(*) AS won_count
FROM cleaned_closed_deals
GROUP BY DATE_TRUNC('month', won_date)
ORDER BY DATE_TRUNC('month', won_date);

-- C4a. Same-month conversion rate: deals closed in the same month leads came in
-- NOTE: Not cohort-based, leads may close in later months
-- Early months (Jul-Nov 2017) show 0% because deals took months to close
WITH monthly_leads AS (
    SELECT
        DATE_TRUNC('month', first_contact_date) AS lead_month,
        COUNT(*) AS lead_count
    FROM cleaned_marketing_qualified_leads
    GROUP BY DATE_TRUNC('month', first_contact_date)
),
monthly_deals AS (
    SELECT
        DATE_TRUNC('month', won_date) AS won_month,
        COUNT(*) AS won_count
    FROM cleaned_closed_deals
    GROUP BY DATE_TRUNC('month', won_date)
)
SELECT
    TO_CHAR(ml.lead_month, 'Mon YYYY')                           AS month,
    ml.lead_count,
    COALESCE(md.won_count, 0)                                    AS same_month_closed,
    ROUND(COALESCE(md.won_count, 0) * 100.0 / ml.lead_count, 2) AS same_month_pct
FROM monthly_leads AS ml
LEFT JOIN monthly_deals AS md ON ml.lead_month = md.won_month
ORDER BY same_month_pct DESC;

-- C4b. Cohort-based conversion rate: of leads that came in each month, how many ever closed?
-- More accurate than same-month, tracks actual lead outcomes regardless of close date
WITH cohort AS (
    SELECT
        DATE_TRUNC('month', mql.first_contact_date) AS cohort_month,
        COUNT(mql.mql_id) AS total_leads,
        COUNT(cd.mql_id)  AS converted
    FROM cleaned_marketing_qualified_leads AS mql
    LEFT JOIN cleaned_closed_deals AS cd ON mql.mql_id = cd.mql_id
    GROUP BY DATE_TRUNC('month', mql.first_contact_date)
)
SELECT
    TO_CHAR(cohort_month, 'Mon YYYY')         AS cohort,
    total_leads,
    converted,
    ROUND(converted * 100.0 / total_leads, 2) AS cohort_pct
FROM cohort
ORDER BY cohort_pct DESC;

-- C4c. Combined: same-month vs cohort conversion rate side by side
-- same_month_pct: deals closed same month / leads that month
-- cohort_pct: of leads that month, how many ever closed regardless of when
-- KEY INSIGHT: early cohorts (Jul-Nov 2017) show 0% same-month but up to 5.5% cohort
-- deals took months to close in early funnel stages
-- Feb-Mar 2018 show strongest cohort conversion at 14%+
-- Apr 2018 leads same-month but ranks 3rd in cohort, some leads still pending at dataset end
WITH monthly_leads AS (
    SELECT
        DATE_TRUNC('month', first_contact_date) AS lead_month,
        COUNT(*) AS lead_count
    FROM cleaned_marketing_qualified_leads
    GROUP BY DATE_TRUNC('month', first_contact_date)
),
monthly_deals AS (
    SELECT
        DATE_TRUNC('month', won_date) AS won_month,
        COUNT(*) AS won_count
    FROM cleaned_closed_deals
    GROUP BY DATE_TRUNC('month', won_date)
),
cohort AS (
    SELECT
        DATE_TRUNC('month', mql.first_contact_date) AS cohort_month,
        COUNT(cd.mql_id) AS converted
    FROM cleaned_marketing_qualified_leads AS mql
    LEFT JOIN cleaned_closed_deals AS cd ON mql.mql_id = cd.mql_id
    GROUP BY DATE_TRUNC('month', mql.first_contact_date)
)
SELECT
    TO_CHAR(ml.lead_month, 'Mon YYYY')                           AS month,
    ml.lead_count                                                AS total_leads,
    COALESCE(md.won_count, 0)                                    AS same_month_closed,
    ROUND(COALESCE(md.won_count, 0) * 100.0 / ml.lead_count, 2) AS same_month_pct,
    co.converted                                                 AS cohort_closed,
    ROUND(co.converted * 100.0 / ml.lead_count, 2)              AS cohort_pct
FROM monthly_leads AS ml
LEFT JOIN monthly_deals AS md ON ml.lead_month = md.won_month
LEFT JOIN cohort AS co        ON ml.lead_month = co.cohort_month
ORDER BY ml.lead_month;


-- =====================================================
-- SECTION D: LEAD SOURCE ANALYSIS
-- =====================================================

-- D1. How many MQLs came from each origin/source?
SELECT
    origin,
    COUNT(*) AS total_leads
FROM cleaned_marketing_qualified_leads
GROUP BY origin
ORDER BY total_leads DESC;

-- D2. What is the conversion rate by origin?
-- COUNT(cd.mql_id) counts only rows where a match exists in closed_deals (converted leads)
-- COUNT(mql.mql_id) counts all leads regardless of conversion
SELECT
    mql.origin,
    COUNT(mql.mql_id)                                       AS total_leads,
    COUNT(cd.mql_id)                                        AS converted,
    ROUND(COUNT(cd.mql_id) * 100.0 / COUNT(mql.mql_id), 2) AS conversion_rate_pct
FROM cleaned_marketing_qualified_leads AS mql
LEFT JOIN cleaned_closed_deals AS cd ON mql.mql_id = cd.mql_id
GROUP BY mql.origin
ORDER BY conversion_rate_pct DESC;

-- KEY INSIGHT: Unknown origin has highest conversion (16.65%), tracking gap means
-- best-converting channel is unidentified, worth investigating attribution
-- Paid search delivers better quality leads than organic despite lower volume
-- Email and social bring volume but convert poorly, review targeting strategy

-- D3. Which business segment has the most closed deals?
-- business_segment only exists in closed_deals so true conversion rate is not calculable
-- This shows distribution of closed deals by segment instead
SELECT
    business_segment,
    COUNT(*) AS total_closed,
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM cleaned_closed_deals), 2) AS pct_of_total
FROM cleaned_closed_deals
GROUP BY business_segment
ORDER BY total_closed DESC;

-- KEY INSIGHT: Top 6 segments (home_decor, health_beauty, car_accessories,
-- household_utilities, construction_tools, audio_video) account for ~57% of all closed deals
-- Olist's seller acquisition is heavily concentrated in home and lifestyle categories

-- D4. What is the distribution of lead types among closed deals?
-- lead_type only exists in closed_deals, same limitation as D3
SELECT
    lead_type,
    COUNT(*) AS total_closed,
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM cleaned_closed_deals), 2) AS pct_of_total
FROM cleaned_closed_deals
GROUP BY lead_type
ORDER BY total_closed DESC;

-- KEY INSIGHT: Online medium sellers are the core acquisition target at 39.43%
-- Online sellers across all sizes account for ~72% of closed deals
-- Large online sellers (online_top) are rare at 1.66%, likely harder to convert
-- Offline and industry sellers represent ~27%, secondary but significant segment

-- D5. What behaviour profile is most common among closed deals?
SELECT
    lead_behaviour_profile,
    COUNT(*) AS total_closed,
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM cleaned_closed_deals), 2) AS pct_of_total
FROM cleaned_closed_deals
GROUP BY lead_behaviour_profile
ORDER BY total_closed DESC;

-- BEHAVIOUR PROFILE CONTEXT
-- Profile labels (cat, eagle, wolf, shark) are defined by Olist's internal sales methodology
-- The dataset documentation does not provide explicit definitions for each profile
-- Interpretation based on animal characteristics is speculative
-- KEY INSIGHT: Cat profile dominates at 48% of closed deals
-- Shark profile is lowest at 2.85%, rarest among converted sellers
-- 21% unknown behaviour, tracking gap affects profile analysis reliability


-- =====================================================
-- SECTION E: SALES PERFORMANCE
-- =====================================================

-- Base temp table for E1-E5
-- Pre-calculates days_to_close by joining both cleaned tables on mql_id
-- Excludes 1 record where won_date < first_contact_date (data entry error)
DROP TABLE IF EXISTS sales_performance;
CREATE TEMP TABLE sales_performance AS
SELECT
    cd.mql_id,
    cd.seller_id,
    cd.sdr_id,
    cd.sr_id,
    cd.business_segment,
    cd.lead_type,
    cd.lead_behaviour_profile,
    cd.business_type,
    cd.declared_monthly_revenue,
    mql.origin,
    mql.first_contact_date,
    cd.won_date,
    (cd.won_date::DATE - mql.first_contact_date) AS days_to_close
FROM cleaned_closed_deals AS cd
LEFT JOIN cleaned_marketing_qualified_leads AS mql
    ON cd.mql_id = mql.mql_id
WHERE cd.won_date >= mql.first_contact_date::TIMESTAMP;

-- Verify: should return 841 (842 minus the 1 excluded record)
SELECT COUNT(*) FROM sales_performance;

-- E1. What is the average time from first contact to closing?
SELECT ROUND(AVG(days_to_close), 2) AS avg_days_to_close
FROM sales_performance;

-- RESULT: Average 48.5 days (~6.5 weeks) from first contact to closed deal

-- E2. Which business segment closes fastest?
-- Filtered to segments with 5+ closed deals for reliability
SELECT
    business_segment,
    COUNT(*) AS total_closed,
    ROUND(AVG(days_to_close), 2) AS avg_days_to_close
FROM sales_performance
GROUP BY business_segment
HAVING COUNT(*) >= 5
ORDER BY avg_days_to_close ASC;

-- E2 KEY INSIGHT: Bags & backpacks, home office, sports leisure close fastest (<30 days)
-- Health & beauty is the fastest high-volume segment at 35.46 days (92 deals)
-- Audio/video electronics and toys take longest, likely more complex seller onboarding
-- Segments with <5 deals excluded for reliability

-- E3. Which origin closes fastest?
-- Filtered to origins with 5+ closed deals for reliability
SELECT
    origin,
    COUNT(*) AS total_closed,
    ROUND(AVG(days_to_close), 2) AS avg_days_to_close
FROM sales_performance
GROUP BY origin
HAVING COUNT(*) >= 5
ORDER BY avg_days_to_close ASC;

-- E3 KEY INSIGHT: Direct traffic converts well (11.22%) AND closes fast (31 days)
-- best overall channel quality combining conversion rate and speed
-- Paid search converts well (12.30%) but takes nearly 2x longer than direct traffic
-- Social has worst combination, low conversion (5.56%) and slowest close (61 days)
-- Display fastest but only 6 deals, not statistically reliable

-- E4. Who are the top performing SDRs by deals closed?
-- SDR = Sales Development Representative, first human contact after lead signs up
-- Confirms information and schedules consultancy with the SR
SELECT
    sdr_id,
    COUNT(*) AS total_closed,
    ROUND(AVG(days_to_close), 2) AS avg_days_to_close
FROM sales_performance
GROUP BY sdr_id
ORDER BY total_closed DESC
LIMIT 10;

-- E4 KEY INSIGHT: Top SDR closed 140 deals, 73% more than 2nd place (81)
-- SDR 9e4d closes fastest at 18.29 avg days with 55 deals, best efficiency
-- SDR f42a has slowest close time at 78.33 days despite 42 deals, review process
-- All IDs anonymized per Olist data privacy policy

-- E5. Who are the top performing SRs by deals closed?
-- SR = Sales Representative, conducts the consultancy and closes the deal
SELECT
    sr_id,
    COUNT(*) AS total_closed,
    ROUND(AVG(days_to_close), 2) AS avg_days_to_close
FROM sales_performance
GROUP BY sr_id
ORDER BY total_closed DESC
LIMIT 10;

-- E5 KEY INSIGHT: Top SR closed 133 deals, 62% more than 2nd place (82)
-- SR fbf4 best efficiency, 59 deals at 21.97 avg days
-- SR de63 appears in BOTH SDR and SR top 10, same person filling dual roles
-- SR 6565 strong performer, 74 deals at 25.78 days
-- All IDs anonymized per Olist data privacy policy