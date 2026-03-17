-- fct_market_overview.sql
-- High-level market summary metrics for the dashboard overview page

WITH totals AS (
    SELECT
        COUNT(DISTINCT job_id)   AS total_jobs,
        COUNT(DISTINCT company)  AS total_companies,
        COUNT(DISTINCT role)     AS total_roles,
        COUNT(DISTINCT locations) AS total_locations
    FROM {{ ref('stg_jobs') }}
),

by_role AS (
    SELECT
        role,
        COUNT(DISTINCT job_id) AS job_count
    FROM {{ ref('stg_jobs') }}
    GROUP BY role
    ORDER BY job_count DESC
    LIMIT 10
),

by_level AS (
    SELECT
        level,
        COUNT(DISTINCT job_id) AS job_count
    FROM {{ ref('stg_jobs') }}
    GROUP BY level
    ORDER BY job_count DESC
),

by_location AS (
    SELECT
        locations,
        COUNT(DISTINCT job_id) AS job_count
    FROM {{ ref('stg_jobs') }}
    GROUP BY locations
    ORDER BY job_count DESC
    LIMIT 10
)

-- Union all summary tables into one mart for easy dashboard consumption
SELECT 'summary'  AS metric_type, 'total_jobs'       AS metric_name, total_jobs::TEXT       AS metric_value FROM totals
UNION ALL
SELECT 'summary',                  'total_companies',  total_companies::TEXT  FROM totals
UNION ALL
SELECT 'summary',                  'total_roles',      total_roles::TEXT      FROM totals
UNION ALL
SELECT 'summary',                  'total_locations',  total_locations::TEXT  FROM totals
UNION ALL
SELECT 'by_role'   AS metric_type, role               AS metric_name, job_count::TEXT        AS metric_value FROM by_role
UNION ALL
SELECT 'by_level'  AS metric_type, level              AS metric_name, job_count::TEXT        AS metric_value FROM by_level
UNION ALL
SELECT 'by_location' AS metric_type, locations         AS metric_name, job_count::TEXT        AS metric_value FROM by_location
