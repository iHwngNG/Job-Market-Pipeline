-- dim_companies.sql
-- Top hiring companies ranked by number of open positions

SELECT
    company,
    COUNT(DISTINCT job_id)  AS total_jobs,
    COUNT(DISTINCT role)    AS unique_roles,
    COUNT(DISTINCT locations) AS unique_locations,
    ROUND(AVG(avg_salary), 0) AS avg_offered_salary,
    MIN(posted_date)        AS earliest_post,
    MAX(posted_date)        AS latest_post
FROM {{ ref('stg_jobs') }}
WHERE company IS NOT NULL
  AND company != ''
GROUP BY company
ORDER BY total_jobs DESC
