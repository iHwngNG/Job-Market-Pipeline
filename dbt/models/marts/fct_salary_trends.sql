-- fct_salary_trends.sql
-- Salary statistics grouped by role, level, and currency
-- Supports salary distribution and trend charts on dashboard

SELECT
    role,
    level,
    currency,
    COUNT(DISTINCT job_id)  AS job_count,
    ROUND(AVG(avg_salary), 0) AS avg_salary,
    MIN(min_salary)         AS lowest_salary,
    MAX(max_salary)         AS highest_salary,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY avg_salary) AS median_salary
FROM {{ ref('stg_jobs') }}
WHERE avg_salary IS NOT NULL
  AND currency != 'Unknown'
GROUP BY role, level, currency
ORDER BY avg_salary DESC
