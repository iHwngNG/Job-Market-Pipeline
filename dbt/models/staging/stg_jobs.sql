-- stg_jobs.sql
-- Staging view: light transformations on top of analytics_jobs
-- Adds avg_salary column, filters out clearly invalid records

WITH base AS (
    SELECT
        job_id,
        title,
        role,
        level,
        company,
        locations,
        working_method,
        min_salary,
        max_salary,
        currency,
        standardized_skills,
        posted_date,
        source,
        url
    FROM {{ source('public', 'analytics_jobs') }}
    WHERE title IS NOT NULL
      AND company IS NOT NULL
)

SELECT
    *,
    -- Calculate average salary for analytics
    CASE
        WHEN min_salary IS NOT NULL AND max_salary IS NOT NULL
            THEN (min_salary + max_salary) / 2
        WHEN min_salary IS NOT NULL
            THEN min_salary
        WHEN max_salary IS NOT NULL
            THEN max_salary
        ELSE NULL
    END AS avg_salary
FROM base
