-- dim_skills.sql
-- Top skills by demand count, filterable by role/location on dashboard
-- Unnests the standardized_skills array so each skill becomes its own row

WITH exploded_skills AS (
    SELECT
        job_id,
        role,
        level,
        locations,
        source,
        UNNEST(string_to_array(standardized_skills, ',')) AS raw_skill
    FROM {{ ref('stg_jobs') }}
    WHERE standardized_skills IS NOT NULL AND standardized_skills != ''
)

SELECT
    TRIM(raw_skill) AS skill_name,
    role,
    locations,
    COUNT(DISTINCT job_id) AS job_count
FROM exploded_skills
WHERE TRIM(raw_skill) != ''
GROUP BY TRIM(raw_skill), role, locations
ORDER BY job_count DESC
