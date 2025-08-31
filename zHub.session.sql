SELECT
     source_name AS source
    ,COUNT(*) AS count
FROM govhack2025.datasets
GROUP BY source_name
ORDER BY source

UNION ALL

SELECT
     'Total' AS source
    ,COUNT(*) AS count
FROM govhack2025.datasets
ORDER BY source
;