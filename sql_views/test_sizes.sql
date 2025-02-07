WITH test AS (
SELECT
    DISTINCT(REPLACE(UNNEST(STRING_TO_ARRAY(total_sizes, ',')), ' ', '')) all_sizes
FROM
    globant.public.globant1
WHERE
    1=1
    AND is_bra
ORDER BY
    all_sizes
)
SELECT
    all_sizes,
    CASE
        WHEN all_sizes LIKE '42%' OR all_sizes LIKE '44%' OR all_sizes LIKE '46%' OR all_sizes LIKE '1X%' OR all_sizes LIKE '2X%' OR all_sizes LIKE '3X%' THEN 'Extra Large'
        WHEN all_sizes LIKE '38%' OR all_sizes LIKE '40%' THEN 'Large'
        WHEN all_sizes LIKE '34%' OR all_sizes LIKE '36%' THEN 'Medium'
        WHEN all_sizes LIKE '30%' OR all_sizes LIKE '32%' THEN 'Small'
        ELSE 'Error' END size_cluster
FROM
    test