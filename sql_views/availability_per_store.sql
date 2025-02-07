CREATE OR REPLACE VIEW vw_availability AS
WITH total AS (
SELECT
	store,
	brand_name,
	product_name,
	product_category,
	retailer,
	color,
	CAST(mrp AS FLOAT) mrp,
	REPLACE(UNNEST(STRING_TO_ARRAY(total_sizes, ',')), ' ', '') total_size
FROM
	globant.public.globant1

WHERE
	1=1
	AND is_bra
), stock AS (
SELECT
	store,
	brand_name,
	product_name,
	product_category,
	retailer,
	color,
	CAST(mrp AS FLOAT) mrp,
	REPLACE(UNNEST(STRING_TO_ARRAY(available_size, ',')), ' ', '') available_size
FROM
	globant.public.globant1

WHERE
	1=1
	AND is_bra
)
SELECT
	t.retailer vendor,
	t.brand_name brand,
	CASE
        WHEN t.total_size LIKE '42%' OR t.total_size LIKE '44%' OR t.total_size LIKE '46%' OR t.total_size LIKE '1X%' OR t.total_size LIKE '2X%' OR t.total_size LIKE '3X%' OR t.total_size LIKE 'XL' THEN 'Extra Large'
        WHEN t.total_size LIKE '38%' OR t.total_size LIKE '40%' OR t.total_size = 'L' THEN 'Large'
        WHEN t.total_size LIKE '34%' OR t.total_size LIKE '36%' OR t.total_size = 'M/L' OR t.total_size = 'M' THEN 'Medium'
        WHEN t.total_size LIKE '30%' OR t.total_size LIKE '32%' OR t.total_size = 'P/S' OR t.total_size = 'S' THEN 'Small'
        ELSE 'Error' END size_group,
    t.total_size size,
    t.product_category category,
    t.product_name product,
	t.color color,
	CASE WHEN s.available_size IS NOT NULL THEN 'Yes' ELSE 'No' END is_available,
    t.mrp mrp
FROM
    total t
LEFT JOIN
        stock s ON
            1=1
            AND t.store = s.store
            AND t.product_name = s.product_name
            AND t.color = s.color
            AND t.product_category = s.product_category
            AND t.retailer = s.retailer
            AND t.brand_name = s.brand_name
            AND t.total_size = s.available_size
            AND t.mrp = s.mrp
--GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9
