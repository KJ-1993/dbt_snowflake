SELECT
    *
FROM
    {{ ref('int_listings_cleansed') }}
WHERE minimum_nights < 1
LIMIT 10