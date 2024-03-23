{{
  config(
    materialized = 'view',
    tags=['intermediate_dim']
    )
}} 
WITH src_hosts AS (
    SELECT
        *
    FROM
        {{ ref('stg_homestay__hosts') }}
)
SELECT
    host_id,
    NVL(
        host_name,
        'Anonymous'
    ) AS host_name,
    is_superhost,
    created_at,
    updated_at
FROM
    src_hosts