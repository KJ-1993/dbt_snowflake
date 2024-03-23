{{ config(
  materialized = 'table',
) }}


{% set var_date= var('str_date') %}


WITH fct_reviews AS (
    SELECT * FROM {{ ref('int_reviews') }}
),
full_moon_dates AS (
    SELECT * FROM {{ ref('seed_full_moon_dates') }}
)

SELECT
  r.*,
  CASE
    WHEN fm.full_moon_date IS NULL THEN 'not full moon'
    ELSE 'full moon'
  END AS is_full_moon,
  to_date('{{ get_thursday_date(var_date) }}','YYYY-MM-DD') as load_timestamp
FROM
  fct_reviews
  r
  LEFT JOIN full_moon_dates
  fm
  ON (TO_DATE(r.review_date) = DATEADD(DAY, 1, fm.full_moon_date))