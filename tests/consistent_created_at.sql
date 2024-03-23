{{
	config(
		severity='error',
		error_if='>= 1',
		warn_if='> 0',
		fail_calc='coalesce(max(failure_pct),0)',
		store_failures=true,
		schema='audit',
		alias='audit_consistent_created_at',
		tags=['Audit_data']
	)
}}


select count(*) as failure_pct from (
SELECT * FROM {{ ref('int_listings_cleansed') }} l
INNER JOIN {{ ref('int_reviews') }} r
USING (listing_id)
WHERE l.created_at >= r.review_date)