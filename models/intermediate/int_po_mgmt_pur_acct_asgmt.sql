{% set start_date= '1999-12-12' %}

{% set end_date= '9999-12-12' %}

{{
    config(
        schema = 'int',
        alias="int_po_mgmt_pur_acct_asgmt",
        tags= ['prcurmt'],
        transient = True
    )
}}

    
{% if  start_date  == '' %}
     {% set sql_qry %}
        SELECT coalesce(DATEADD(day,-7,MAX(kortex_upld_ts)),'1990-01-01') FROM klg_nga_kla.prcurmt.po_mgmt_pur_acct_asgmt
     {% endset %}

     {% set results = run_query(sql_qry) %}
    {% if execute %}
    {# Return the first column #}
    {% set start_date = results.columns[0][0]   %}
    {% else %}
    {% set start_date = [] %}
    {% endif %}
    
    {{ log("Getting start_date : " ~ start_date) }}


{% endif %}


{% if end_date == '' %}

    {% set sql_qry %}
        SELECT GETDATE()::TIMESTAMP_NTZ 
    {% endset %}

    {% set results = run_query(sql_qry) %}
    {% if execute %}
    {# Return the first column #}
    {% set end_date = results.columns[0][0]   %}
    {% else %}
    {% set end_date = [] %}
    {% endif %}

    {{ log("Getting end_date : " ~ end_date) }}

    
{% endif %}

{% if start_date > end_date %}

    {{ exceptions.raise_compiler_error(" `start_date` should be less than`end_date`. Got: " ~ start_date ~ ", " ~ end_date) }}
    
{% endif %}

{% set sql_qry %}
SELECT COUNT(1) FROM {{ source('stage', 'kla_ecc_po_ekkn') }} 
 WHERE kortex_upld_ts > '{{start_date}}' AND kortex_upld_ts <= '{{end_date}}'

{% endset %}

{% set results = run_query(sql_qry) %}
{% if execute %}
    {# Return the first column #}
    {% set record_count = results.columns[0][0]   %}
    {% else %}
    {% set record_count = [] %}
{% endif %}
{{ log("recound_count: : " ~ record_count) }}


with cte as(
select * from {{ ref('stg_prcurmt__kla_ecc_po_ekkn') }}
)
select acc.pur_doc_nbr,
                acc.pur_doc_line_nbr,
                acc.pur_doc_line_acct_asgmt_seq_nbr,
                acc.pur_acct_asgmt_del_ind,
                acc.pur_acct_asgmt_cre_dt,
                acc.pur_acct_asgmt_qty,
                acc.mult_acct_asgmt_distbn_pct,
                acc.net_order_val,
                acc.gl_acct_nbr,
                acc.cost_ctr_nbr,
                acc.sales_order_nbr,
                acc.sales_order_line_nbr,
                acc.delvry_sched_line_nbr,
                acc.cntl_area_cd,
                acc.prft_ctr_cd,
                acc.src_nm,
                md5(acc.source_system || acc.src_nm || acc.pur_doc_nbr || acc.pur_doc_line_nbr || acc.pur_doc_line_acct_asgmt_seq_nbr) as hash_key,
                acc.kortex_upld_ts,
                acc.kortex_dprct_ts from cte acc