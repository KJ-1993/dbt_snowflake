 -- depends_on: {{ ref('po_mgmt_pur_hdr') }}
{{
    config(
        materialized='incremental',
        unique_key = ['src_nm','pur_doc_nbr', 'pur_doc_line_nbr',  'pur_doc_line_acct_asgmt_seq_nbr', 'pur_trns_event_type_cd',
        'matrl_doc_cal_yr','matrl_doc_nbr',
        'matrl_doc_line_nbr'],
        incremental_strategy='delete+insert',
        schema = 'prcurmt',
        alias="po_mgmt_pur_hist",
        tags= ['procurement'],
        transient = False
        
    )
}}

select * from {{ ref('int_po_mgmt_pur_hist') }}
{% if var('from_date') != '' and var('to_date') != '' %}
 
    WHERE kortex_upld_ts > '{{var('from_date')}}' AND kortex_upld_ts <= '{{var('to_date')}}'
    and pur_doc_nbr in (select pur_doc_nbr from {{ ref('po_mgmt_pur_hdr') }})
    
{%else%}

    {% if is_incremental() %}

        WHERE kortex_upld_ts > '{{from_to_date_logic('stage.kla_ecc_po_hist_ekbe',this)[0]}}' 
        AND kortex_upld_ts <= '{{from_to_date_logic('stage.kla_ecc_po_hist_ekbe',this)[1]}}'
        and pur_doc_nbr in (select pur_doc_nbr from {{ ref('po_mgmt_pur_hdr') }})

    {%endif%}

    
{% endif %} 

