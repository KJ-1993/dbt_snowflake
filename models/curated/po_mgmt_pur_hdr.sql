{{
    config(
        materialized='incremental',
        unique_key = ['src_nm','pur_doc_nbr'],
        incremental_strategy='delete+insert',
        schema = 'prcurmt',
        alias="po_mgmt_pur_hdr",
        tags= ['procurement'],
        transient = False
        
    )
}}


select * from {{ ref('int_po_mgmt_pur_hdr') }}
{% if var('from_date') != '' and var('to_date') != '' %}
 
    WHERE kortex_upld_ts > '{{var('from_date')}}' AND kortex_upld_ts <= '{{var('to_date')}}'

{%else%}

    {% if is_incremental() %}
        -- this filter will only be applied on an incremental run
        where kortex_upld_ts > '{{from_to_date_logic('stage.kla_ecc_po_ekko',this)[0]}}'
        and kortex_upld_ts<=  '{{from_to_date_logic('stage.kla_ecc_po_ekko',this)[1]}}'
    {% endif %}
 
    
{% endif %}