{{
    config(
        schema = 'stage',
        alias="stg_prcurmt__kla_ecc_po_ekko",
        tags= ['prcurmt']
    )
}}
with kla_ecc_po_ekko as (
    select * from {{ source('stage', 'kla_ecc_po_ekko') }}
),

final as (

    select 
stg.ebeln as pur_doc_nbr,
        stg.bukrs as co_cd,
        stg.lifnr as vndr_nbr,
        stg.aedat as cre_dt,
        stg.bedat as pur_doc_dt,
        stg.waers as curr_cd,
        stg.loekz as del_ind,
        stg.ernam as cre_user_id,
        stg.ekgrp as pur_group_cd,
        stg.zterm as pymt_term_cd,
        stg.bstyp as pur_doc_catg_type_cd,
        --stg.batxt as pur_doc_catg_type_desc,
        stg.bsart as pur_doc_type_cd,
        stg.ekorg as pur_org_cd,
        stg.kdatb as valid_start_dt,
        stg.kdate as valid_end_dt,
        stg.unsez as intrnl_ref_txt,
        stg.lands as tax_rtn_ctry_cd,
        stg.lifre as altn_pay_to_vndr_nbr,
        stg.llief as sply_vndr_nbr,
        stg.reswk as ship_from_loc_nbr,
        stg.inco1 as inco_term1_cd,
        stg.inco2 as inco_term2_txt,
        stg.zbd1t as cash_disc_term1_day_nbr,
        stg.zbd2t as cash_disc_term2_day_nbr,
        stg.zbd3t as cash_disc_term3_day_nbr,
        stg.zbd1p as cash_disc1_pct,
        stg.zbd2p as cash_disc2_pct,
        md5(src_nm || stg.ebeln) as hash_key,
        stg.kortex_upld_ts,
        stg.kortex_dprct_ts,
        stg.src_nm  as src_nm
    from kla_ecc_po_ekko AS stg

)

select * from final
