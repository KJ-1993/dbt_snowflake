{{
    config(
    materialized ='ephemeral'
    )
}}

with stg_prcurmt__kla_ecc_po_ekko as(
    select * from {{ ref('stg_prcurmt__kla_ecc_po_ekko') }}
),
final as(
    select 
      pur_doc_nbr,
      co_cd,
      vndr_nbr,
      cre_dt,
      pur_doc_dt,
      curr_cd,
      del_ind,
      cre_user_id,
      pur_group_cd,
      pymt_term_cd,
      pur_doc_catg_type_cd,
      pur_doc_type_cd,
      pur_org_cd,
      valid_start_dt,
      valid_end_dt,
      intrnl_ref_txt,
      tax_rtn_ctry_cd,
      altn_pay_to_vndr_nbr,
      sply_vndr_nbr,
      ship_from_loc_nbr,
      inco_term1_cd,
      inco_term2_txt,
      cash_disc_term1_day_nbr,
      cash_disc_term2_day_nbr,
      cash_disc_term3_day_nbr,
      cash_disc1_pct,
      cash_disc2_pct,
      md5( src_nm||pur_doc_nbr) as hash_key ,
      kortex_upld_ts,
      kortex_dprct_ts,
      src_nm
    from stg_prcurmt__kla_ecc_po_ekko
)
select * from final