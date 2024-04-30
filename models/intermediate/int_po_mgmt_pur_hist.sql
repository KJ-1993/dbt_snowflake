{{
    config(
        materialized='ephemeral'
    )
}}

with stg_prcurmt__kla_ecc_po_hist_ekbe as(
    select * from {{ ref('stg_prcurmt__kla_ecc_po_hist_ekbe') }}
),
final as(
    select 
    pur_doc_nbr,
    pur_doc_line_nbr,
    pur_doc_line_acct_asgmt_seq_nbr,
    pur_trns_event_type_cd,
    matrl_doc_cal_yr,
    matrl_doc_nbr,
    matrl_doc_line_nbr,
    pur_hist_catg_cd,
    mvmt_type_cd,
    mvmt_rsn_cd,
    batch_nbr,
    pur_doc_postng_dt,
    pur_doc_dt,
    cre_dt,
    cre_time,
    cre_user_id,
    pur_hist_qty,
    pur_hist_buom_qty,
    pur_hist_pur_price_uom_qty,
    gr_blok_stock_pur_price_uom_qty,
    gr_blok_stock_puom_qty,
    delvry_note_qty,
    delvry_note_uom,
    local_curr_val,
    doc_val,
    curr_cd,
    acct_clrng_local_curr_val,
    entry_invc_local_curr_val,
    acct_clrng_val,
    local_curr_cd,
    acct_clrng_pur_doc_curr_val,
    invc_pur_doc_curr_val,
    pur_tax_cd,
    debit_credt_cd,
    delvry_cmplt_ind,
    manl_ref_nbr,
    ref_doc_nbr,
    ref_doc_line_nbr,
    seq_confm_nbr,
    ref_doc_fisc_yr,
    matrl_nbr,
    loc_nbr,
    src_nm,
    md5(src_nm|| pur_doc_nbr||pur_doc_line_nbr||pur_doc_line_acct_asgmt_seq_nbr||pur_trns_event_type_cd||matrl_doc_cal_yr||matrl_doc_nbr||matrl_doc_line_nbr) as hash_key,
    kortex_upld_ts,
    kortex_dprct_ts
    from stg_prcurmt__kla_ecc_po_hist_ekbe 

) select * from final