{% set start_date=var('from_date') %}

{% set end_date =var('to_date') %}

{{
    config(
        schema = 'int',
        alias="int_po_mgmt_pur_hist_test",
        tags= ['prcurmt'],
        transient = true,
        materialized ='table',
        pre_hook=["{{ po_mgmt_pur_hist_remove_data(start_date,end_date) }}"]
    )
}}
with stg_prcurmt__kla_ecc_po_hist_ekbe as(
    select * from {{ ref('stg_prcurmt__kla_ecc_po_hist_ekbe') }}
),
final as (
    SELECT DISTINCT
					his.pur_doc_nbr,
					his.pur_doc_line_acct_asgmt_seq_nbr,
					his.pur_doc_line_nbr,
					his.pur_trns_event_type_cd,
					his.matrl_doc_cal_yr,
					his.matrl_doc_nbr,
					his.matrl_doc_line_nbr,
					his.pur_hist_catg_cd,
					his.mvmt_type_cd,
					his.mvmt_rsn_cd,
					his.batch_nbr,
					his.pur_doc_postng_dt,
					his.pur_doc_dt,
					his.cre_dt,
					his.cre_time,
					his.cre_user_id,
					his.pur_hist_qty,
					his.pur_hist_buom_qty,
					his.pur_hist_pur_price_uom_qty,
					his.gr_blok_stock_pur_price_uom_qty,
					his.gr_blok_stock_puom_qty,
					his.delvry_note_qty,
					his.delvry_note_uom,
					his.local_curr_val,
					his.doc_val,
					his.curr_cd,
					his.acct_clrng_local_curr_val,
					his.entry_invc_local_curr_val,
					his.acct_clrng_val,
					his.local_curr_cd,
					his.acct_clrng_pur_doc_curr_val,
					his.invc_pur_doc_curr_val,
					his.pur_tax_cd,
					his.debit_credt_cd,
					his.delvry_cmplt_ind,
					his.manl_ref_nbr,
					his.ref_doc_nbr,
					his.ref_doc_line_nbr,
					his.seq_confm_nbr,
					his.ref_doc_fisc_yr,
					his.matrl_nbr,
					his.loc_nbr,
					his.src_nm,
					md5(his.src_nm|| his.pur_doc_nbr||his.pur_doc_line_nbr||his.pur_doc_line_acct_asgmt_seq_nbr||his.pur_trns_event_type_cd||his.matrl_doc_cal_yr||his.matrl_doc_nbr||his.matrl_doc_line_nbr) as hash_key,
					his.kortex_upld_ts,
					his.kortex_dprct_ts
			from stg_prcurmt__kla_ecc_po_hist_ekbe as his
			WHERE 
				his.kortex_upld_ts > '{{from_to_date_logic('stage.kla_ecc_po_hist_ekbe','dev_kla.prcurmt.po_mgmt_pur_hist')[0]}}'
			AND 
				his.kortex_upld_ts <= '{{from_to_date_logic('stage.kla_ecc_po_hist_ekbe','dev_kla.prcurmt.po_mgmt_pur_hist')[1]}}'
			AND
				his.pur_doc_nbr in (select pur_doc_nbr from deva_kla.prcurmt.po_mgmt_pur_hdr)
		
)
select * from final


