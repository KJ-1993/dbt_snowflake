{{
    config(
        schema = 'stage',
        alias="stg_prcurmt__kla_ecc_po_hist_ekbe",
        tags= ['prcurmt']
    )
}}
with kla_ecc_po_hist_ekbe as(
    select * from {{ source('stage', 'kla_ecc_po_hist_ekbe') }}
),
final as (

   select 
		EBELN AS pur_doc_nbr,
		EBELP AS pur_doc_line_nbr,
		ZEKKN AS pur_doc_line_acct_asgmt_seq_nbr,
		VGABE AS pur_trns_event_type_cd,
		GJAHR AS matrl_doc_cal_yr,
		BELNR AS matrl_doc_nbr,
		BUZEI AS matrl_doc_line_nbr,
		BEWTP AS pur_hist_catg_cd,
		BWART AS mvmt_type_cd,
		GRUND AS mvmt_rsn_cd,
		CHARG AS batch_nbr,
		BUDAT AS pur_doc_postng_dt,
		BLDAT AS pur_doc_dt,
		CPUDT AS cre_dt,
		CPUTM AS cre_time,
		ERNAM AS cre_user_id,
		MENGE AS pur_hist_qty,
		BAMNG AS pur_hist_buom_qty,
		BPMNG AS pur_hist_pur_price_uom_qty,
		BPWES AS gr_blok_stock_pur_price_uom_qty,
		WESBS AS gr_blok_stock_puom_qty,
		LSMNG AS delvry_note_qty,
		LSMEH AS delvry_note_uom,
		DMBTR AS local_curr_val, 
		WRBTR AS doc_val,
		WAERS AS curr_cd, 
		AREWR AS acct_clrng_local_curr_val,
		REEWR AS entry_invc_local_curr_val, 
		AREWW AS acct_clrng_val,
		HSWAE AS local_curr_cd,
		AREWB AS acct_clrng_pur_doc_curr_val,
		REWRB AS invc_pur_doc_curr_val,
		MWSKZ AS pur_tax_cd,
		SHKZG AS debit_credt_cd,
		ELIKZ AS delvry_cmplt_ind,
		XBLNR AS manl_ref_nbr,
		LFBNR AS ref_doc_nbr,
		LFPOS AS ref_doc_line_nbr,
		ETENS AS seq_confm_nbr,
		LFGJA AS ref_doc_fisc_yr,
		MATNR AS matrl_nbr,
		WERKS AS loc_nbr,
		src_nm AS src_nm,
		kortex_upld_ts AS kortex_upld_ts,
		kortex_dprct_ts AS kortex_dprct_ts
	from kla_ecc_po_hist_ekbe		
			
	)
select * from final