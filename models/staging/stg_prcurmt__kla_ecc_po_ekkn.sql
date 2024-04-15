{{
    config(
        schema = 'stage',
        alias="stg_prcurmt__kla_ecc_po_ekkn",
        tags= ['prcurmt'],
        transient = False,
        materialized='ephemeral'
    )
}}
with renaming_fields as (
    select *
            
            from {{ source('stage', 'kla_ecc_po_ekkn') }}
        
            
    )
select EBELN as pur_doc_nbr,
            EBELP as pur_doc_line_nbr,
            ZEKKN as pur_doc_line_acct_asgmt_seq_nbr,
            LOEKZ as pur_acct_asgmt_del_ind,
            AEDAT as pur_acct_asgmt_cre_dt,
            MENGE as pur_acct_asgmt_qty,
            VPROZ as mult_acct_asgmt_distbn_pct,
            NETWR as net_order_val,
            SAKTO as gl_acct_nbr,
            KOSTL as cost_ctr_nbr,
            VBELN as sales_order_nbr,
            VBELP as sales_order_line_nbr,
            VETEN as delvry_sched_line_nbr,
            KOKRS as cntl_area_cd,
            PRCTR as prft_ctr_cd,
            source_system,
            src_nm,
            kortex_upld_ts,
            kortex_dprct_ts
             from renaming_fields