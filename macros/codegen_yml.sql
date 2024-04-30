{% macro codegen_yml() %}

{{ codegen.generate_source(schema_name= 'stage', table_names = '["KLA_ECC_PO_HIST_EKBE"]') }}
    
{% endmacro %}