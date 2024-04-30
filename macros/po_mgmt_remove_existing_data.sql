{% macro po_mgmt_remove_existing_data(stage_table,domain_table) %}

{% if var('from_date') != '' and var('to_date') != '' %}
   {{ log("Running  if condition : " ~ start_date ~ ", " ~  end_date ) }} 
    DELETE FROM {{domain_table}} 
    WHERE kortex_upld_ts > '{{var('from_date')}}' AND kortex_upld_ts <= '{{var('to_date')}}'

{% else %}    

        {% if is_incremental() %}


          DELETE FROM {{domain_table}} 
            WHERE kortex_upld_ts > '{{from_to_date_logic(stage_table,domain_table)[0]}}'
             AND kortex_upld_ts <=  '{{from_to_date_logic(stage_table,domain_table)[1]}}'


        {%endif%} 
{% endif %}

{% endmacro %}