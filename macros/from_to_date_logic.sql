{% macro from_to_date_logic(stg_table,domain_table) %}

    {% set start_date=var('from_date') %}

    {% set end_date =var('to_date') %} 

    {% if  start_date  == '' %}
        {% set sql_qry %}
            SELECT coalesce(DATEADD(day,-7,MAX(kortex_upld_ts)),'1990-01-01') FROM {{domain_table}}
        {% endset %}

        {% set results = run_query(sql_qry) %}
        {% if execute %}
        {# Return the first column #}
        {% set start_date = results.columns[0][0]   %}
        {% else %}
        {% set start_date = [] %}
        {% endif %}
        
        {{ log("Getting start_date : " ~ start_date) }}


    {% endif %}

    {% if end_date == '' %}

    {% set sql_qry %}
        SELECT GETDATE()::TIMESTAMP_NTZ 
    {% endset %}

    {% set results = run_query(sql_qry) %}
    {% if execute %}
   
        {% set end_date = results.columns[0][0]   %}
        {% else %}
        {% set end_date = [] %}
    {% endif %}

    {{ log("Getting end_date : " ~ end_date) }}

    {% endif %}

{% if start_date > end_date %}

    {{ exceptions.warn(" `start_date` should be less than`end_date`. Got: " ~ start_date ~ ", " ~ end_date) }}
    
{% endif %}

{% set sql_qry %}
    SELECT COUNT(1) FROM {{stg_table}}  WHERE kortex_upld_ts > '{{start_date}}' AND kortex_upld_ts <= '{{end_date}}'
{% endset %}


{% set results = run_query(sql_qry) %}
    {% if execute %}
    {# Return the first column #}
    {% set record_count = results.columns[0][0]   %}
    {% else %}
    {% set record_count = [] %}
    {% endif %}

    {{ log("recound_count: : " ~ record_count) }}

{% if record_count==0 %}

 {{ exceptions.warn("No records present " ~ recound_count ) }}
    
{% endif %}


{{ return([start_date,end_date]) }}
    
{% endmacro %}