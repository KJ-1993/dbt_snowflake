{% macro get_thursday_date(strdate) %}


{% set sql %}

select dateadd(day, 3, TRUNC( to_date( {{ strdate }} ), 'WEEK')) as date
    
{% endset %}


{% set results = run_query(sql) %}

    {% if execute %}
        {% set results_list = results.columns[0][0] %}
        {{ log("Running some_macro: " ~ results_list) }}
    {% else %}
        {% set results_list = [] %}
    {% endif %}

    {{ return(results_list) }}


    
{% endmacro %}