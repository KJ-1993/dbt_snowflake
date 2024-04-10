{% macro check_input_var() %}

    {% set query %}

    select * from dbt.meta_info
        
    {% endset %}

    {% set results = run_query(query) %}

    {% if execute %}
        {% set results_list = results.rows %}
    {% else %}
        {% set results_list = [] %}
    {% endif %}

    {{ log("Running results_list: " ~ results_list) }}

    {% for item in results_list %}

        {% set table= item[0] %}

        {% set s3_path=item[1] %}

        {% set type=item[3] %}

        {{ log("Executing insert_metadata macro " ~ table ~ ", " ~ s3_path ~ "," ~ type ) }}

    {%endfor%}
    
{% endmacro %}