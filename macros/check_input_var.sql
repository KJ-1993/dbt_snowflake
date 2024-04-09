{% macro check_input_var() %}

    {% set input_date= var('str_date') %}

    {{ log("Running input: " ~ input_date) }}

    {% if input_date is None %}

     {{ log("Running loop: " ~ input_date) }}

        
    {% endif %}
    
{% endmacro %}