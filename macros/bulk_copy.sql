{% macro bulk_copy() %}


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

        {{ log("Executing insert_metadata macro " ~ table ~ ", " ~ s3_path) }}

        {% set copy_cmd %}

            copy into  {{ table }}
            from @my_s3_stage/{{ s3_path }}
            
        {% endset %}

        {{ log("Running some_loop: " ~ table ~ ", " ~ s3_path~ ", " ~ copy_cmd) }}

        {% if execute %}

            {% set insert_result = run_query(copy_cmd) %}
                
            {{ log(insert_result, info=true) }}
        {%else%}

            {{ exceptions.raise_compiler_error("Please check the meta_info results " ) }}

        {% endif %}

        {% set store_meta_info_qry %}

        insert into  dbt.store_meta_info_copy_cmd
       with cte as (SELECT "file" as s3_path,"status" as status,"rows_parsed" as rows_parsed,"rows_loaded" as rows_loaded 
       FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())))
       select *,'{{table}}' as target_table from cte         

        {% endset %}

        {% set results = run_query(store_meta_info_qry) %}
        
    {% endfor %}





    
{% endmacro %}
