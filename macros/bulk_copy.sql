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

        {% set type=item[3] %}

        {{ log("Executing insert_metadata macro " ~ table ~ ", " ~ s3_path) }}

        {% if type == 'csv' %}
            

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

        {% elif type == 'parquet' %}

            {% set copy_cmd %}

               COPY INTO car_data
               FROM  @dbt.my_s3_stage_pg/{{ s3_path }}  
               match_by_column_name = case_insensitive
                
            {% endset %}

             {{ log("Running some_loop: " ~ table ~ ", " ~ s3_path~ ", " ~ copy_cmd) }}

            {% if execute %}

                {% set insert_result = run_query(copy_cmd) %}
                    
                {{ log(insert_result, info=true) }}


            {%else%}

                {{ exceptions.raise_compiler_error("Please check the meta_info results " ) }}

            {% endif %}

        {% endif %}
        
        {% set sql_qry %}

                SELECT rows_produced,query_id
                FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY())
                WHERE QUERY_ID = LAST_QUERY_ID()
                
        {% endset %}

        {% set results = run_query(sql_qry) %}

        {% if execute %}
                {% set results_list = results.rows %}
        {% else %}
                {% set results_list = [] %}
        {% endif %}

        {{ log("Running results_list: " ~ results_list) }}

        {% for item in results_list %}

                 {% set rows_produced= item.ROWS_PRODUCED %}
                 {% set query_id= item.QUERY_ID %}


                {{ log("checking record_count " ~ rows_produced ~ ", " ~ query_id) }}

                {% if rows_produced > 0 %}

                        {% set sql_stmt %}
                        insert into  dbt.store_meta_info_copy_cmd
                        with cte as (SELECT "file" as s3_path,"status" as status,"rows_parsed" as rows_parsed,"rows_loaded" as rows_loaded 
                            FROM TABLE(RESULT_SCAN('{{ query_id }}')))
                            select *,'{{ table }}' as target_table,current_timestamp() as load_timestamp from cte  
                            
                        {% endset %}

                        {% if execute %}
                            {% set results = run_query(sql_stmt) %}
                        {% else %}

                            {{ log("somwthing went wrong", info=True) }}
                
                        {% endif %}

                {% endif %}



        {% endfor %}
        
    {% endfor %}





    
{% endmacro %}
