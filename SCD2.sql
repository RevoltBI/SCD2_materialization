{% materialization l00_history_increment, adapter='snowflake' %}

    {# /*
        create temporary table in database based on sql query
        get column names
    */ #}


    {{ log(" REVOLT-BI Message: RUN_ID - " ~ invocation_id ) }} ## id runu

    {% set start_time = modules.datetime.datetime.now() %}

    {% set HASH_TYPE = 256 %}   {# /* 224, 256 ,384, 512 */ #}

    {% set temp_input_table = database ~ "." ~ schema ~ ".temp_input_table"  %}

    {%- call statement('create_temp_input_table') -%}
      {{ create_table_as(True, temp_input_table , sql) }}
    {%- endcall -%}



     {%- set input_relation = [] -%}

    {%- for i in range(sql|length-4) -%}
            {%- if sql[i:i+4]|upper == 'FROM' -%}
                {% set step1 = sql[i+4:]|trim %}
                {% set step2 = step1|replace('-','') %}
                {% set step3 = step2|replace(' ','.') %}
                {% set step3 = step2|replace('"','') %}
                {% set step4 = step3.split('.') %}
                {% for word in step4 %}
                      {%- if input_relation.append(word) %} {% endif -%}
                {% endfor %}
                {% for word in range(3) %}
                      {%- if input_relation.append('') %} {% endif -%}
                {% endfor %}
            {%- endif -%}
    {%- endfor %}

     {{ log(" REVOLT-BI Message: input relation - " ~ input_relation ) }}


    {% set temp_columns_info = adapter.get_columns_in_relation( temp_input_table ) %}
    {% set temp_columns = [] %}
    {% for col in temp_columns_info %}
        {{ temp_columns.append( col.name ) }}
    {% endfor %}

    {{ log(" REVOLT-BI Message:  temporary table " ~ temp_input_table ~ " was sucesfully created!") }}
    {{ log(" REVOLT-BI Message:  Temporary table columns: " ~ temp_columns) }}


    {#
    {% set temp_load_type = database ~ "." ~ schema ~ ".temp_load_type"  %}
    {%- set load_type_sql = load_type_table( table ) -%}
    {% call statement('load_type') -%}
      {{ create_table_as(True, temp_load_type , load_type_sql) }}
    {%- endcall %}
    #}

    {%- set database_ = input_relation[0] -%}
    {%- set schema_ = input_relation[1] -%}
    {%- set table_ = input_relation[2] -%}

    {%- call statement('get_column_values', fetch_result=true) %}

        WITH TAB AS (SELECT    UPPER(trg_database) AS trg_database
                      ,UPPER(trg_schema) AS trg_schema
                      ,UPPER(trg_table) AS trg_table
                      ,TYPE
                      ,START_DATE
        {# /*
              Changing fixed DB to database_ which is a variable for database from input
         */ #}
        FROM DWH_RAW_QA.MAINTENANCE.DAILY_RAW_LOAD  --{{database_|upper}}.MAINTENANCE.DAILY_RAW_LOAD
        QUALIFY ROW_NUMBER() OVER (PARTITION BY trg_database ,trg_schema, trg_table ORDER BY id DESC) = 1)
        SELECT TYPE
        FROM TAB
        WHERE trg_database = '{{database_|upper}}'
            AND trg_schema = '{{schema_|upper}}'
            AND trg_table = '{{table_|upper}}'

    {%- endcall -%}

    {%- set value_list = load_result('get_column_values') -%}
    {{ log(" REVOLT-BI Message:  Load Type: " ~ value_list) }}

    {% if value_list['data']|length == 0 %}
      {% set load_type = 'INCR' %}  
      {{ log(" REVOLT-BI Message:  Load type was assigned automaticaly!!! (FULL , INCR) - check table  " ~ database_|upper ~ ".MAINTENANCE.DAILY_RAW_LOAD ") }}
    {% else %}
      {% set load_type = value_list['data'][0][0]|upper %}
    {% endif %}

    {{ log(" REVOLT-BI Message:  Load Type: " ~ load_type) }}

    {# /*
        get varialbes from config
        variables contain the names of specific columns
        get target relation (table where we will write)
    */ #}

    {%- set id_columns = config.get('id_columns') -%}
    {%- set time_column = config.get('time_column') -%}
    {%- set date_deleted_column = config.get('date_deleted_column') -%}
    {%- if date_deleted_column == None -%}
      {%- set date_deleted_column = config.get('isdeleted_column') -%}
    {%- endif -%}

    {%- set ADDED_COLUMNS_NAMES = config.get('added_column_names') -%}
    {%- if ADDED_COLUMNS_NAMES == None or ADDED_COLUMNS_NAMES|length != 4 -%}
      {%- set ADDED_COLUMNS_NAMES = ['HASHED_ID','HASH_CHANGE_DETECTION','DWH_VALID_FROM','DWH_VALID_TO','DWH_DATE_DELETED'] -%}
    {%- endif -%}

    {%- set target_relation = this -%}

    {{ log(" REVOLT-BI Message:  columns variables was loaded!") }}
    {{ log(" REVOLT-BI Message:  ID Columns: " ~ id_columns) }}
    {{ log(" REVOLT-BI Message:  TIME Column: " ~ time_column) }}
    {{ log(" REVOLT-BI Message:  DELETED Column: " ~ date_deleted_column) }}
    {{ log(" \n REVOLT-BI Warning: ID and TIME columns must be in Temporary table columns!!! \n If not, change SQL query! ") }}


    {%- set database_ = this.database -%}
    {%- set schema_ = this.schema -%}
    {%- set table_ = this.name -%}
    {%- set target_relation_test = adapter.get_relation( database=database_,
                                                    schema=schema_,
                                                    identifier=table_) -%}

    {%- set table_exists = target_relation_test is not none -%}

    {%- if load_type == 'INCR' -%}

          {%- if table_exists -%}

              {{ log(" REVOLT-BI Message:  ----------------------------- UPDATE - INCREMENT ---------------------------------------") }}


              {%- set relation_input = api.Relation.create( database=database,
                                                            schema=schema,
                                                            identifier='temp_input_table') -%}

              {% set temp_origin_target_table = database ~ "." ~ schema ~ ".origin_target_table"  %}
    
              {%- call statement('create_origin_target_table') -%}
                {{ create_table_as(True, temp_origin_target_table , "SELECT * FROM " ~ target_relation_test) }}
              {%- endcall -%}                                              

              {% set target_columns_info = adapter.get_columns_in_relation( target_relation ) %}
              {% set deleted_columns_info = adapter.get_missing_columns( target_relation, relation_input ) %}
              {% set added_columns_info = adapter.get_missing_columns( relation_input, target_relation ) %}

              {% set target_columns = [] %}
              {% for col in target_columns_info %}
                {% if col.name not in ADDED_COLUMNS_NAMES %}
                {{ target_columns.append( col.name ) }}
                {% endif %}
              {% endfor %}

              {% set deleted_columns = [] %}
              {% for col in deleted_columns_info %}
                {% if col.name not in ADDED_COLUMNS_NAMES %}
                {{ deleted_columns.append( col.name ) }}
                {% endif %}
              {% endfor %}

              {% set added_columns = [] %}
              {% for col in added_columns_info %}
                {% if col.name not in ('DWH_UPDATED',time_column) %}
                {{ added_columns.append( col.name ) }}
                {% endif %}
              {% endfor %}

              {{ log(" REVOLT-BI Message:  target columns: " ~ target_columns ) }}
              {{ log(" REVOLT-BI Message:  deleted columns: " ~ deleted_columns ) }}
              {{ log(" REVOLT-BI Message:  added columns: " ~ added_columns ) }}



                  {%- set sql_1 = "SELECT * FROM " ~ temp_input_table -%}
                  {%- set sql_prep_1 = SCD2_query_prep1_incr( sql_1, target_relation, id_columns, time_column,
                                                          date_deleted_column, target_columns, deleted_columns, added_columns, ADDED_COLUMNS_NAMES ) -%}
                  {{ log(" REVOLT-BI Message:  first prep SQL queries were sucesfully created!") }}

                    {% set temp_table_prep1 = database ~ "." ~ schema ~ ".temp_table_prep1"  %}
                    {%- call statement('create_temp_table_prep1') -%}
                      {{ create_table_as(True, temp_table_prep1 , sql_prep_1) }}
                    {%- endcall -%}
                  {{ log(" REVOLT-BI Message:  temporary table " ~ temp_table_prep1 ~ " was sucesfully created!") }}



                  {% set temp_1_columns_info = adapter.get_columns_in_relation( temp_table_prep1 ) %}
                  {% set temp_1_columns = [] %}
                  {% for col in temp_1_columns_info %}
                      {{ temp_1_columns.append( col.name ) }}
                  {% endfor %}
                  {{ log(" REVOLT-BI Message:  temp 1 columns: " ~ temp_1_columns ) }}

                  {%- set sql_2 = "SELECT * FROM " ~ temp_table_prep1 -%}
                  {%- set sql_prep_2 = SCD2_query_prep2_incr( sql_2, id_columns, time_column, date_deleted_column, temp_1_columns, ADDED_COLUMNS_NAMES, HASH_TYPE ) -%}
                  {{ log(" REVOLT-BI Message:  second prep SQL queries were sucesfully created!") }}

                    {% set temp_table_prep2 = database ~ "." ~ schema ~ ".temp_table_prep2"  %}
                    {%- call statement('create_temp_table_prep2') -%}
                      {{ create_table_as(True, temp_table_prep2 , sql_prep_2) }}
                    {%- endcall -%}
                  {{ log(" REVOLT-BI Message:  temporary table " ~ temp_table_prep2 ~ " was sucesfully created!") }}


                  {% for col in added_columns_info %}
                    {% if col.name not in ('DWH_UPDATED',time_column) %}
                    {%- set add_column_sql = add_column_in_target( target_relation, col.name , col.dtype ) -%}
                    {% call statement('add_column_to_target') -%}
                      {{ add_column_sql  }}
                    {%- endcall %}
                    {{ log(" REVOLT-BI Message:  column " ~ col.name ~ " was added to target table!") }}
                    {% endif %}
                  {% endfor %}


                  {% set all_columns_info = adapter.get_columns_in_relation( temp_table_prep2 ) %}
                  {% set all_columns = [] %}
                  {% for col in all_columns_info %}
                    {{ all_columns.append( col.name ) }}
                  {% endfor %}
                  {%- set final_sql_query = SCD2_final_incr( temp_table_prep2, target_relation, id_columns, all_columns,date_deleted_column, ADDED_COLUMNS_NAMES ) -%}
                  {{ log(" REVOLT-BI Message:  final SQL queries were sucesfully created!") }}


                      {% call statement('main') -%}
                        {{ final_sql_query  }}
                      {%- endcall %}

                  {{ log(" REVOLT-BI Message:  SCD2 was sucesfully finished!!!") }}

                  {%- call statement('inserted_values', fetch_result=true) %}  
    
                  SELECT COUNT(*) FROM (
                                          SELECT * FROM {{target_relation}} 
                                          EXCEPT
                                          SELECT * FROM {{temp_origin_target_table}} )
                  
                  {%- endcall -%}
                  {%- set inserted_values_list = load_result('inserted_values') -%}
                  {% set inserted_values = inserted_values_list['data'][0][0] %}


          {%- else -%}

                  {{ log(" REVOLT-BI Message:  ----------------------------- CREATE - INCREMENT ---------------------------------------") }}


                  {% set temp_hash_id_table = database ~ "." ~ schema ~ ".temp_hash_id_table"  %}
                  {%- call statement('create_temp_hash_id_table') -%}
                    {{ create_table_as(True, temp_hash_id_table , hash_id_incr(  temp_input_table, id_columns,
                                                                            temp_columns, time_column, date_deleted_column, ADDED_COLUMNS_NAMES )) }}
                  {%- endcall -%}


              {%- set sql_2 = "SELECT * FROM " ~ temp_hash_id_table -%}
              {%- set sql_prep_2 = SCD2_query_prep2_incr( sql_2, id_columns, time_column, date_deleted_column, temp_columns, ADDED_COLUMNS_NAMES, HASH_TYPE ) -%}
              {{ log(" REVOLT-BI Message:  second prep SQL queries were sucesfully created!") }}

                  {%- call statement('main') -%}
                    {{ create_table_as(False, target_relation , sql_prep_2) }}
                  {%- endcall -%}
              {{ log(" REVOLT-BI Message:  SCD2 table " ~ target_relation ~ " was sucesfully created!!!") }}

              {%- call statement('inserted_values', fetch_result=true) %}  SELECT COUNT(*) FROM {{target_relation}} {%- endcall -%}
              {%- set inserted_values_list = load_result('inserted_values') -%}
              {% set inserted_values = inserted_values_list['data'][0][0] %}

          {%- endif -%}

    {%- else -%}

          {%- if table_exists -%} 

                {{ log(" REVOLT-BI Message:  ----------------------------- UPDATE - FULL LOAD ---------------------------------------") }}

                {%- set relation_input = api.Relation.create( database=database, schema=schema, identifier='temp_input_table') -%}
                {{ log(" REVOLT-BI Message:  ----------------------------- UPDATE - FULL LOAD ---------------------------------------") }}
                {{ log(" REVOLT-BI Message:  ----------------------------- UPDATE - FULL LOAD ---------------------------------------") }}
                {{ log(" REVOLT-BI Message:  schema: " ~ schema) }}

                {{ log(" REVOLT-BI Message:  database: " ~ database) }}

                {{ log(" REVOLT-BI Message:  relation_input: " ~ relation_input) }}

                {% set temp_origin_target_table = database ~ "." ~ schema ~ ".origin_target_table"  %}
      
                {%- call statement('create_origin_target_table') -%}
                  {{ create_table_as(True, temp_origin_target_table , "SELECT * FROM " ~ target_relation_test) }}
                {%- endcall -%}    

                {% set target_columns_info = adapter.get_columns_in_relation( target_relation ) %}
                {% set deleted_columns_info = adapter.get_missing_columns( target_relation, relation_input ) %}
                {% set added_columns_info = adapter.get_missing_columns( relation_input, target_relation ) %}

                {% set target_columns = [] %}
                {% for col in target_columns_info %}
                  {% if col.name not in ADDED_COLUMNS_NAMES %}
                  {{ target_columns.append( col.name ) }}
                  {% endif %}
                {% endfor %}

                {% set deleted_columns = [] %}
                {% for col in deleted_columns_info %}
                  {% if col.name not in ADDED_COLUMNS_NAMES %}
                  {{ deleted_columns.append( col.name ) }}
                  {% endif %}
                {% endfor %}

                {% set added_columns = [] %}
                {% for col in added_columns_info %}
                  {% if col.name not in ('DWH_UPDATED',time_column) %}
                  {{ added_columns.append( col.name ) }}
                  {% endif %}
                {% endfor %}

                {{ log(" REVOLT-BI Message:  target columns: " ~ target_columns ) }}
                {{ log(" REVOLT-BI Message:  deleted columns: " ~ deleted_columns ) }}
                {{ log(" REVOLT-BI Message:  added columns: " ~ added_columns ) }}


                    {% for col in added_columns_info %}
                      {% if col.name not in ('DWH_UPDATED',time_column) %}
                      {%- set add_column_sql = add_column_in_target( target_relation, col.name , col.dtype ) -%}
                      {% call statement('add_column_to_target') -%}
                        {{ add_column_sql  }}
                      {%- endcall %}
                      {{ log(" REVOLT-BI Message:  column " ~ col.name ~ " was added to target table!") }}
                      {% endif %}
                    {% endfor %}





                        {% set temp_upadate_table = database ~ "." ~ schema ~ ".temp_upadate_table"  %}
                        {%- call statement('prep1') -%}
                          {{ create_table_as(False, temp_upadate_table , full_update(  temp_input_table, target_relation, id_columns, temp_columns,
                                                                                    time_column,  ADDED_COLUMNS_NAMES, date_deleted_column, HASH_TYPE )) }}
                        {%- endcall -%}


                    {{ log(" REVOLT-BI Message:  temporary update table was sucesfully created!!!") }}

                    {% set all_columns_info = adapter.get_columns_in_relation( temp_upadate_table ) %}
                    {% set all_columns = [] %}
                    {% for col in all_columns_info %}
                      {{ all_columns.append( col.name ) }}
                    {% endfor %}

                    {%- set final_marge_query = merge_tables( temp_upadate_table, target_relation, id_columns, all_columns,date_deleted_column, ADDED_COLUMNS_NAMES ) -%}
                    {{ log(" REVOLT-BI Message:  final SQL queries were sucesfully created!") }}


                        {% call statement('main') -%}
                          {{ final_marge_query  }}
                        {%- endcall %}


                    {{ log(" REVOLT-BI Message:  SCD2 table " ~ target_relation ~ " was sucesfully upadated!!!") }}

                    {%- call statement('inserted_values', fetch_result=true) %}  
    
                    SELECT COUNT(*) FROM (
                                            SELECT * FROM {{target_relation}} 
                                            EXCEPT
                                            SELECT * FROM {{temp_origin_target_table}} )
                    
                    {%- endcall -%}
                    {%- set inserted_values_list = load_result('inserted_values') -%}
                    {% set inserted_values = inserted_values_list['data'][0][0] %}


          {%- else -%}

                      {{ log(" REVOLT-BI Message:  ----------------------------- CREATE - FULL LOAD ---------------------------------------") }}


                        {%- call statement('main') -%}
                          {{ create_table_as(False, target_relation , full_create(  temp_input_table, id_columns, temp_columns,
                                                                                    time_column,  ADDED_COLUMNS_NAMES, date_deleted_column, HASH_TYPE )) }}
                        {%- endcall -%}

                    {{ log(" REVOLT-BI Message:  SCD2 table " ~ target_relation ~ " was sucesfully created!!!") }}

                    {%- call statement('inserted_values', fetch_result=true) %}  SELECT COUNT(*) FROM {{temp_input_table}} {%- endcall -%}
                    {%- set inserted_values_list = load_result('inserted_values') -%}
                    {% set inserted_values = inserted_values_list['data'][0][0] %}

          {%- endif -%}
    {%- endif -%}

                                              {# /* ------------- Log table ------------ */ #}


  {% set end_time = modules.datetime.datetime.now() %}
  {% set time_diff = end_time - start_time %}

  {{ log(" REVOLT-BI Message:  duration - seconds : " ~ time_diff.seconds ) }}
  {{ log(" REVOLT-BI Message:  inserted values (count) : " ~ inserted_values ) }}

  {% call statement('log_table') -%}   -- {{input_relation[0]|upper}}
      INSERT INTO DWH_RAW_QA.MAINTENANCE.DAILY_HISTORY_LOAD (RUN_ID, TRG_DATABASE,TRG_SCHEMA,TRG_TABLE,PROCESSED_RECORDS,START_DATE,END_DATE,DURATION,TYPE,SUCCESSFUL)
      VALUES ('{{invocation_id}}','{{input_relation[0]|upper}}','{{input_relation[1]|upper}}','{{input_relation[2]|upper}}',{{inserted_values}},
      '{{start_time}}','{{end_time}}',{{time_diff.seconds}},'{{load_type}}',1)
  {%- endcall %}

  {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}



{% macro merge_tables(  temp_upadate_table,
                        target_relation,
                        id_columns,
                        all_columns,
                        date_deleted_column,
                        ADDED_COLUMNS_NAMES
                     ) %}
 {# /* ------------- INCREMENTAL WRITE to target table ------------ */ #}
--merge_tables
MERGE INTO {{ target_relation }} t USING {{ temp_upadate_table }} s
    ON  s."{{ADDED_COLUMNS_NAMES[2]}}" = t."{{ADDED_COLUMNS_NAMES[2]}}"
        {%- if id_columns is iterable and (id_columns is not string and id_columns is not mapping) %}
          {%- for col in id_columns %}
            AND s."{{ col }}" = t."{{ col }}"
          {%- endfor %}
        {%- else %}
            AND s."{{ id_columns }}" = t."{{ id_columns }}"
        {%- endif %}
    WHEN MATCHED THEN
        UPDATE SET t."{{ADDED_COLUMNS_NAMES[3]}}" = s."{{ADDED_COLUMNS_NAMES[3]}}" , t."{{ADDED_COLUMNS_NAMES[4]}}" = s."{{ADDED_COLUMNS_NAMES[4]}}"
    WHEN NOT MATCHED THEN
        INSERT  (
                {%- for col in all_columns %}
                  "{{ col }}" {%- if not loop.last -%},{%- endif -%}
                {%- endfor %}
                )
        VALUES  (
                {%- for col in all_columns %}
                  s."{{ col }}" {%- if not loop.last -%},{%- endif -%}
                {%- endfor %}
                );

{% endmacro %}



{% macro full_update (
                        temp_input_table,
                        target_relation,
                        id_columns,
                        all_columns,
                        time_column,
                        ADDED_COLUMNS_NAMES,
                        date_deleted_column,
                        HASH_TYPE
                      ) %}
-- full_update
WITH    NEW_TABLE AS (SELECT  HASH(
                                    {%- if id_columns is iterable and (id_columns is not string and id_columns is not mapping) %}
                                      {%- for col in id_columns %}
                                        "{{ col }}"::STRING {%- if not loop.last -%},{%- endif -%}
                                      {%- endfor %}
                                    {%- else %}
                                        "{{ id_columns }}"::STRING
                                    {%- endif %}
                                    )    AS "{{ADDED_COLUMNS_NAMES[0]}}"
                                    ,SHA2(
                                                        {%- for col in all_columns -%}
                                                          {%- if col not in id_columns and col not in [time_column,ADDED_COLUMNS_NAMES[0],ADDED_COLUMNS_NAMES[1],date_deleted_column,
                                                                                                      ADDED_COLUMNS_NAMES[2],ADDED_COLUMNS_NAMES[3],ADDED_COLUMNS_NAMES[4]] %}
                                                                          IFNULL("{{col}}"::STRING,'nUlL') || '__' ||
                                                          {%- endif %}
                                                        {%- endfor %}
                                                                        '_', {{ HASH_TYPE }}
                                                                        )         AS "{{ADDED_COLUMNS_NAMES[1]}}"
                                ,*
                      FROM {{ temp_input_table }}    ),

        OLD_TABLE AS (SELECT * FROM {{ target_relation }}
                      WHERE "{{ADDED_COLUMNS_NAMES[3]}}" IS NULL   ),

        HISTORY_1 AS (SELECT o."{{ADDED_COLUMNS_NAMES[0]}}"
                                ,o."{{ADDED_COLUMNS_NAMES[1]}}"
                                ,o."{{ADDED_COLUMNS_NAMES[2]}}"
                                ,added."{{time_column}}" AS "{{ADDED_COLUMNS_NAMES[3]}}"
                                ,CASE   ----------------------------------------------------------------------!!!!!!!!!!!!!!!!!!!--------------------!!!!!!!!!!!!!!!!!!!!------------------!!!!!!!!!!!!!!!!!!!!!!!------------------!!!!!!!!!!!!!!!!!!!----------------------
                                        WHEN deleted."{{ADDED_COLUMNS_NAMES[0]}}" IS NULL AND o."{{ADDED_COLUMNS_NAMES[4]}}" IS NULL
                                                                  THEN (SELECT MIN("{{time_column}}") FROM NEW_TABLE)   ---CURRENT_TIMESTAMP()::DATETIME
                                        ELSE o."{{ADDED_COLUMNS_NAMES[4]}}"
                                 END AS "{{ADDED_COLUMNS_NAMES[4]}}"
                            {%- if id_columns is iterable and (id_columns is not string and id_columns is not mapping) %}
                              {%- for col in id_columns %}
                                ,o."{{ col }}"
                              {%- endfor %}
                            {%- else %}
                                ,o."{{ id_columns }}"
                            {%- endif %}
                             {%- for col in all_columns -%}
                              {%- if col not in id_columns and col not in [time_column,ADDED_COLUMNS_NAMES[0],ADDED_COLUMNS_NAMES[1],date_deleted_column,
                                                                          ADDED_COLUMNS_NAMES[2],ADDED_COLUMNS_NAMES[3],ADDED_COLUMNS_NAMES[4]] %}
                                ,o."{{col}}"
                              {%- endif %}
                            {%- endfor %}
                        FROM OLD_TABLE o
                        LEFT JOIN NEW_TABLE added
                            ON o."{{ADDED_COLUMNS_NAMES[0]}}" = added."{{ADDED_COLUMNS_NAMES[0]}}"
                            AND o."{{ADDED_COLUMNS_NAMES[1]}}" <> added."{{ADDED_COLUMNS_NAMES[1]}}"
                        LEFT JOIN NEW_TABLE deleted
                            ON o."{{ADDED_COLUMNS_NAMES[0]}}" = deleted."{{ADDED_COLUMNS_NAMES[0]}}"  ),

        HISTORY_2 AS (SELECT n."{{ADDED_COLUMNS_NAMES[0]}}"
                              ,n."{{ADDED_COLUMNS_NAMES[1]}}"
                              ,n."{{time_column}}" AS "{{ADDED_COLUMNS_NAMES[2]}}"
                              ,NULL AS "{{ADDED_COLUMNS_NAMES[3]}}"
                              ,NULL AS "{{ADDED_COLUMNS_NAMES[4]}}"
                              {%- if id_columns is iterable and (id_columns is not string and id_columns is not mapping) %}
                              {%- for col in id_columns %}
                                ,n."{{ col }}"
                              {%- endfor %}
                            {%- else %}
                                ,n."{{ id_columns }}"
                            {%- endif %}
                             {%- for col in all_columns -%}
                              {%- if col not in id_columns and col not in [time_column,ADDED_COLUMNS_NAMES[0],ADDED_COLUMNS_NAMES[1],date_deleted_column,
                                                                          ADDED_COLUMNS_NAMES[2],ADDED_COLUMNS_NAMES[3],ADDED_COLUMNS_NAMES[4]] %}
                                ,n."{{col}}"
                              {%- endif %}
                            {%- endfor %}
                      FROM OLD_TABLE o
                      RIGHT JOIN NEW_TABLE n
                          ON o."{{ADDED_COLUMNS_NAMES[0]}}" = n."{{ADDED_COLUMNS_NAMES[0]}}"
                          AND o."{{ADDED_COLUMNS_NAMES[1]}}" = n."{{ADDED_COLUMNS_NAMES[1]}}"
                      WHERE o."{{ADDED_COLUMNS_NAMES[0]}}" IS NULL )
SELECT * FROM HISTORY_1
UNION ALL
SELECT * FROM HISTORY_2

{% endmacro %}



{% macro full_create(  temp_input_table,
                        id_columns,
                        all_columns,
                        time_column,
                        ADDED_COLUMNS_NAMES,
                        date_deleted_column,
                        HASH_TYPE
                      ) %}
-- ful_create
SELECT HASH(
        {%- if id_columns is iterable and (id_columns is not string and id_columns is not mapping) %}
          {%- for col in id_columns %}
            "{{ col }}"::STRING {%- if not loop.last -%},{%- endif -%}
          {%- endfor %}
        {%- else %}
            "{{ id_columns }}"::STRING
        {%- endif %}
        )    AS "{{ADDED_COLUMNS_NAMES[0]}}"
        ,SHA2(
                            {%- for col in all_columns -%}
                              {%- if col not in id_columns and col not in [time_column,ADDED_COLUMNS_NAMES[0],ADDED_COLUMNS_NAMES[1],date_deleted_column,
                                                                          ADDED_COLUMNS_NAMES[2],ADDED_COLUMNS_NAMES[3],ADDED_COLUMNS_NAMES[4]] %}
                                              IFNULL("{{col}}"::STRING,'nUlL') || '__' ||
                              {%- endif %}
                            {%- endfor %}
                                             '_', {{ HASH_TYPE }}
                                            )         AS "{{ADDED_COLUMNS_NAMES[1]}}"

          ,"{{time_column}}" AS "{{ADDED_COLUMNS_NAMES[2]}}"
          ,NULL AS "{{ADDED_COLUMNS_NAMES[3]}}"
          ,NULL AS "{{ADDED_COLUMNS_NAMES[4]}}"
        {%- for col in all_columns %}
          {%- if col not in [time_column, date_deleted_column] %}
            ,"{{ col }}"
          {%- endif %}
        {%- endfor %}
FROM {{ temp_input_table }}

{% endmacro %}




{% macro SCD2_query_prep1_incr(  sql_1,
                            target_relation,
                            id_columns,
                            time_column,
                            date_deleted_column,
                            target_columns,
                            deleted_columns,
                            added_columns,
                            ADDED_COLUMNS_NAMES
                             ) %}

--SCD2_query_prep1_incr
WITH  INPUT_TABLE AS ( {{ sql_1 }} ),
      NEW_RECORDS AS (SELECT   DISTINCT
                              {%- if id_columns is iterable and (id_columns is not string and id_columns is not mapping) %}
                                {%- for col in id_columns %}
                                                    n."{{ col }}",
                                {%- endfor %}
                              {%- else %}
                                                    n."{{ id_columns }}",
                              {%- endif %}
                                                    n."{{time_column}}" AS "DWH_UPDATED"
                              {%- for col in target_columns %}
                                {%- if col not in id_columns and col not in [time_column, date_deleted_column] and col not in deleted_columns %}
                                                    ,n."{{ col }}"
                                {%- endif %}
                              {%- endfor %}
                              {%- if deleted_columns != [] -%}
                                {%- for col in deleted_columns %}
                                                    ,NULL AS "{{ col }}"
                                {%- endfor %}
                              {%- endif %}
                              {%- if added_columns != [] -%}
                                {%- for col in added_columns %}
                                                    ,"{{ col }}"
                                {%- endfor %}
                              {%- endif %}
                                            ,HASH(
                              {%- if id_columns is iterable and (id_columns is not string and id_columns is not mapping) %}
                                {%- for col in id_columns %}
                                                    n."{{ col }}"::STRING {%- if not loop.last %},{% endif -%}
                                {%- endfor %}
                              {%- else %}
                                                    n."{{ id_columns }}"::STRING
                              {%- endif %})  AS "{{ADDED_COLUMNS_NAMES[0]}}"
                                                    ,n."{{date_deleted_column}}"
                              FROM INPUT_TABLE n
                              LEFT JOIN {{ target_relation }} h
                                    ON  {% if id_columns is iterable and (id_columns is not string and id_columns is not mapping) %}
                                          {%- for col in id_columns -%}
                                                    n."{{ col }}" = h."{{ col }}" {%- if not loop.last %} and {% endif -%}
                                          {%- endfor %}
                                        {%- else %}
                                        n."{{ id_columns }}" = h."{{ id_columns }}"
                                        {%- endif %}
                              LEFT JOIN ( SELECT
                                              {% if id_columns is iterable and (id_columns is not string and id_columns is not mapping) %}
                                                {%- for col in id_columns %}
                                                  "{{ col }}",
                                                {%- endfor %}
                                              {%- else %}
                                                  "{{ id_columns }}",
                                              {%- endif %}
                                                  MAX("{{ADDED_COLUMNS_NAMES[2]}}") AS "dwh_max_valid_from",
                                                  MAX("{{ADDED_COLUMNS_NAMES[3]}}") AS "dwh_max_valid_to"
                                          FROM {{ target_relation }}
                                          GROUP BY
                                              {% if id_columns is iterable and (id_columns is not string and id_columns is not mapping) %}
                                                {%- for col in id_columns -%}
                                                  "{{ col }}" {%- if not loop.last %}, {% endif -%}
                                                {%- endfor -%}
                                              {%- else %}
                                                  "{{ id_columns }}"
                                              {%- endif %}) m
                                    ON  {% if id_columns is iterable and (id_columns is not string and id_columns is not mapping) %}
                                          {%- for col in id_columns -%}
                                                    n."{{ col }}" = m."{{ col }}" {%- if not loop.last %} and {% endif -%}
                                          {%- endfor %}
                                        {%- else %}
                                        n."{{ id_columns }}" = m."{{ id_columns }}"
                                        {%- endif %}
                              WHERE (h."{{ADDED_COLUMNS_NAMES[3]}}" IS NULL AND h."{{ADDED_COLUMNS_NAMES[2]}}" <= n."{{time_column}}")
                                    OR (h."{{ADDED_COLUMNS_NAMES[3]}}" IS NULL AND h."{{ADDED_COLUMNS_NAMES[2]}}" IS NULL)
                                    OR (n."{{time_column}}" >= m."dwh_max_valid_from" AND n."{{time_column}}" >= m."dwh_max_valid_to")
                           ),
      OLD_RECORDS AS (SELECT
                              {%- if id_columns is iterable and (id_columns is not string and id_columns is not mapping) %}
                                {%- for col in id_columns %}
                                                      "{{ col }}",
                                {%- endfor %}
                              {%- else %}
                                                      "{{ id_columns }}",
                              {%- endif %}
                                                      "{{ADDED_COLUMNS_NAMES[2]}}" AS "DWH_UPDATED"
                              {%- for col in target_columns %}
                                {%- if col not in id_columns and col not in [time_column, date_deleted_column]  %}
                                                      ,"{{col}}"
                                {%- endif %}
                              {%- endfor %}
                              {%- if added_columns != [] -%}
                                {%- for col in added_columns %}
                                                      ,NULL AS "{{ col }}"
                                {%- endfor %}
                              {%- endif %}
                                                      ,"{{ADDED_COLUMNS_NAMES[0]}}"
                                                      ,"{{date_deleted_column}}"
                            FROM {{ target_relation }}
                            WHERE "{{ADDED_COLUMNS_NAMES[0]}}" IN (SELECT DISTINCT "{{ADDED_COLUMNS_NAMES[0]}}" FROM NEW_RECORDS)
                                    AND "{{ADDED_COLUMNS_NAMES[3]}}" IS NULL
                            )
SELECT
        {%- for col in target_columns %}
          {%- if col != time_column %}
            "{{ col }}",
          {%- endif %}
        {%- endfor %}
        {%- for col in added_columns %}
            "{{ col }}",
        {%- endfor %}
            "{{ADDED_COLUMNS_NAMES[0]}}",
            "DWH_UPDATED",
            "{{date_deleted_column}}"
FROM NEW_RECORDS
UNION ALL
SELECT
        {%- for col in target_columns %}
          {%- if col != time_column %}
            "{{ col }}",
          {%- endif %}
        {%- endfor %}
        {%- for col in added_columns %}
            "{{ col }}",
        {%- endfor %}
            "{{ADDED_COLUMNS_NAMES[0]}}",
            "DWH_UPDATED",
            "{{date_deleted_column}}"
FROM OLD_RECORDS

{% endmacro %}


{% macro SCD2_query_prep2_incr(  sql_2,
                            id_columns,
                            time_column,
                            date_deleted_column,
                            temp_1_columns,
                            ADDED_COLUMNS_NAMES,
                            HASH_TYPE
                             ) %}

--SCD2_query_prep2_incr
WITH  INPUT_TABLE AS ( {{ sql_2 }} ),
      HASH_TABLE  AS (SELECT
                            {%- if id_columns is iterable and (id_columns is not string and id_columns is not mapping) %}
                              {%- for col in id_columns %}
                                              "{{ col }}",
                              {%- endfor %}
                            {%- else %}
                                              "{{ id_columns }}",
                            {%- endif %}
                                              "{{ADDED_COLUMNS_NAMES[0]}}"
                                              ,"DWH_UPDATED"
                            {%- for col in temp_1_columns %}
                              {%- if col not in id_columns and col not in [time_column, date_deleted_column,ADDED_COLUMNS_NAMES[0],'DWH_UPDATED','FILL_DELETED_DATE'] %}
                                              ,"{{col}}"
                              {%- endif %}
                            {%- endfor %}
                                      ,SHA2(
                            {%- for col in temp_1_columns -%}
                              {%- if col not in id_columns and col not in [time_column, date_deleted_column,ADDED_COLUMNS_NAMES[0],'DWH_UPDATED','FILL_DELETED_DATE'] %}
                                              IFNULL("{{col}}"::STRING,'nUlL') || '__' ||
                              {%- endif %}
                            {%- endfor %}
                                             '_', {{ HASH_TYPE }}
                                            )         AS "{{ADDED_COLUMNS_NAMES[1]}}"
                                              ,"{{date_deleted_column}}"
                    FROM INPUT_TABLE),
     HISTORY_1 AS  (
                    SELECT a."{{ADDED_COLUMNS_NAMES[0]}}" AS "{{ADDED_COLUMNS_NAMES[0]}}"
                            ,a."{{ADDED_COLUMNS_NAMES[1]}}" AS "{{ADDED_COLUMNS_NAMES[1]}}"
                            ,a."DWH_UPDATED" AS "{{ADDED_COLUMNS_NAMES[2]}}"
                            ,b."DWH_UPDATED" AS "{{ADDED_COLUMNS_NAMES[3]}}"
                    FROM HASH_TABLE a
                    JOIN HASH_TABLE b
                        ON  a."{{ADDED_COLUMNS_NAMES[0]}}" = b."{{ADDED_COLUMNS_NAMES[0]}}"
                    WHERE TIMEDIFF('second',a."DWH_UPDATED", b."DWH_UPDATED") > 0
                        AND  b."{{ADDED_COLUMNS_NAMES[1]}}" <> a."{{ADDED_COLUMNS_NAMES[1]}}"
                    QUALIFY ROW_NUMBER() OVER (PARTITION BY a."{{ADDED_COLUMNS_NAMES[0]}}", a."{{ADDED_COLUMNS_NAMES[1]}}"
                                                ORDER BY a."DWH_UPDATED", TIMEDIFF('second',a."DWH_UPDATED", b."DWH_UPDATED")) = 1 ),
            {# /*
                Logic:
                     Pairs with the same HASH are always taken (via join), those with a positive time difference are selected.
                     the smallest difference relative to the HASHi and the initial timestamp (DWH_UPDATED) is selected
             */ #}
     HISTORY_2 AS  (SELECT ht."{{ADDED_COLUMNS_NAMES[0]}}" AS "{{ADDED_COLUMNS_NAMES[0]}}"
                            ,ht."{{ADDED_COLUMNS_NAMES[1]}}" AS "{{ADDED_COLUMNS_NAMES[1]}}"
                            ,ht."DWH_UPDATED" AS "{{ADDED_COLUMNS_NAMES[2]}}"
                            ,NULL AS "{{ADDED_COLUMNS_NAMES[3]}}"
                     FROM HASH_TABLE ht
                     LEFT JOIN (SELECT "{{ADDED_COLUMNS_NAMES[0]}}",MAX("{{ADDED_COLUMNS_NAMES[3]}}") AS "MAX_DATE" FROM HISTORY_1 GROUP BY 1) md
                        ON md."{{ADDED_COLUMNS_NAMES[0]}}" = ht."{{ADDED_COLUMNS_NAMES[0]}}"
                     WHERE (ht."DWH_UPDATED" >= md."MAX_DATE" OR md."MAX_DATE" IS NULL)
                     QUALIFY ROW_NUMBER() OVER (PARTITION BY ht."{{ADDED_COLUMNS_NAMES[0]}}" ORDER BY ht."DWH_UPDATED") = 1) ,
    DATE_DELETED_DETECTION_1 AS (

                                  SELECT ht."{{ADDED_COLUMNS_NAMES[0]}}"
                                        ,ht."{{ADDED_COLUMNS_NAMES[1]}}"
                                        ,MIN(ht."{{date_deleted_column}}") AS "{{date_deleted_column}}"
                                  FROM HASH_TABLE ht
                                  JOIN HISTORY_1 h
                                          ON ht."{{ADDED_COLUMNS_NAMES[0]}}"  = h."{{ADDED_COLUMNS_NAMES[0]}}"
                                              AND ht."{{ADDED_COLUMNS_NAMES[1]}}"  = h."{{ADDED_COLUMNS_NAMES[1]}}"
                                              AND h."{{ADDED_COLUMNS_NAMES[2]}}" <= ht."DWH_UPDATED"  AND ht."DWH_UPDATED" < h."{{ADDED_COLUMNS_NAMES[3]}}"
                                              AND h."{{ADDED_COLUMNS_NAMES[2]}}" < ht."{{date_deleted_column}}" -- AND ht."{{date_deleted_column}}" <= h."{{ADDED_COLUMNS_NAMES[3]}}"
                                  GROUP BY 1,2  ),
    DATE_DELETED_DETECTION_2 AS (

                                  SELECT ht."{{ADDED_COLUMNS_NAMES[0]}}"
                                        ,ht."{{ADDED_COLUMNS_NAMES[1]}}"
                                        ,MIN(ht."{{date_deleted_column}}") AS "{{date_deleted_column}}"
                                  FROM HASH_TABLE ht
                                  JOIN HISTORY_2 h
                                          ON ht."{{ADDED_COLUMNS_NAMES[0]}}"  = h."{{ADDED_COLUMNS_NAMES[0]}}"
                                              AND ht."{{ADDED_COLUMNS_NAMES[1]}}"  = h."{{ADDED_COLUMNS_NAMES[1]}}"
                                              AND h."{{ADDED_COLUMNS_NAMES[2]}}" <= ht."DWH_UPDATED"
                                              AND h."{{ADDED_COLUMNS_NAMES[2]}}" < ht."{{date_deleted_column}}"
                                  GROUP BY 1,2  ),
    UNION_HISTORY AS (
                            SELECT h.*,ddd."{{date_deleted_column}}" FROM HISTORY_1 h
                            LEFT JOIN DATE_DELETED_DETECTION_1 ddd
                                          ON ddd."{{ADDED_COLUMNS_NAMES[0]}}"  = h."{{ADDED_COLUMNS_NAMES[0]}}"
                                              AND ddd."{{ADDED_COLUMNS_NAMES[1]}}"  = h."{{ADDED_COLUMNS_NAMES[1]}}"
                            UNION ALL
                            SELECT h.*,ddd."{{date_deleted_column}}" FROM HISTORY_2 h
                            LEFT JOIN DATE_DELETED_DETECTION_2 ddd
                                          ON ddd."{{ADDED_COLUMNS_NAMES[0]}}"  = h."{{ADDED_COLUMNS_NAMES[0]}}"
                                              AND ddd."{{ADDED_COLUMNS_NAMES[1]}}"  = h."{{ADDED_COLUMNS_NAMES[1]}}"
                        )
SELECT
            h.*
        {%- for col in temp_1_columns %}
          {%- if col not in id_columns and col not in [time_column, date_deleted_column,ADDED_COLUMNS_NAMES[0],'DWH_UPDATED','FILL_DELETED_DATE'] %}
            ,"{{col}}"
          {%- endif %}
        {%- endfor %}
        {%- if id_columns is iterable and (id_columns is not string and id_columns is not mapping) %}
          {%- for col in id_columns %}
            ,"{{ col }}"
          {%- endfor %}
        {%- else %}
            ,"{{ id_columns }}"
        {%- endif %}
FROM UNION_HISTORY h
LEFT JOIN (SELECT DISTINCT "{{ADDED_COLUMNS_NAMES[1]}}"
                            ,"{{ADDED_COLUMNS_NAMES[0]}}"
                        {%- if id_columns is iterable and (id_columns is not string and id_columns is not mapping) %}
                          {%- for col in id_columns %}
                            ,"{{ col }}"
                          {%- endfor %}
                        {%- else %}
                            ,"{{ id_columns }}"
                        {%- endif %}
                        {%- for col in temp_1_columns %}
                          {%- if col not in id_columns and col not in [time_column, date_deleted_column,ADDED_COLUMNS_NAMES[0],'DWH_UPDATED','FILL_DELETED_DATE'] %}
                            ,"{{col}}"
                          {%- endif %}
                        {%- endfor %}
            FROM HASH_TABLE) ht
                ON ht."{{ADDED_COLUMNS_NAMES[1]}}"  = h."{{ADDED_COLUMNS_NAMES[1]}}"
                AND ht."{{ADDED_COLUMNS_NAMES[0]}}"  = h."{{ADDED_COLUMNS_NAMES[0]}}"
ORDER BY "{{ADDED_COLUMNS_NAMES[0]}}","{{ADDED_COLUMNS_NAMES[2]}}"

{% endmacro %}


{% macro SCD2_final_incr(  temp_table_prep2,
                            target_relation,
                            id_columns,
                            all_columns,
                            date_deleted_column,
                            ADDED_COLUMNS_NAMES
                          ) %}
 {# /* ------------- INCREMENTAL WRITE to target table ------------ */ #}
--SCD2_final_incr
MERGE INTO {{ target_relation }} t USING {{ temp_table_prep2 }} s
    ON  s."{{ADDED_COLUMNS_NAMES[2]}}" = t."{{ADDED_COLUMNS_NAMES[2]}}"
        {%- if id_columns is iterable and (id_columns is not string and id_columns is not mapping) %}
          {%- for col in id_columns %}
            AND s."{{ col }}" = t."{{ col }}"
          {%- endfor %}
        {%- else %}
            AND s."{{ id_columns }}" = t."{{ id_columns }}"
        {%- endif %}
    WHEN MATCHED THEN
        UPDATE SET t."{{ADDED_COLUMNS_NAMES[3]}}" = s."{{ADDED_COLUMNS_NAMES[3]}}" , t."{{date_deleted_column}}" = s."{{date_deleted_column}}"
    WHEN NOT MATCHED THEN
        INSERT  (
                {%- for col in all_columns %}
                  "{{ col }}" {%- if not loop.last -%},{%- endif -%}
                {%- endfor %}
                )
        VALUES  (
                {%- for col in all_columns %}
                  s."{{ col }}" {%- if not loop.last -%},{%- endif -%}
                {%- endfor %}
                );

{% endmacro %}


{% macro hash_id_incr(  temp_input_table,
                      id_columns,
                      all_columns,
                      time_column,
                      date_deleted_column,
                      ADDED_COLUMNS_NAMES
                     ) %}
--hash_id_incr
SELECT HASH(
        {%- if id_columns is iterable and (id_columns is not string and id_columns is not mapping) %}
          {%- for col in id_columns %}
            "{{ col }}"::STRING {%- if not loop.last -%},{%- endif -%}
          {%- endfor %}
        {%- else %}
            "{{ id_columns }}"::STRING
        {%- endif %}
        )    AS "{{ADDED_COLUMNS_NAMES[0]}}"
        {%- for col in all_columns %}
          {%- if col not in [time_column, date_deleted_column] %}
            ,"{{ col }}"
          {%- endif %}
        {%- endfor %}
            ,"{{time_column}}" AS "DWH_UPDATED"
            ,"{{date_deleted_column}}" AS "{{date_deleted_column}}"
            ,LAST_VALUE("{{date_deleted_column}}" IGNORE NULLS)
                OVER (PARTITION BY
                                    {%- if id_columns is iterable and (id_columns is not string and id_columns is not mapping) %}
                                      {%- for col in id_columns %}
                                        "{{ col }}" {%- if not loop.last -%},{%- endif -%}
                                      {%- endfor %}
                                    {%- else %}
                                        "{{ id_columns }}"
                                    {%- endif %}
                 order by "{{time_column}}" rows between unbounded preceding and current row) as "FILL_DELETED_DATE"
FROM {{ temp_input_table }}

{% endmacro %}



{% macro add_column_in_target(  target_relation,
                                column_name,
                                column_dtype
                              ) %}

    ALTER TABLE {{ target_relation }} ADD COLUMN "{{column_name}}" {{column_dtype}};

{% endmacro %} 
