
/* ==============================================
  File: 05_create_stream_task_by_driving_table.sql
  Description:  This script creates a driving table to manage streams and tasks for Iceberg tables in Snowflake.
                It dynamically creates streams and tasks based on the entries in the driving table.
                If the driving table is named different, update the table name in the script accordingly.
                Replace the stored procedure call with the new procedure name if it has been changed.
                Modify the script if to create stream and tasks in different database or schema than in session context.
 ===============================================
 Change History
===============================================
 Date        | Author        | Description
-------------|---------------|------------------------------------------------------
2025-07-10   | J. Ma         | Created
2025-07-25   | J. Ma         | Updated the call in task to update_glue_metadata_location to pass on get_ddl 
===============================================
*/
 
-- create the driving table
  create or replace table iceberg_table_list (id number, snow_db_name varchar, snow_schema_name varchar, snow_table_name varchar, athena_db_name varchar, athena_table_name varchar, task_schedule_minutes number, updated_date datetime); -- populate the driving table with the list of iceberg tables and their corresponding Athena database and table names
-- insert some sample data into the driving table, ie:
 insert into iceberg_table_list values ( 1, 'iceberg_db', 'testsc',  'simple_schema_smi2', 'aj_test', 'iceberg_table_from_boto2', 1, current_date);

-- create streams and tasks for each iceberg table that in driving table


CREATE OR REPLACE PROCEDURE CREATE_STREAMS_AND_TASKS_FOR_TABLES()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
 declare
    rs RESULTSET default (select snow_db_name, snow_schema_name, snow_table_name, athena_db_name, athena_table_name, task_schedule_minutes from iceberg_table_list);
    vw1_cur CURSOR for rs;
    my_sql varchar;
    stream_name varchar;
    task_name varchar ;
begin
    for vw1 in vw1_cur do
       stream_name := concat(vw1.snow_table_name, '_str');
       my_sql := 'CREATE OR REPLACE STREAM ' || :stream_name || ' ON TABLE ' || vw1.snow_db_name||'.'||vw1.snow_schema_name||'.'|| vw1.snow_table_name || ';';
       execute immediate :my_sql;
       
       task_name := concat(vw1.snow_table_name, '_task');
       my_sql :=  '
            CREATE OR REPLACE TASK ' || :task_name || '
            WAREHOUSE = XSMALL_WH  
            SCHEDULE = '''|| vw1.task_schedule_minutes ||' MINUTE''
            WHEN SYSTEM$STREAM_HAS_DATA('''||:stream_name||''')
            AS
            BEGIN
                call update_glue_metadata_location('''||vw1.athena_db_name||''', 
                '''||vw1.athena_table_name||''',  
                get_ddl(''table'', '''||vw1.snow_db_name||'.'||vw1.snow_schema_name||'.'||vw1.snow_table_name||'''),
                CAST(GET(PARSE_JSON(SYSTEM$GET_ICEBERG_TABLE_INFORMATION('''||vw1.snow_table_name||''')), ''metadataLocation'') AS VARCHAR),
                '''||:stream_name||''');
            END;
        ';
        
       execute immediate :my_sql;
    end for;
end;
$$;
