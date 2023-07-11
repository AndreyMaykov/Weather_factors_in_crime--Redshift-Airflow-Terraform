# Object: Airflow DAG
# Apache Airflow version: 2.5.0

# Author: Andrey Maykov
# Date: 2023-03-31

# Description:
# The DAG is used in the project to orchestrate all the ETL operations.
 

from airflow.decorators import dag, task
from airflow.decorators.task_group import task_group
from airflow.models.variable import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator
from airflow.providers.amazon.aws.operators.redshift_sql import RedshiftSQLOperator

import json
from datetime import datetime

@dag(
    schedule_interval=None,
    catchup=False,
    start_date=datetime.now()
)
def weather_factors_in_crime(): 
    @task_group(group_id="create_schemas_sprocs_upload_data")
    def create_schemas_sprocs_upload_data_group():
        # Upload the data files from the local filesystem
        # to the project's S3 bucket
        @task_group(group_id="upload_data_to_s3")
        def upload_data_to_s3_group():
            # Read local and S3 paths to the data 
            # specified in the data_files.json file.
            @task(task_id="get_data_file_names")
            def get_data_file_names():
                f = open('/opt/airflow/data/auxfs/data_files.json')       
                data_files = json.load(f)['data_files']
                dfs = [
                        {'data_file_local': aa['local'], 
                         'data_file_s3': aa['s3']}
                        for aa in data_files
                      ]               
                f.close()
                return dfs
            
            # Create (using dynamic file mapping) and carry out 
            # the upload tasks
            upload_data_files = LocalFilesystemToS3Operator.partial(
                task_id="upload_data_files",
                filename="/opt/airflow/data/{{params.data_file_local}}",
                dest_key="{{params.data_file_s3}}",
                dest_bucket=Variable.get("s3_bucket"),
                aws_conn_id="aws_s3_connection",
                replace=False,
            ).expand(params=get_data_file_names())
            
        upload_data_to_s3_group_task = upload_data_to_s3_group()
    
        # Create a schema for the project's target tables
        create_schema_wxcr = RedshiftSQLOperator(
            task_id="create_schema_wxcr",
            sql="CREATE SCHEMA wxcr;",
        )
        
        # Create a schema for staging and transforming 
        # the project data
        create_schema_wxcr_be = RedshiftSQLOperator(
            task_id="create_schema_wxcr_be",
            sql="CREATE SCHEMA wxcr_be;",
        )
        
        # Create a table for logging the cldeansing operations
        create_log_table = RedshiftSQLOperator(
            task_id="create_log_table",
            sql="sql/create_log_tbl.sql",
        )
        
        # Create stored procedures from SQL files 
        # stored in the local /opt/airflow/sql/sprocs directory
        @task_group(group_id="create_sprocs")
        def create_sprocs_group():
            sproc_names = [
                    'load_clean_agencies__sproc', 
                    'load_clean_weather_stations__sproc', 
                    'load_meteoparam_station_specs__sproc',
                    'create_meteoparam_tbl_defs__sproc',
                    'unpivot_meteoparam_tbl__sproc',
                    'join_meteoparam_unpv_tbls__sproc',
                    'load_clean_crime_incidents__sproc',
                    'split_offenses_ci__sproc',
                    'create_date_range__sproc',
                    'create_dim_dates__sproc',
                    'create_dim_hours__sproc',
                    'add_keys_fct_crime_incidents__sproc',
                    'add_keys_fct_meteodata__sproc'
            ]

            for name in sproc_names:
                create_sproc_task = RedshiftSQLOperator(
                    task_id=name,
                    sql=f'sql/sprocs/{name}.sql',
                )
         
        create_sprocs_group_task = create_sprocs_group()
        
        create_schema_wxcr_be >> create_log_table
        
        [create_schema_wxcr, create_log_table] >> create_sprocs_group_task
        
    create_schemas_sprocs_upload_data_group_task = create_schemas_sprocs_upload_data_group()
       
    # 1. Fetch data from the agencies.csv file located in the project's S3 bucket
    # 2. Handle data errors (for detail, see load_clean_agencies__sproc.sql)
    # 3. Load the cleaned data into the wxcr.dim_agencies table
    load_clean_agencies = RedshiftSQLOperator(
        task_id="load_clean_agencies",
        sql="sql/sprocs/load_clean_agencies__call.sql",
        params=dict(
            s3_bucket=Variable.get("s3_bucket"),
            iam_role=Variable.get("iam_role_arn")
        ),
    )
    
    # 1. Fetch data from the weather_stations.csv file located in the project's S3 bucket
    # 2. Handle data errors (for detail, see load_clean_weather_stations__sproc.sql)
    # 3. Load the cleaned data into the wxcr.dim_weather_stations table
    load_clean_weather_stations = RedshiftSQLOperator(
        task_id="load_clean_weather_stations",
        sql="sql/sprocs/load_clean_weather_stations__call.sql",
        params=dict(
            s3_bucket=Variable.get("s3_bucket"),
            iam_role=Variable.get("iam_role_arn")
        ),
    )
    
    # Retrieve the information from meteoparam_names_fmts.csv and station_names.csv 
    load_meteoparam_station_specs = RedshiftSQLOperator(
        task_id="load_meteoparam_station_specs",
        sql="sql/sprocs/load_meteoparam_station_specs__call.sql",
        params=dict(
            s3_bucket=Variable.get("s3_bucket"),
            iam_role=Variable.get("iam_role_arn")
        ),
    )
    
    # Create strings required to create tables for staging meteoparameter data 
    # (e.g. datetime DATETIME, Philadelphia DECIMAL(5, 2), New_York DECIMAL(5, 2), ... 
    # for temperature data).
    create_meteoparam_tbl_defs = RedshiftSQLOperator(
        task_id="create_meteoparam_tbl_defs",
        sql="CALL wxcr_be.create_meteoparam_tbl_defs()",
    )
    
    @task_group(group_id="load_unpivot_meteoparam_tables")
    def load_unpivot_meteoparam_tables_group():
        @task(task_id="get_meteoparam_tbl_defs")
        def get_meteoparam_tbl_defs(): 
            hook = PostgresHook(postgres_conn_id="postgres_default")
            connection = hook.get_conn()
            cursor = connection.cursor()
            cursor.execute("SELECT tbl_name, col_defs FROM wxcr_be.meteoparam_tbl_defs") 
            sources = cursor.fetchall()
            return [{'tbl_name': item[0], 'col_defs': item[1]} for item in sources]
        
        gtwd = get_meteoparam_tbl_defs()
          
        # Create staging tables for temperture, humidity, etc. using the strings 
        # generated by the create_meteoparam_tbl_defs task and retrieved 
        # by the get_meteoparam_tbl_defs() function
        create_meteoparam_tbl = RedshiftSQLOperator.partial(
            task_id="create_meteoparam_tbl",
            sql="CREATE TABLE wxcr_be.{{params.tbl_name}} ({{params.col_defs}})",
        ).expand(params=gtwd)
        
        # Populate the staging tables created by the previous task
        copy_meteoparam_tbl = RedshiftSQLOperator.partial(
            task_id="copy_meteoparam_tbl",
            sql="sql/copy_meteoparam_tbl.sql",
            params=dict(
                s3_bucket=Variable.get("s3_bucket"),
                iam_role=Variable.get("iam_role_arn"),
            )
        ).expand(params=gtwd)

        # Transforms the meteoparameter tables so that the data 
        # from all the station-specific columns is placed into a single column 
        # and the station names corresponding to the data origing is indicated 
        # in another column).
        unpivot_meteoparam_tbl = RedshiftSQLOperator.partial(
            task_id="unpivot_meteoparam_tbl",
            sql="sql/sprocs/unpivot_meteoparam_tbl__call.sql",
        ).expand(params=gtwd)
        
        create_meteoparam_tbl >> copy_meteoparam_tbl >> unpivot_meteoparam_tbl
        
    load_unpivot_meteoparam_tables_group_task = load_unpivot_meteoparam_tables_group()
    
    # Join the six unpivoted meteoparameter tables on the stn (the station's name) 
    # and datetime (the date and time of the measurment) columns 
    # so that the temperature, humidity, etc. datasets are collected in a single table.
    join_meteoparam_unpv_tables = RedshiftSQLOperator(
        task_id="join_meteoparam_unpv_tables",
        sql="CALL wxcr_be.join_meteoparam_unpv_tbls()",
    )
    
    # 1. Fetch data from the crime_incidents.csv file located in the project's S3 bucket
    # 2. Handle data errors (for detail, see load_clean_crime_incidents__sproc.sql)
    load_clean_crime_incidents = RedshiftSQLOperator(
        task_id="load_clean_crime_incidents",
        sql="sql/sprocs/load_clean_crime_incidents__call.sql",
        params=dict(
            s3_bucket=Variable.get("s3_bucket"),
            iam_role=Variable.get("iam_role_arn")
        ),
    )
    
    # Split all the "compound" offense names originating from crime_incidents.csv
    # into "standard" offense names and organize them in a normalized form
    # (for detail, see the project's Readme.md and split_offenses_ci__sproc.sql)
    split_ci_offenses = RedshiftSQLOperator(
        task_id="split_ci_offenses",
        sql="CALL wxcr_be.split_offenses_ci();",
    )
    
    # Create a table wxcr_be.ci_offense_names_denormalized where 
    #      - one column contains crime incident ids; 
    #      - each of the other columns corresponds to a "standard" offense name, 
    #        while its values indicate whether the name is relevant to the given incident id
    @task_group(group_id="pivot_ci_table")
    def pivot_ci_table_group():
        @task(task_id="get_offense_names")
        def get_offense_names():
            hook = PostgresHook(postgres_conn_id="postgres_default")
            connection = hook.get_conn()
            cursor = connection.cursor()
            cursor.execute("SELECT DISTINCT offense_name FROM wxcr_be.ci_offense_names_normalized") 
            sources = cursor.fetchall()
            offense_names = [
                f"'{source[0]}'"
                for source in sources
            ]
            print("offense_names = ", offense_names)
            return ", ".join(offense_names)

        get_offense_names_task = get_offense_names()

        pivot_ci_tbl = RedshiftSQLOperator(
            task_id="pivot_ci_tbl",
            sql="sql/pivot_ci_tbl.sql",
        )

        get_offense_names_task >> pivot_ci_tbl
        
    pivot_ci_table_group_task = pivot_ci_table_group()
     
    @task_group(group_id="create_date_time_tables")
    def create_date_time_tables_group(): 
        # Create a table containing a date sequence.
        # The beginning and end date can be determined 
        # either automatically (by the meteoparmeter 
        # and crime incident datasets) or manually (by the user). 
        # In the first case, use 
        # sql="CALL wxcr_be.create_dim_dates(NULL, NULL)";
        # in the second,
        # sql="CALL wxcr_be.create_dim_dates(<beginning_date>, <end_date>)",
        # e.g. sql="CALL wxcr_be.create_dim_dates(\'1990-10-10\', \'2020-10-10\')"
        # Also see the project's Readme.md
        create_dim_dates = RedshiftSQLOperator(
            task_id="create_dim_dates",
            sql="CALL wxcr_be.create_dim_dates(NULL, NULL)",
        )
        
        # Create the table containing the hours 
        # 00:00, 01:00, ... , 23:00
        create_dim_hours = RedshiftSQLOperator(
            task_id="create_dim_hours",
            sql="CALL wxcr_be.create_dim_hours()",
        )
        
    create_date_time_tables_group_task = create_date_time_tables_group()
    
    
    
    # Creates the tables fct_meteodata and fct_crime_incidents 
    # with the required distribution and sort keys.
    @task_group(group_id="create_fct_tables")
    def create_fct_tables_group():
        create_fct_meteodata = RedshiftSQLOperator(
            task_id="create_fct_meteodata",
            sql="sql/create_fct_meteodata.sql",
        )
        
        create_fct_crime_incidents = RedshiftSQLOperator(
            task_id="create_fct_crime_incidents",
            sql="sql/create_fct_crime_incidents.sql",
        )
        
        add_keys_fct_meteodata = RedshiftSQLOperator(
            task_id="add_keys_fct_meteodata",
            sql="CALL wxcr_be.add_keys_fct_meteodata()",
        )
        
        add_keys_fct_crime_incidents = RedshiftSQLOperator(
            task_id="add_keys_fct_crime_incidents",
            sql="CALL wxcr_be.add_keys_fct_crime_incidents()",
        )
        
        create_fct_meteodata >> add_keys_fct_meteodata
        
        create_fct_crime_incidents >> add_keys_fct_crime_incidents
        
    create_fct_tables_group_task = create_fct_tables_group()
    

    create_schemas_sprocs_upload_data_group_task \
    >> [
            load_clean_agencies, 
            load_clean_weather_stations, 
            load_clean_crime_incidents, 
            load_meteoparam_station_specs
    ] 

    
    load_clean_crime_incidents >> split_ci_offenses >> pivot_ci_table_group_task
    
    load_meteoparam_station_specs >> create_meteoparam_tbl_defs \
    >> load_unpivot_meteoparam_tables_group_task >> join_meteoparam_unpv_tables
    
    [load_clean_crime_incidents, join_meteoparam_unpv_tables] >> create_date_time_tables_group_task
    
    [pivot_ci_table_group_task, create_date_time_tables_group_task] >> create_fct_tables_group_task 

    
weather_factors_in_crime_dag = weather_factors_in_crime()

