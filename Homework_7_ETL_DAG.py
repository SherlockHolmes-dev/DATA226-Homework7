from airflow import DAG
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime

# Define Snowflake connection details
SNOWFLAKE_CONN_ID = 'snowflake_conn'

# SQL queries to create tables
create_user_session_channel_table = """
CREATE TABLE IF NOT EXISTS dev.raw_data.user_session_channel (
    userId int not NULL,
    sessionId varchar(32) primary key,
    channel varchar(32) default 'direct'
);
"""

create_session_timestamp_table = """
CREATE TABLE IF NOT EXISTS dev.raw_data.session_timestamp (
    sessionId varchar(32) primary key,
    ts timestamp
);
"""

# SQL queries to populate tables
create_stage_and_copy_user_session_channel = """
CREATE OR REPLACE STAGE dev.raw_data.blob_stage
url = 's3://s3-geospatial/readonly/'
file_format = (type = csv, skip_header = 1, field_optionally_enclosed_by = '"');

COPY INTO dev.raw_data.user_session_channel
FROM @dev.raw_data.blob_stage/user_session_channel.csv;
"""

create_stage_and_copy_session_timestamp = """
COPY INTO dev.raw_data.session_timestamp
FROM @dev.raw_data.blob_stage/session_timestamp.csv;
"""

# Function to execute Snowflake queries
def execute_snowflake_query(query):
    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    hook.run(query)

# Define the DAG
with DAG(
    'snowflake_etl_dag',
    default_args={'start_date': datetime(2023, 10, 23)},
    schedule_interval=None,
    catchup=False,
    description='ETL DAG to create tables and load data from S3 to Snowflake'
) as dag:

    # Task 1: Create user_session_channel table
    @task
    def create_user_session_channel_table_task():
        execute_snowflake_query(create_user_session_channel_table)

    # Task 2: Create session_timestamp table
    @task
    def create_session_timestamp_table_task():
        execute_snowflake_query(create_session_timestamp_table)

    # Task 3: Populate user_session_channel table
    @task
    def populate_user_session_channel_task():
        execute_snowflake_query(create_stage_and_copy_user_session_channel)

    # Task 4: Populate session_timestamp table
    @task
    def populate_session_timestamp_task():
        execute_snowflake_query(create_stage_and_copy_session_timestamp)

    # Task Dependencies
    create_user_session_channel_table_task() >> populate_user_session_channel_task()
    create_session_timestamp_table_task() >> populate_session_timestamp_task()

