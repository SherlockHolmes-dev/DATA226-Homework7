from airflow import DAG
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime

# Define Snowflake connection details
SNOWFLAKE_CONN_ID = 'snowflake_conn'

# SQL query to join tables and create the session_summary table in the analytics schema
create_session_summary_table = """
CREATE TABLE IF NOT EXISTS dev.analytics.session_summary AS
SELECT 
    usc.userId, 
    usc.sessionId, 
    usc.channel, 
    st.ts AS session_timestamp
FROM 
    dev.raw_data.user_session_channel usc
JOIN 
    dev.raw_data.session_timestamp st
ON 
    usc.sessionId = st.sessionId;
"""

# SQL query to check for duplicate records in the session_summary table
check_for_duplicates = """
SELECT 
    sessionId, 
    COUNT(*) AS duplicate_count
FROM 
    dev.analytics.session_summary
GROUP BY 
    sessionId
HAVING 
    COUNT(*) > 1;
"""

# Function to execute Snowflake queries
def execute_snowflake_query(query):
    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    hook.run(query)

# Function to get results from a Snowflake query
def fetch_snowflake_query(query):
    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    return hook.get_records(query)

# Define the DAG
with DAG(
    'snowflake_elt_with_duplicates_check',
    default_args={'start_date': datetime(2023, 10, 23)},
    schedule_interval=None,
    catchup=False,
    description='ELT DAG to join tables, store result in analytics schema, and check for duplicates'
) as dag:

    # Task 1: Join tables and create session_summary
    @task
    def create_session_summary_table_task():
        execute_snowflake_query(create_session_summary_table)

    # Task 2: Check for duplicate records in session_summary
    @task
    def check_duplicates_task():
        duplicates = fetch_snowflake_query(check_for_duplicates)
        if duplicates:
            raise Exception(f"Duplicate records found: {duplicates}")
        else:
            print("No duplicate records found.")

    # Task Dependencies
    create_session_summary_table_task() >> check_duplicates_task()

