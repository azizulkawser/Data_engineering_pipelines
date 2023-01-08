#import libraries
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.bigquery_check_operator import BigQueryCheckOperator
from google.cloud import bigquery
from google.oauth2 import service_account
from airflow import DAG
from datetime import timedelta, datetime

# Define the default_args dictionary
default_args = {
  'owner': 'Azizul Kawser',
  'start_date': datetime(2022, 12, 9),
  'email': ['azizulkawser.aust@gmail.com'],
  'email_on_failure': False,
  'email_on_retry': False,
  'retries': 2,
  'retry_delay': timedelta(minutes=1),
}

# Instantiate the DAG object
dag = DAG(
    'ecommerce_bigquery_data_pipeline_',  # DAG name that appears in Airflow
    default_args=default_args,
    description='process daily tables of raw data',
    schedule_interval=timedelta(days=1),
    catchup=True
)

#This function processing raw data fusing SQL and load that process data into bigQuery database
def run_etl(ds=None):
    credentials = service_account.Credentials.from_service_account_file(
        '/opt/airflow/dags/your_jason_file_name.json')
    # Construct a BigQuery client object.
    client = bigquery.Client(credentials=credentials)
    #Set table_id to the ID of the destination table.
    #format is table_id = "project_name.dataset_name.table_name"
    table_id = "my-data-engineering-project.ecommerce_data.process_data_bigquery"

    job_config = bigquery.QueryJobConfig(destination=table_id)
    job_config.write_disposition = "WRITE_TRUNCATE"
    
    #Extract the data
    sql_query = """
    SELECT
        user_pseudo_id as user_id,
        count(*) as no_of_events,
        TIMESTAMP_MICROS(user_first_touch_timestamp) as start_time,
        device.category as device,
        device.operating_system as OS,
        device.web_info.browser as browser,
        geo.country as country,
        geo.region as region,
        geo.city as city,
        user_ltv.revenue as revenue
    FROM
        `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_20210131`
    GROUP BY 1,3,4,5,6,7,8,9,10
    """
    # Start the query, passing in the extra configuration.
    query_job = client.query(sql_query, job_config=job_config)  # Make an API request.
    query_job.result()  # Wait for the job to complete.
    
    

    
# BigQueryCheckOperator check that the SQL inside returns a single row
# in this case it basically checks that the data exists
# Define the BigQueryCheckOperator task
check_raw_data = BigQueryCheckOperator(
    task_id="check_raw_data_bigquery",  # name appears in Airflow
    sql="""
    SELECT COUNT(*) FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_20210131` 
    """,
    use_legacy_sql=False,
    dag=dag,
    gcp_conn_id='gcp_bq_con'  # Authentication in Airflow
)

# Define the PythonOperator task
process_and_load_data = PythonOperator(
    task_id="Process_and_load_data_bigquery",  # name appears in Airflow
    python_callable=run_etl,
    dag=dag
)

# Set 'check_raw_data' to run prior to 'process_and_load_data' and 'send_email' task at the last
check_raw_data >> process_and_load_data
