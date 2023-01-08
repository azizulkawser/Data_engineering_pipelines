# import libraries
from airflow.operators.python_operator import PythonOperator
from google.cloud import bigquery
import pandas as pd
from google.oauth2 import service_account
from airflow import DAG
from datetime import timedelta, datetime

# Define the default_args dictionary
default_args = {
  'owner': 'Azizul Kawser',
  'start_date': datetime(2023, 1, 7),
  'email': ['azizulkawser.aust@gmail.com'],
  'email_on_failure': False,
  'email_on_retry': False,
  'retries': 1,
  'retry_delay': timedelta(minutes=1),
}

# Instantiate the DAG object
dag = DAG(
    'sunspot_data_pipeline',  # Name that appears in Airflow
    default_args=default_args,
    description='clean sunspot data, calculate annual averages and save results to BigQuery',
    schedule_interval=timedelta(days=1),
    catchup=True
)

#This function extracts the data from datalake, transform that and load into bigQuery database
def run_etl(ds=None):
    credentials = service_account.Credentials.from_service_account_file(
        '/opt/airflow/dags/your_jason_file_name.json')
    # Construct a BigQuery client object.
    client = bigquery.Client(credentials=credentials)
    #Set table_id to the ID of the destination table.
    #format is table_id = "project_name.dataset_name.table_name"
    table_id = "my-data-engineering-project.sunspot_data.yearly_mean_sunspot"

    job_config = bigquery.LoadJobConfig()
    job_config.write_disposition = "WRITE_TRUNCATE"

    
    #Extract the data
    #read csv file
    df = pd.read_csv('/opt/airflow/dags/SN_d_tot_V2.0.csv',  sep=';')
    
    #Transform the Data
    # replace the negative sunspot_number with zero
    df.loc[(df['sunspot_number'] < 0 ), 'sunspot_number'] = 0
    # construct date from year, month, day column value
    df['Date'] = pd.to_datetime(df[['year', 'month', 'day']])
    #Finally delete year, month, day, decimal_year column form the dataframe
    df.drop(['year', 'month', 'day', 'decimal_year'], axis=1, inplace=True)
    # shift column 'Date' column to first position
    first_column = df.pop('Date')
    # insert column using insert(position,column_name,first_column) function
    df.insert(0, 'Date', first_column)
    # 'Date' should be date type. lets convert the 'Date' column to datetime format
    df['Date']= pd.to_datetime(df['Date'],
    #attempt to infer date format of each date
    infer_datetime_format = True,
    #retun NA for rows where conversion failed
    errors = 'coerce')
    #calculate annual averages and group by year
    df_year_wise = df.groupby(pd.Grouper(key='Date', freq='y')).agg({'sunspot_number': 'mean'}).reset_index()

    # Start the query, passing in the extra configuration.
    query_job = client.load_table_from_dataframe(df_year_wise, table_id,job_config=job_config)  # Make an API request.
    query_job.result()  # Wait for the job to complete.

# Define the PythonOperator task
process_and_load_data = PythonOperator(
    task_id="load_data_to_bigquery",  # name appears in Airflow
    python_callable=run_etl,
    dag=dag
)
