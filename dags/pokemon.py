import os
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from google.cloud import storage
from airflow.providers.google.cloud.hooks.gcs import GCSHook
import requests
import pandas as pd
import io
from datetime import datetime


# Configure your GCS bucket name and BigQuery parameters
BUCKET_NAME = 'pokemon-dataset-zein'
UPLOAD_FILE_NAME = 'pokemon_species_data.csv'
CLEANED_FILE_NAME = 'cleaned_pokemon_species_data.csv'
BIGQUERY_TABLE_ID = 'tonal-carving-420807.pokemon.pokemon_species_data'

def fetch_pokemon_species(pokemon_id):
    url = f'https://pokeapi.co/api/v2/pokemon-species/{pokemon_id}/'
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        return {
            'id': data['id'],
            'name': data['name'],
            'base_happiness': data['base_happiness'],
            'capture_rate': data['capture_rate'],
            'forms_switchable': data['forms_switchable'],
            'gender_rate': data['gender_rate'],
            'habitat_name': data['habitat']['name'] if data['habitat'] else None,
            'has_gender_differences': data['has_gender_differences'],
            'hatch_counter': data['hatch_counter'],
            'is_baby': data['is_baby'],
            'is_legendary': data['is_legendary'],
            'is_mythical': data['is_mythical'],
            'shape_name': data['shape']['name'] if data['shape'] else None,
            'growth_rate': data['growth_rate']['name'] if data['growth_rate'] else None
        }
    else:
        return None

def fetch_and_store_pokemon_data(**kwargs):
    pokemon_data_list = []
    for pokemon_id in range(1, 1000):  # Adjust the range as needed for testing
        data = fetch_pokemon_species(pokemon_id)
        if data:
            pokemon_data_list.append(data)
    df = pd.DataFrame(pokemon_data_list)
    
    # Save to CSV
    csv_data = df.to_csv(index=False)
    
    # Upload to Google Cloud Storage using GCSHook
    gcs_hook = GCSHook(gcp_conn_id='GCSZein')
    gcs_hook.upload(
        bucket_name=BUCKET_NAME,
        object_name=UPLOAD_FILE_NAME,
        data=csv_data,
        mime_type='text/csv',
        timeout=7200
    )
    print(f'Uploaded {UPLOAD_FILE_NAME} to GCS.')

def retrieve_and_clean_data(**kwargs):
    gcs_hook = GCSHook(gcp_conn_id='GCSZein')
    data = gcs_hook.download(
        bucket_name=BUCKET_NAME,
        object_name=UPLOAD_FILE_NAME,
        timeout=7200
    )
    
    df = pd.read_csv(io.StringIO(data.decode('utf-8')))
    
    # Handle missing values
    for column in df.columns:
        if df[column].dtype == 'object':
            df[column].fillna(df[column].mode()[0], inplace=True)
        else:
            df[column].fillna(df[column].median(), inplace=True)
    
    # Save cleaned data to CSV
    cleaned_csv_data = df.to_csv(index=False)
    
    # Upload cleaned data to GCS
    gcs_hook.upload(
        bucket_name=BUCKET_NAME,
        object_name=CLEANED_FILE_NAME,
        data=cleaned_csv_data,
        mime_type='text/csv',
        timeout=7200
    )
    print(f'Uploaded {CLEANED_FILE_NAME} to GCS.')

# Define the default_args for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

# Define the DAG
with DAG(
    dag_id='pokemon_data_pipeline',
    default_args=default_args,
    schedule_interval='@daily',  # Schedule interval can be adjusted as needed
    catchup=False,
) as dag:
    
    fetch_and_store_task = PythonOperator(
        task_id='fetch_and_store_pokemon_data',
        python_callable=fetch_and_store_pokemon_data,
        provide_context=True
    )
    
    retrieve_and_clean_task = PythonOperator(
        task_id='retrieve_and_clean_data',
        python_callable=retrieve_and_clean_data,
        provide_context=True
    )

    clean_bigquery_table_task = BigQueryInsertJobOperator(
    task_id='clean_bigquery_table',
    configuration={
        "query": {
            "query": f"DELETE FROM `{BIGQUERY_TABLE_ID}` WHERE TRUE",
            "useLegacySql": False,
        }
    },
    gcp_conn_id='GCSZein',  # Use the connection ID
    )

    upload_to_bigquery_task = GCSToBigQueryOperator(
        task_id='upload_to_bigquery',
        bucket=BUCKET_NAME,
        source_objects=[CLEANED_FILE_NAME],
        destination_project_dataset_table=BIGQUERY_TABLE_ID,
        source_format='CSV',
        write_disposition='WRITE_TRUNCATE',
        skip_leading_rows=1,
        create_disposition='CREATE_IF_NEEDED',
        schema_fields=[
            {'name': 'id', 'type': 'INT64', 'mode': 'REQUIRED'},
            {'name': 'name', 'type': 'STRING', 'mode': 'REQUIRED'},
            {'name': 'base_happiness', 'type': 'FLOAT64', 'mode': 'NULLABLE'},
            {'name': 'capture_rate', 'type': 'INT64', 'mode': 'REQUIRED'},
            {'name': 'forms_switchable', 'type': 'BOOL', 'mode': 'REQUIRED'},
            {'name': 'gender_rate', 'type': 'INT64', 'mode': 'REQUIRED'},
            {'name': 'habitat_name', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'has_gender_differences', 'type': 'BOOL', 'mode': 'REQUIRED'},
            {'name': 'hatch_counter', 'type': 'FLOAT64', 'mode': 'NULLABLE'},
            {'name': 'is_baby', 'type': 'BOOL', 'mode': 'REQUIRED'},
            {'name': 'is_legendary', 'type': 'BOOL', 'mode': 'REQUIRED'},
            {'name': 'is_mythical', 'type': 'BOOL', 'mode': 'REQUIRED'},
            {'name': 'shape_name', 'type': 'STRING', 'mode': 'REQUIRED'},
            {'name': 'growth_rate', 'type': 'STRING', 'mode': 'REQUIRED'},
        ],
        gcp_conn_id='GCSZein',  # Use the connection ID
    )

    fetch_and_store_task >> retrieve_and_clean_task >> clean_bigquery_table_task >> upload_to_bigquery_task