from airflow.decorators import dag, task
from datetime import datetime, timedelta
from google.cloud import bigquery

PROJECT_ID = "seu_projeto"
DATASET = "seu_dataset"
TABLE = "binance_klines"
BQ_TABLE_ID = f"{PROJECT_ID}.{DATASET}.{TABLE}"
BUCKET_NAME = "seu-bucket"
GCS_PARQUET_PATH = "binance_klines.parquet"

default_args = {
    'owner': 'vitor',
    'depends_on_past': False,
    'start_date': datetime(2025, 2, 25),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

@dag(
    dag_id="binance_carga_bigquery",
    default_args=default_args,
    description="DAG para carregar Parquet do GCS para BigQuery",
    schedule_interval=None,
    catchup=False,
    tags=["gcs", "bigquery", "parquet"]
)
def load_bigquery_dag():

    @task()
    def load_to_bigquery():
        uri = f"gs://{BUCKET_NAME}/{GCS_PARQUET_PATH}"

        bq_client = bigquery.Client()
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET,
            write_disposition="WRITE_APPEND"
        )

        job = bq_client.load_table_from_uri(uri, BQ_TABLE_ID, job_config=job_config)
        job.result()

        print(f"Carregado para {BQ_TABLE_ID} via Parquet")

    load_to_bigquery()

load_bigquery_dag()
