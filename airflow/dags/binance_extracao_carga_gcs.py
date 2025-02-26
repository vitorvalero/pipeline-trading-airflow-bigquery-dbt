from airflow.decorators import dag, task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta, timezone
import requests
import pandas as pd
from google.cloud import storage

SYMBOL = "BTCBRL"
INTERVAL = "1m"
LIMIT = 720  
START_TIME = 1602583200000  
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
    dag_id="binance_extracao_carga_gcs",
    default_args=default_args,
    description="DAG para extrair Klines da Binance e salvar Parquet no GCS",
    schedule_interval="0 1 * * *",
    catchup=False,
    tags=["binance", "gcs", "parquet"]
)
def extract_binance_dag():

    @task()
    def fetch_klines():
        now = datetime.now(timezone.utc)  
        yesterday_utc = datetime(now.year, now.month, now.day, tzinfo=timezone.utc) - timedelta(days=1)
        end_time = int(yesterday_utc.timestamp() * 1000)

        all_data = []
        start_time = START_TIME

        while start_time < end_time:
            url = f"https://api.binance.com/api/v3/klines?symbol={SYMBOL}&interval={INTERVAL}&limit={LIMIT}&startTime={start_time}"
            response = requests.get(url)

            if response.status_code == 200:
                data = response.json()
                if not data:
                    break  

                df = pd.DataFrame(data, columns=[
                    "open_time", "open_price", "high_price", "low_price", "close_price", "volume", 
                    "close_time", "quote_asset_volume", "number_of_trades", 
                    "taker_buy_base_asset_volume", "taker_buy_quote_asset_volume", "_unused"
                ])
                
                df["open_time"] = pd.to_datetime(df["open_time"], unit="ms")
                df["close_time"] = pd.to_datetime(df["close_time"], unit="ms")
                df.drop(columns=["_unused"], inplace=True)

                all_data.append(df)
                start_time = int(df.iloc[-1]["close_time"].timestamp() * 1000) + 1
            else:
                print("Erro na API:", response.text)
                break

        return pd.concat(all_data, ignore_index=True) if all_data else pd.DataFrame()

    @task()
    def save_to_gcs(df: pd.DataFrame):
        if df.empty:
            print("Nenhum dado para salvar.")
            return None

        temp_parquet_path = "/tmp/binance_klines.parquet"
        df.to_parquet(temp_parquet_path, index=False)

        storage_client = storage.Client()
        bucket = storage_client.bucket(BUCKET_NAME)
        blob = bucket.blob(GCS_PARQUET_PATH)
        blob.upload_from_filename(temp_parquet_path)

        print(f"Parquet salvo em: gs://{BUCKET_NAME}/{GCS_PARQUET_PATH}")
        return f"gs://{BUCKET_NAME}/{GCS_PARQUET_PATH}"

    trigger_next_dag = TriggerDagRunOperator(
        task_id="trigger_binance_carga_bigquery",
        trigger_dag_id="binance_carga_bigquery",
        wait_for_completion=False,
    )

    df = fetch_klines()
    gcs_uri = save_to_gcs(df)
    gcs_uri >> trigger_next_dag  

extract_binance_dag()
