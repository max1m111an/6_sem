import datetime
import json
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

# Константы
DATE = str(datetime.datetime.now().date())
CONT_TYPE = "20"
WEIGHT = "10"
CURRENCY = "RUB"

URL = f"http://transfer-enigma.ru/api/v2/routes/calculate"

LOCAL_FILE_PATH = "/opt/airflow/data/transfer_date.json"
PROCESSED_CSV_PATH = "/opt/airflow/data/processed_transfer_data.csv"
PARQUET_FILE_PATH = "/opt/airflow/data/transfer.parquet"

def get_data():
    json_data = {
        "dispatchDate":DATE,
        "departureInternalIds":[90],
        "destinationInternalIds":[22,446,449,450,448,447,445,451,444,452,453],
        "departureExternalIds":["d1b5fcf8-4ebf-11ef-b849-005056956645"],
        "destinationExternalIds":["597a35bc-ed69-4a11-942b-480481867c76"],
        "containerType":CONT_TYPE,
        "cargoWeight":WEIGHT,
        "currency":CURRENCY
    }
    response = requests.post(URL, json=json_data)
    if response.status_code == 200:
        transfer_data = response.json()
        with open(LOCAL_FILE_PATH, "w") as file:
            json.dump(transfer_data, file)
    else:
        raise Exception(
            f"Error fetching data from Transfer Enigma API: {response.status_code}"
        )

with DAG(
    "transfer_dag",
    default_args={
        "depends_on_past": False,
        "email": ["airflow@example.com"],
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
        "retry_delay": datetime.timedelta(minutes=5),
    },
    # [END default_args]
    description="Transfer Dag",
    schedule=None,
    start_date=days_ago(1),
) as dag:
    python = PythonOperator(
        task_id='python',
        python_callable=get_data
    )

[python]