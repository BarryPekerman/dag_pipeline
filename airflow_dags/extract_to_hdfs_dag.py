from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests, zipfile, os
from io import BytesIO

default_args = {
    "owner": "airflow",
    "start_date": datetime.now() - timedelta(days=1),
    "retries": 1,
}

def download_and_extract():
    url = "https://analyse.kmi.open.ac.uk/open-dataset/download"
    target_dir = "/opt/airflow/data/csvs"
    os.makedirs(target_dir, exist_ok=True)

    response = requests.get(url)
    if response.status_code != 200:
        raise Exception(f"Failed to download file: {response.status_code}")

    zip_file = zipfile.ZipFile(BytesIO(response.content))
    zip_file.extractall(target_dir)

with DAG(
    dag_id="extract_to_hdfs_dag",
    default_args=default_args,
    schedule=None,
    catchup=False,
    description="Downloads and extracts ZIP using Python, then uploads to HDFS",
) as dag:

    download_and_extract_task = PythonOperator(
        task_id="download_and_extract",
        python_callable=download_and_extract
    )

    upload_to_hdfs = PythonOperator(
        task_id="upload_to_hdfs",
        python_callable=lambda: os.system(
            "hdfs dfs -mkdir -p /data/student_csvs && hdfs dfs -put -f /opt/airflow/data/csvs/*.csv /data/student_csvs/"
        )
    )

    download_and_extract_task >> upload_to_hdfs

