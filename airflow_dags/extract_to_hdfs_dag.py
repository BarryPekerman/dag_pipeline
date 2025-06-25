from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta
from hdfs import InsecureClient
import requests, zipfile, pathlib
from io import BytesIO
import pandas as pd

default_args = {
    "owner": "airflow",
    "start_date": datetime.now() - timedelta(days=1),
    "retries": 1,
}

HDFS_URL = "http://hadoop-hadoop-hdfs-nn.hadoop.svc.cluster.local:9870"
HDFS_USER = "airflow"
EXTRACT_DIR= "/opt/airflow/data/csvs"

# Step 1: Download and extract the dataset
def download_and_extract_zip():
    url = "https://analyse.kmi.open.ac.uk/open-dataset/download"
    target_dir = "/opt/airflow/data/csvs"

    print(f"Downloading ZIP from {url}", flush=True)
    response = requests.get(url, allow_redirects=True)
    if response.status_code != 200:
        raise Exception(f"Failed to download file: {response.status_code}")

    pathlib.Path(target_dir).mkdir(parents=True, exist_ok=True)

    with zipfile.ZipFile(BytesIO(response.content)) as zip_file:
        zip_file.extractall(target_dir)
        print(f"Extracted files to: {target_dir}", flush=True)

# Step 2: Upload to HDFS using pandas and verbose logging

def upload_to_hdfs_task():
    client = InsecureClient(HDFS_URL, user=HDFS_USER)

    for root, _, files in os.walk(EXTRACT_DIR):
        for file in files:
            local_path = os.path.join(root, file)
            rel_path = os.path.relpath(local_path, EXTRACT_DIR)
            hdfs_path = f"/datasets/{rel_path}"

            try:
                with open(local_path, "rb") as reader:
                    client.write(hdfs_path, reader, overwrite=True)
                print(f"Uploaded {file} successfully.")
            except Exception as e:
                print(f"Upload failed for {file}: {e}")


# DAG definition
with DAG(
    "my_pipeline",
    default_args=default_args,
    schedule=None,
    catchup=False,
    description="Download ZIP, extract CSVs, upload to HDFS (Python only)",
) as dag:

    download_and_extract = PythonOperator(
        task_id="download_and_extract",
        python_callable=download_and_extract_zip
    )

    upload_to_hdfs = PythonOperator(
        task_id="upload_to_hdfs",
        python_callable=upload_to_hdfs_task
    )

    download_and_extract >> upload_to_hdfs

