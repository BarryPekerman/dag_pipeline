from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from hdfs import InsecureClient
import requests, zipfile, os
from io import BytesIO
import traceback
import os

default_args = {
    "owner": "airflow",
    "start_date": datetime.now() - timedelta(days=1),
    "retries": 1,
}

# Step 1: Download and extract the dataset
def download_and_extract_zip():
    url = "https://analyse.kmi.open.ac.uk/open-dataset/download"
    target_dir = "/opt/airflow/data/csvs"
    if not os.path.exists(target_dir):
        client.makedirs(target_dir)

    print(f"Downloading ZIP from {url}")
    response = requests.get(url, allow_redirects=True)
    if response.status_code != 200:
        raise Exception(f"Failed to download file: {response.status_code}")

    with zipfile.ZipFile(BytesIO(response.content)) as zip_file:
        zip_file.extractall(target_dir)
        print(f"Extracted files to: {target_dir}")

# Step 2: Upload to HDFS
def upload_to_hdfs_py():

    print(">>> Starting upload_to_hdfs_py()")

    try:
        local_dir = '/opt/airflow/data/csvs'
        print(f">>> Checking local_dir: {local_dir}")

        if not os.path.exists(local_dir):
            raise Exception(f"Local directory does not exist: {local_dir}")

        files = os.listdir(local_dir)
        print(f">>> Found {len(files)} files: {files}")

        hdfs_url = 'http://hadoop-hadoop-hdfs-nn.hadoop.svc.cluster.local:9870'
        hdfs_target_dir = '/data/student_csvs'

        print(f">>> Connecting to HDFS at {hdfs_url}")
        client = InsecureClient(hdfs_url, user='hadoop')
        print(">>> Connected to HDFS")

        client.makedirs(hdfs_target_dir, exist_ok=True)
        print(f">>> HDFS directory ready: {hdfs_target_dir}")

        for filename in files:
            if filename.endswith('.csv'):
                local_path = os.path.join(local_dir, filename)
                hdfs_path = f'{hdfs_target_dir}/{filename}'
                print(f">>> Uploading {local_path} to {hdfs_path}")
                client.upload(hdfs_path, local_path, overwrite=True)

        print(">>> Upload complete")

    except Exception:
        print(">>> Exception occurred in upload_to_hdfs_py:")
        print(traceback.format_exc())
        raise

# DAG definition
with DAG(
    dag_id="extract_to_hdfs_dag",
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
        python_callable=upload_to_hdfs_py
    )

    download_and_extract >> upload_to_hdfs

