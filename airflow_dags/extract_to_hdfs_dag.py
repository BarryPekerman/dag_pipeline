from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from hdfs import InsecureClient
import requests, zipfile, pathlib
from io import BytesIO

default_args = {
    "owner": "airflow",
    "start_date": datetime.now() - timedelta(days=1),
    "retries": 1,
}

# Step 1: Download and extract the dataset
def download_and_extract_zip():
    url = "https://analyse.kmi.open.ac.uk/open-dataset/download"
    target_dir = "/opt/airflow/data/csvs"

    print(f"Downloading ZIP from {url}")
    response = requests.get(url, allow_redirects=True)
    if response.status_code != 200:
        raise Exception(f"Failed to download file: {response.status_code}")

    pathlib.Path(target_dir).mkdir(parents=True, exist_ok=True)

    with zipfile.ZipFile(BytesIO(response.content)) as zip_file:
        zip_file.extractall(target_dir)
        print(f"Extracted files to: {target_dir}")

# Step 2: Upload to HDFS
def upload_to_hdfs_task():
    print(">>> Starting upload_to_hdfs_task()")

    local_dir = '/opt/airflow/data/csvs'
    files = list(pathlib.Path(local_dir).glob("*.csv"))
    print(f">>> Found {len(files)} files: {[str(f) for f in files]}")

    hdfs_url = 'http://hadoop-hadoop-hdfs-nn.hadoop.svc.cluster.local:9870'
    hdfs_target_dir = '/data/student_csvs'

    print(f">>> Connecting to HDFS at {hdfs_url}")
    client = InsecureClient(hdfs_url, user='hadoop')
    print(">>> Connected to HDFS")

    # Avoid using unsupported args — exist_ok is not valid
    if not client.status(hdfs_target_dir, strict=False):
        client.makedirs(hdfs_target_dir)
        print(f">>> Created HDFS directory: {hdfs_target_dir}")
    else:
        print(f">>> HDFS directory already exists: {hdfs_target_dir}")

    for file_path in files:
        hdfs_path = f'{hdfs_target_dir}/{file_path.name}'
        print(f">>> Uploading {file_path} to {hdfs_path}")
        client.upload(hdfs_path, str(file_path), overwrite=True)

    print(">>> Upload complete")

# DAG definition
with DAG(
    dag_id="extract_to_hdfs_dag",
    default_args=default_args,
    schedule_interval=None,
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

