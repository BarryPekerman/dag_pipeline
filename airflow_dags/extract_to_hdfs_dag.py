from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
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

    print(f"Downloading ZIP from {url}", flush=True)
    response = requests.get(url, allow_redirects=True)
    if response.status_code != 200:
        raise Exception(f"Failed to download file: {response.status_code}")

    pathlib.Path(target_dir).mkdir(parents=True, exist_ok=True)

    with zipfile.ZipFile(BytesIO(response.content)) as zip_file:
        zip_file.extractall(target_dir)
        print(f"Extracted files to: {target_dir}", flush=True)

# Step 2: Upload to HDFS using buffered write
def upload_to_hdfs_task():
    print(">>> Starting upload_to_hdfs_task()", flush=True)

    local_dir = '/opt/airflow/data/csvs'
    files = list(pathlib.Path(local_dir).glob("*.csv"))
    print(f">>> Found {len(files)} files: {[str(f) for f in files]}", flush=True)

    hdfs_url = 'http://hadoop-hadoop-hdfs-nn.hadoop.svc.cluster.local:9870'
    hdfs_target_dir = '/data/student_csvs'

    print(f">>> Connecting to HDFS at {hdfs_url}", flush=True)
    client = InsecureClient(hdfs_url, user='root')
   # client = InsecureClient(hdfs_url, user='hadoop', timeout=10)
    print(">>> Connected to HDFS", flush=True)

    if not client.status(hdfs_target_dir, strict=False):
        client.makedirs(hdfs_target_dir)
        print(f">>> Created HDFS directory: {hdfs_target_dir}", flush=True)
    else:
        print(f">>> HDFS directory already exists: {hdfs_target_dir}", flush=True)

    for file_path in files:
        hdfs_path = f'{hdfs_target_dir}/{file_path.name}'
        print(f">>> Uploading {file_path} to {hdfs_path}", flush=True)
        try:
            with open(file_path, 'r', encoding='utf-8') as local_file:
                with client.write(hdfs_path, overwrite=True, encoding='utf-8') as writer:
                    for i, line in enumerate(local_file):
                        writer.write(line)
                        if i % 500 == 0:
                            print(f"  ...wrote {i} lines to {hdfs_path}", flush=True)
            print(f">>> Finished uploading {file_path}", flush=True)
        except Exception as e:
            print(f"!!! Failed to upload {file_path}: {e}", flush=True)
            raise

    print(">>> Upload complete", flush=True)

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

