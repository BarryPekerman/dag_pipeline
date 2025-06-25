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
    print(">>> Starting upload_to_hdfs_task()", flush=True)

    local_dir = '/opt/airflow/data/csvs'
    files = list(pathlib.Path(local_dir).glob("*.csv"))
    print(f">>> Found {len(files)} files: {[str(f) for f in files]}", flush=True)

    hdfs_url = 'http://hadoop-hadoop-hdfs-nn.hadoop.svc.cluster.local:9870'
    hdfs_target_dir = '/data/student_csvs'

    print(f">>> Connecting to HDFS at {hdfs_url}", flush=True)
    client = InsecureClient(hdfs_url, user='airflow')
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
            df = pd.read_csv(file_path)
            print(f"  - Loaded DataFrame with shape: {df.shape}", flush=True)
            print(f"  - First 3 rows:\n{df.head(3)}", flush=True)

            csv_data = df.to_csv(index=False, lineterminator="\n")
            with client.write(hdfs_path, overwrite=True, encoding='utf-8') as writer:
                for i, line in enumerate(csv_data.splitlines()):
                    writer.write(line + "\n")
                    if i % 500 == 0:
                        print(f"    ...wrote {i} lines to {hdfs_path}", flush=True)

            print(f">>> Finished uploading {file_path} ({i+1} lines)", flush=True)

        except UnicodeDecodeError as ude:
            print(f"!!! Unicode decode error in {file_path}: {ude}", flush=True)
            raise
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

