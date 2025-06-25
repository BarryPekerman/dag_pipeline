from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from hdfs import InsecureClient
import os

default_args = {
    "owner": "airflow",
    "start_date": datetime.now() - timedelta(days=1),
    "retries": 1,
}

# Python function to upload extracted CSVs to HDFS
def upload_to_hdfs_py():
    local_dir = '/opt/airflow/data/csvs'
    hdfs_url = 'http://hadoop-hadoop-hdfs-nn.hadoop.svc.cluster.local:9870'
    hdfs_target_dir = '/data/student_csvs'

    client = InsecureClient(hdfs_url, user='hadoop')

    client.makedirs(hdfs_target_dir, exist_ok=True)

    for filename in os.listdir(local_dir):
        if filename.endswith('.csv'):
            local_path = os.path.join(local_dir, filename)
            hdfs_path = f'{hdfs_target_dir}/{filename}'
            print(f"Uploading {local_path} to {hdfs_path}")
            client.upload(hdfs_path, local_path, overwrite=True)

# DAG definition
with DAG(
    dag_id="extract_to_hdfs_dag",
    default_args=default_args,
    schedule=None,
    catchup=False,
    description="Downloads, extracts ZIP, uploads CSVs to HDFS",
) as dag:

    # Bash task to download and extract ZIP
    download_and_extract = BashOperator(
        task_id="download_and_extract",
        bash_command="""
            mkdir -p /opt/airflow/data/csvs &&
            wget -O /opt/airflow/data/student_data.zip https://analyse.kmi.open.ac.uk/open-dataset/download &&
            unzip -o /opt/airflow/data/student_data.zip -d /opt/airflow/data/csvs
        """
    )

    # Python task to upload CSVs to HDFS
    upload_to_hdfs = PythonOperator(
        task_id="upload_to_hdfs",
        python_callable=upload_to_hdfs_py
    )

    download_and_extract >> upload_to_hdfs

