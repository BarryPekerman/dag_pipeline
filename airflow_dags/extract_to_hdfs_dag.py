from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "airflow",
    "start_date": datetime.now() - timedelta(days=1),
    "retries": 1,
}

with DAG(
    dag_id="extract_to_hdfs_dag",
    default_args=default_args,
    schedule=None,
    catchup=False,
    description="Downloads and extracts ZIP, uploads CSVs to HDFS",
) as dag:

    download_and_extract = BashOperator(
        task_id="download_and_extract",
        bash_command="""
            mkdir -p /opt/airflow/data/csvs &&
            wget -O /opt/airflow/data/student_data.zip https://analyse.kmi.open.ac.uk/open-dataset/download &&
            unzip -o /opt/airflow/data/student_data.zip -d /opt/airflow/data/csvs
        """
    )

    upload_to_hdfs = BashOperator(
        task_id="upload_to_hdfs",
        bash_command="""
            hdfs dfs -mkdir -p /data/student_csvs &&
            hdfs dfs -put -f /opt/airflow/data/csvs/*.csv /data/student_csvs/
        """
    )

    download_and_extract >> upload_to_hdfs

