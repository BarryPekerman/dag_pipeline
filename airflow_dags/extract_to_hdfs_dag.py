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
    schedule_interval=None,
    catchup=False,
    description="Extracts CSVs from ZIP and loads to HDFS",
) as dag:

    extract_zip = BashOperator(
        task_id="extract_zip",
        bash_command="unzip -o /opt/airflow/data/student_data.zip -d /opt/airflow/data/csvs"
    )

    upload_to_hdfs = BashOperator(
        task_id="upload_to_hdfs",
        bash_command="hdfs dfs -mkdir -p /data/student_csvs && hdfs dfs -put -f /opt/airflow/data/csvs/*.csv /data/student_csvs/"
    )

    extract_zip >> upload_to_hdfs

