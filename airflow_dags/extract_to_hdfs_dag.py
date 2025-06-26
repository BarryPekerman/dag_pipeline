from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta
from hdfs import InsecureClient
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import requests, zipfile, pathlib
from io import BytesIO
import os

default_args = {
    "owner": "airflow",
    "start_date": datetime.now() - timedelta(days=1),
    "retries": 1,
}

HDFS_URL = "http://hadoop-hadoop-hdfs-nn.hadoop.svc.cluster.local:9870"
HDFS_NAMENODE = "hdfs://hadoop-hadoop-hdfs-nn.hadoop.svc.cluster.local:9000"
HDFS_USER = "airflow"
EXTRACT_DIR = "/opt/airflow/data/csvs"

def download_and_extract_zip():
    url = "https://analyse.kmi.open.ac.uk/open-dataset/download"
    target_dir = EXTRACT_DIR

    print(f"Downloading ZIP from {url}", flush=True)
    response = requests.get(url, allow_redirects=True)
    if response.status_code != 200:
        raise Exception(f"Failed to download file: {response.status_code}")

    pathlib.Path(target_dir).mkdir(parents=True, exist_ok=True)

    with zipfile.ZipFile(BytesIO(response.content)) as zip_file:
        zip_file.extractall(target_dir)
        print(f"Extracted files to: {target_dir}", flush=True)

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

def aggregate_with_pyspark():
    spark = SparkSession.builder \
        .appName("AvgStudentScores") \
        .master("local[*]") \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3") \
        .getOrCreate()

    assessments_path = f"{HDFS_NAMENODE}/datasets/assessments.csv"
    student_assessments_path = f"{HDFS_NAMENODE}/datasets/studentAssessment.csv"

    assessments_df = spark.read.csv(assessments_path, header=True, inferSchema=True)
    student_df_raw = spark.read.csv(student_assessments_path, header=True, inferSchema=True)

    student_df = student_df_raw.filter("score IS NOT NULL AND score != ''") \
                               .withColumn("score", F.col("score").cast("double")) \
                               .filter("score >= 0 AND score <= 100")

    joined_df = student_df.join(assessments_df, on="id_assessment", how="inner")

    result_df = joined_df.groupBy("code_module").agg(F.round(F.avg("score"), 2).alias("avg_score"))

    jdbc_url = "jdbc:postgresql://postgres-postgresql.postgres.svc.cluster.local:5432/airflow_db"
    jdbc_properties = {
        "user": "airflow",
        "password": "airflowpass",
        "driver": "org.postgresql.Driver"
    }

    print("Writing aggregated results to PostgreSQL", flush=True)
    result_df.write.jdbc(
        url=jdbc_url,
        table="avg_scores_by_module",
        mode="overwrite",
        properties=jdbc_properties
    )

    print("Write complete.", flush=True)
    spark.stop()

with DAG(
    "my_pipeline",
    default_args=default_args,
    schedule=None,
    catchup=False,
    description="Download ZIP, extract CSVs, upload to HDFS, write PySpark results to PostgreSQL",
) as dag:

    download_and_extract = PythonOperator(
        task_id="download_and_extract",
        python_callable=download_and_extract_zip
    )

    upload_to_hdfs = PythonOperator(
        task_id="upload_to_hdfs",
        python_callable=upload_to_hdfs_task
    )

    aggregate_task = PythonOperator(
        task_id="aggregate_with_pyspark",
        python_callable=aggregate_with_pyspark
    )

    download_and_extract >> upload_to_hdfs >> aggregate_task

