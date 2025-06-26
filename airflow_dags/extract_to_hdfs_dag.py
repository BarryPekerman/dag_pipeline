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

# Step 1: Download and extract the dataset
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

# Step 2: Upload to HDFS
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

# Step 3: Run PySpark aggregation and write to PostgreSQL
def aggregate_with_pyspark():
    spark = SparkSession.builder \
        .appName("StudentAggregator") \
        .master("local[*]") \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3") \
        .getOrCreate()

    print("Reading CSV files from HDFS", flush=True)

    sa_path = f"{HDFS_NAMENODE}/datasets/studentAssessment.csv"
    a_path = f"{HDFS_NAMENODE}/datasets/assessments.csv"
    si_path = f"{HDFS_NAMENODE}/datasets/studentInfo.csv"

    student_assessments = spark.read.csv(sa_path, header=True, inferSchema=True)
    assessments = spark.read.csv(a_path, header=True, inferSchema=True)
    student_info = spark.read.csv(si_path, header=True, inferSchema=True)

    print("Joining and cleaning data...", flush=True)

    # Join studentAssessment with assessments to get code_module
    joined = student_assessments.join(assessments, on="id_assessment", how="inner") \
                                .join(student_info, on="id_student", how="inner")

    # Filter: score is not null, is_banked = 0, student did not withdraw
    clean = joined.filter(
        (F.col("score").isNotNull()) &
        (F.col("is_banked") == 0) &
        (F.col("final_result") != "Withdrawn")
    )

    # Aggregate average score per module
    avg_scores = clean.groupBy("code_module").agg(F.avg("score").alias("avg_score"))

    print("Result preview:")
    avg_scores.show()

    jdbc_url = "jdbc:postgresql://postgres-postgresql.postgres.svc.cluster.local:5432/airflow_db"
    jdbc_props = {
        "user": "airflow",
        "password": "airflowpass",
        "driver": "org.postgresql.Driver"
    }

    print("Writing avg_scores_by_module to PostgreSQL...", flush=True)
    avg_scores.write.jdbc(url=jdbc_url, table="avg_scores_by_module", mode="overwrite", properties=jdbc_props)

    print("Aggregation complete.", flush=True)
    spark.stop()

# DAG definition
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

