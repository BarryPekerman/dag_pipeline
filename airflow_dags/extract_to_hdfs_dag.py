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

def get_common_dataframes(spark):
    sa_path = f"{HDFS_NAMENODE}/datasets/studentAssessment.csv"
    a_path = f"{HDFS_NAMENODE}/datasets/assessments.csv"
    si_path = f"{HDFS_NAMENODE}/datasets/studentInfo.csv"
    sa = spark.read.csv(sa_path, header=True, inferSchema=True)
    a = spark.read.csv(a_path, header=True, inferSchema=True)
    si = spark.read.csv(si_path, header=True, inferSchema=True)
    joined = sa.join(a, on="id_assessment", how="inner").join(si, on="id_student", how="inner")
    return joined, a

def aggregate_by_module():
    spark = SparkSession.builder \
        .appName("AggregateByModule") \
        .master("local[*]") \
        .config("spark.jars", "/opt/mysql-connector-java.jar") \
        .getOrCreate()

    joined, assessments = get_common_dataframes(spark)
    clean = joined.select(
        "id_assessment",
        "id_student",
        assessments["code_module"].alias("module"),
        "score",
        "is_banked",
        "final_result"
    ).filter(
        (F.col("score").isNotNull()) &
        (F.col("is_banked") == 0) &
        (F.col("final_result") != "Withdrawn")
    )

    avg_scores = clean.groupBy("module").agg(F.avg("score").alias("avg_score"))
    avg_scores.show()

    jdbc_url = "jdbc:mysql://mysql.mysql.svc.cluster.local:3306/airflow_db"
    jdbc_props = {"user": "airflow", "password": "airflowpassword", "driver": "com.mysql.cj.jdbc.Driver"}

    avg_scores.write.jdbc(url=jdbc_url, table="avg_scores_by_module", mode="overwrite", properties=jdbc_props)
    spark.stop()

def aggregate_by_student():
    spark = SparkSession.builder \
        .appName("AggregateByStudent") \
        .master("local[*]") \
        .config("spark.jars", "/opt/mysql-connector-java.jar") \
        .getOrCreate()

    joined, _ = get_common_dataframes(spark)
    clean = joined.select("id_student", "score", "is_banked").filter(
        (F.col("score").isNotNull()) &
        (F.col("is_banked") == 0)
    )

    avg_scores = clean.groupBy("id_student").agg(F.avg("score").alias("avg_score"))
    avg_scores.show()

    jdbc_url = "jdbc:mysql://mysql.mysql.svc.cluster.local:3306/airflow_db"
    jdbc_props = {"user": "airflow", "password": "airflowpassword", "driver": "com.mysql.cj.jdbc.Driver"}

    avg_scores.write.jdbc(url=jdbc_url, table="avg_scores_by_student", mode="overwrite", properties=jdbc_props)
    spark.stop()

with DAG(
    "my_pipeline",
    default_args=default_args,
    schedule=None,
    catchup=False,
    description="Download ZIP, extract CSVs, upload to HDFS, then run parallel PySpark aggregations to MySQL",
) as dag:

    download_and_extract = PythonOperator(
        task_id="download_and_extract",
        python_callable=download_and_extract_zip
    )

    upload_to_hdfs = PythonOperator(
        task_id="upload_to_hdfs",
        python_callable=upload_to_hdfs_task
    )

    aggregate_by_module_task = PythonOperator(
        task_id="aggregate_by_module",
        python_callable=aggregate_by_module
    )

    aggregate_by_student_task = PythonOperator(
        task_id="aggregate_by_student",
        python_callable=aggregate_by_student
    )

    download_and_extract >> upload_to_hdfs >> [aggregate_by_module_task, aggregate_by_student_task]

