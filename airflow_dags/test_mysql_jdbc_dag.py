from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta
from pyspark.sql import SparkSession

default_args = {
    "owner": "airflow",
    "start_date": datetime.now() - timedelta(days=1),
    "retries": 1,
}

def test_mysql_jdbc():
    spark = SparkSession.builder \
        .appName("MySQLJDBCTest") \
        .master("local[*]") \
        .config("spark.jars", "/opt/mysql-connector-java.jar") \
        .getOrCreate()

    jdbc_url = "jdbc:mysql://mysql.mysql.svc.cluster.local:3306/airflow_db"
    jdbc_props = {
        "user": "airflow",
        "password": "airflowpassword",
        "driver": "com.mysql.cj.jdbc.Driver"
    }

    try:
        df = spark.read.jdbc(url=jdbc_url, table="information_schema.tables", properties=jdbc_props)
        df.show(5)
    except Exception as e:
        print("❌ JDBC connection failed:", e)
        raise
    finally:
        spark.stop()

with DAG(
    "test_mysql_jdbc",
    default_args=default_args,
    schedule=None,
    catchup=False,
    description="Test MySQL JDBC connection with Spark",
) as dag:

    test_conn = PythonOperator(
        task_id="test_jdbc_connection",
        python_callable=test_mysql_jdbc
    )

