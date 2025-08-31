from airflow import DAG
from airflow.providers.apache.livy.operators.livy import LivyOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='livy_spark_test',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['spark', 'livy']
) as dag:

    spark_test_job = LivyOperator(
        task_id="run_spark_job_cluster",
        livy_conn_id="livy",
        file="/opt/bitnami/spark/jobs/test_job.py",
        conf={
            "spark.master":"spark://spark-master:7077",
            "spark.app.name":"arrow-spark",
            "spark.pyspark.python":"python3",
            "spark.driver.memory": "512m",
            "spark.executor.memory": "512m",
            },
    )

    spark_test_job