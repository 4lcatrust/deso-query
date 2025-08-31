from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
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
    dag_id='standalone_spark_test',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['spark', 'standalone']
) as dag:

    spark_test_job = SparkSubmitOperator(
        task_id="run_spark_job",
        application="/opt/bitnami/spark/jobs/test_job.py",
        conn_id="spark",
        conf={
            "spark.master": "spark://spark-master:7077",
            "spark.submit.deployMode": "cluster",
            "spark.app.name": "arrow-spark"
        },
        verbose=True
    )

    spark_test_job
