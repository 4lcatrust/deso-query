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
    dag_id='spark_test',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['spark']
) as dag:

    spark_test_job = SparkSubmitOperator(
        task_id="run_spark_job",
        application="/opt/bitnami/spark/jobs/test_job.py",  # mounted on spark-master
        conn_id="spark",  # make sure you configure this in Airflow UI
        name="arrow-spark",
        conf={"spark.jars.ivy": "/opt/bitnami/.ivy2"},
        verbose=True
    )

    spark_test_job
