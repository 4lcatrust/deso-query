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
    catchup=False
) as dag:
    
    spark_submit = SparkSubmitOperator(
        task_id='submit_spark_job',
        application='/opt/airflow/dags/spark-jobs/test_job.py',
        conn_id='spark',
        conf={
            "spark.jars.ivy": "/opt/bitnami/.ivy2"
        },
    )

    spark_submit