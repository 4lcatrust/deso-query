from airflow import DAG
from airflow.providers.apache.livy.operators.livy import LivyOperator
from airflow.utils.trigger_rule import TriggerRule
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
    dag_id='livy_hello_test',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['spark', 'livy']
) as dag:

    spark_test_job = LivyOperator(
        task_id="run_spark_job_cluster",
        trigger_rule=TriggerRule.ALL_SUCCESS,
        livy_conn_id="livy",
        file="/opt/bitnami/spark/jobs/hello_job.py",
        conf={
            "spark.master":"spark://spark-master:7077",
            "spark.app.name":"arrow-spark",
            "spark.pyspark.python":"python3",
            "spark.driver.memory": "512m",
            "spark.executor.memory": "512m",
            },
        polling_interval=5,      # seconds; >0 means “keep polling until done”
    )

    spark_test_job