from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta
from module.utilities import get_airflow_variables


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
    dag_id='dag_daily_transaction_summary_extract_dq',
    default_args=default_args,
    schedule_interval=None,
    catchup=False
) as dag:
    start_task = DummyOperator(
        task_id='start',
        dag=dag
    )

    extract_dq_task = SparkSubmitOperator(
        task_id="extract_dq",
        conn_id="spark",
        application=get_airflow_variables("LOCAL_AIRFLOW_PATH") + "spark-jobs/daily_transaction_summary_extract_dq.py",
        name="daily_transaction_summary_extract_and_dq",
        conf={
            "spark.jars.ivy": "/opt/bitnami/.ivy2",
            "spark.driver.userClassPathFirst": "true",
            "spark.executor.userClassPathFirst": "true",
            "spark.driver.extraClassPath": "/opt/bitnami/spark/jars/*",
            "spark.executor.extraClassPath": "/opt/bitnami/spark/jars/*",
            "spark.hadoop.fs.s3a.access.key": get_airflow_variables("MINIO_ACCESS_KEY"),
            "spark.hadoop.fs.s3a.secret.key": get_airflow_variables("MINIO_SECRET_KEY"),
            "spark.hadoop.fs.s3a.endpoint": "http://minio:9000",
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "spark.hadoop.fs.s3a.path.style.access": "true",
            "spark.hadoop.fs.s3a.connection.ssl.enabled": "false"
        },
        # jars=",".join([
        #     get_airflow_variables("LOCAL_AIRFLOW_PATH") + "driver/postgresql-42.7.5.jar",
        #     get_airflow_variables("LOCAL_AIRFLOW_PATH") + "driver/clickhouse-jdbc-0.8.0.jar",
        #     get_airflow_variables("LOCAL_AIRFLOW_PATH") + "driver/hadoop-aws-3.3.6.jar",
        #     get_airflow_variables("LOCAL_AIRFLOW_PATH") + "driver/hadoop-common-3.3.6.jar",
        #     get_airflow_variables("LOCAL_AIRFLOW_PATH") + "driver/hadoop-auth-3.3.6.jar",
        #     get_airflow_variables("LOCAL_AIRFLOW_PATH") + "driver/aws-java-sdk-bundle-1.11.1026.jar",
        # f"{shared_jar_path}/postgresql-42.7.5.jar",
        # f"{shared_jar_path}/clickhouse-jdbc-0.8.0.jar",
        # f"{shared_jar_path}/hadoop-aws-3.3.6.jar",
        # f"{shared_jar_path}/hadoop-common-3.3.6.jar",
        # f"{shared_jar_path}/hadoop-auth-3.3.6.jar",
        # f"{shared_jar_path}/aws-java-sdk-bundle-1.11.1026.jar",
        # f"{shared_jar_path}/woodstox-core-6.4.0.jar",
        # ]),
        application_args=[
            "--pg_url", get_airflow_variables("POSTGRES_JDBC_URL"),
            "--pg_user", get_airflow_variables("POSTGRES_USER"),
            "--pg_pass", get_airflow_variables("POSTGRES_PASSWORD"),
            "--exec_date", "{{ ds }}"
        ],
        dag=dag
    )

    start_task >> extract_dq_task