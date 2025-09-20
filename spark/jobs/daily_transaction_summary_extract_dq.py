from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count_distinct, regexp_replace, trim, sum as spark_sum
from pyspark.sql.types import *
import os
import time
import logging
import json
import argparse
import sys
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Define expected schema
EXPECTED_SCHEMA = {
    "fct_transactions": {
        "payment_key": StringType(),
        "customer_key": StringType(),
        "time_key": StringType(),
        "item_key": StringType(),
        "store_key": StringType(),
        "quantity": IntegerType(),
        "unit": StringType(),
        "unit_price": IntegerType(),
        "total_price": IntegerType()
    },
    "dim_item": {
        "item_key": StringType(),
        "item_name": StringType(),
        "desc": StringType(),
        "unit_price": FloatType(),
        "man_country": StringType(),
        "supplier": StringType(),
        "unit": StringType()
    },
    "dim_time": {
        "time_key": StringType(),
        "date": StringType(),
        "hour": IntegerType(),
        "day": IntegerType(),
        "week": StringType(),
        "month": IntegerType(),
        "quarter": StringType(),
        "year": IntegerType()
    }
}

TABLE_SOURCE = [
    ("public", "fct_transactions"),
    ("public", "dim_item"),
    ("public", "dim_time")
]

def create_spark_session(app_name: str):
    os.environ["HADOOP_USER_NAME"] = "airflow"
    spark = SparkSession.builder \
        .appName(app_name) \
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ACCESS_KEY", "minioadmin")) \
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_SECRET_KEY", "minioadmin")) \
        .config("spark.hadoop.fs.s3a.endpoint", os.getenv("MINIO_ENDPOINT", "http://minio:9000")) \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.hadoop.security.authentication", "simple") \
        .config("spark.hadoop.security.authorization", "false") \
        .config("spark.hadoop.fs.defaultFS", "file:///") \
        .config("spark.sql.warehouse.dir", "file:/tmp/spark-warehouse") \
        .getOrCreate()
    return spark

def dq_check(df, tablename):
    expected_schema = EXPECTED_SCHEMA[tablename]
    actual_columns = set(df.columns)
    expected_columns = set(expected_schema.keys())

    quality_metrics = {}
    quality_metrics[f"{tablename}_schema_validity"] = int(actual_columns == expected_columns) * 100

    if tablename == "fct_transactions":
        for col_name in ["customer_key", "item_key", "time_key", "quantity", "unit_price", "total_price"]:
            quality_metrics[f"{tablename}_null_{col_name}"] = (
                df.filter(col(col_name).isNotNull()).count() / df.count() * 100 if df.count() > 0 else 0
            )
        for col_name in ["quantity", "unit_price", "total_price"]:
            quality_metrics[f"{tablename}_negative_{col_name}"] = (
                df.filter(col(col_name) > 0).count() / df.count() * 100 if df.count() > 0 else 0
            )

    elif tablename == "dim_item":
        for col_name in ["item_key", "desc", "item_name", "unit_price"]:
            quality_metrics[f"{tablename}_null_{col_name}"] = (
                df.filter(col(col_name).isNotNull()).count() / df.count() * 100 if df.count() > 0 else 0
            )
        quality_metrics[f"{tablename}_negative_unit_price"] = (
            df.filter(col("unit_price") > 0).count() / df.count() * 100 if df.count() > 0 else 0
        )

    elif tablename == "dim_time":
        quality_metrics[f"{tablename}_null_time_key"] = (
            df.filter(col("time_key").isNotNull()).count() / df.count() * 100 if df.count() > 0 else 0
        )

    return quality_metrics

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--pg_url", required=True)
    parser.add_argument("--pg_user", required=True)
    parser.add_argument("--pg_pass", required=True)
    parser.add_argument("--exec_date", default=datetime.today().strftime("%Y-%m-%d"))  # optional
    return parser.parse_args()


def main():
    args = parse_args()
    
    spark = create_spark_session("Daily-Transaction-Summary-Extract-and-DQ")
    sc = spark.sparkContext
    jdbc_url = args.pg_url
    jdbc_user = args.pg_user
    jdbc_pass = args.pg_pass
    staging_path = "s3a://staging"
    dq_path = "s3a://staging-dq"
    current_date = args.exec_date
    
    dataframes = {}

    for schema, table in TABLE_SOURCE:
        df = spark.read \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", f"{schema}.{table}") \
            .option("user", jdbc_user) \
            .option("password", jdbc_pass) \
            .option("driver", "org.postgresql.Driver") \
            .load()

        dataframes[table] = df
        df.write.parquet(f"{staging_path}/{current_date}.{schema}.{table}.parquet", mode="overwrite")
        logger.info(f"✅ Extracted and saved: {table}")

    all_metrics = {}
    dq_records = []

    for table, df in dataframes.items():
        metrics = dq_check(df, table)
        all_metrics[table] = metrics
        for metric, val in metrics.items():
            dq_records.append({
                "dag_id": "dag_daily_transaction_summary",
                "table_name": table,
                "metric": metric,
                "value": float(val),
                "processed_at": datetime.now().isoformat(),
                "exec_date": current_date
            })

    dq_df = spark.createDataFrame(dq_records)
    dq_df.write.mode("overwrite").partitionBy("exec_date").parquet(f"{dq_path}/metrics.parquet")
    logger.info(f"Writing {len(dq_records)} DQ records for {current_date}")
    logger.info("✅ DQ metrics saved to MinIO (staging-dq)")

    logger.info(json.dumps(all_metrics, indent=2))

    failed = {
        table: {m: v for m, v in metrics.items() if v < 90}
        for table, metrics in all_metrics.items()
        if any(v < 90 for v in metrics.values())
    }

    if failed:
        raise Exception(f"❌ DQ check failed: {json.dumps(failed, indent=2)}")
    else:
        logger.info("✅ All DQ checks passed")

    try:
        spark.stop()
        sc.stop()
        logger.info("✅ Spark stopped cleanly")

        jvm = sc._gateway.jvm
        jvm.java.lang.System.exit(0)
    except Exception as e:
        logger.warning(f"Error during Spark shutdown: {e}")
    finally:
        os._exit(0)

if __name__ == "__main__":
    main()
