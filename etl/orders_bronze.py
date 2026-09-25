from shared.spark_utils import init_spark
from google.cloud import bigquery
from pyspark.sql import functions as F


def app(table_name, load_date, reprocess_flag):
    spark = init_spark(app_name="orders_bronze")
    df = (
        spark.read.format("csv")
        .option("header", True)
        .option("inferSchema", True)
        .load("gs://ap-ecom-raw/orders/")
    )

    df.write.mode("overwrite").format("parquet").partitionBy("load_dt").parquet(
        "gs://ap-ecom-bronze/orders_bronze/"
    )

    client = bigquery.Client()
    sql = f"""
        CREATE OR REPLACE EXTERNAL TABLE
        `gp-ct-sbox-con-gcp07f-de.ap_ecom_bronze.{table_name}`
        WITH PARTITION COLUMNS (
            load_dt DATE
        )
        OPTIONS (
            format = 'PARQUET',
            uris = ['gs://ap-ecom-bronze/orders_bronze/*'],
            hive_partition_uri_prefix = 'gs://ap-ecom-bronze/orders_bronze'
        )
        """

    try:
        client.query(sql).result()
        print(f"Successfully updated external table definition for {table_name}")
    except Exception as e:
        print(f"Failed to update BigQuery external table: {e}")
