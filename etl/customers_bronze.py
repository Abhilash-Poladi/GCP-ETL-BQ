from shared.spark_utils import init_spark
from google.cloud import bigquery

def app(table_name, load_date, reprocess_flag):
    spark = init_spark(app_name="customers_bronze")
    df = (
        spark.read.format("csv").option("header", True).load("gs://ap-ecom-raw/customers/")
    )


    df.write.mode("overwrite").format("parquet").partitionBy("load_dt").parquet(
        "gs://ap-ecom-bronze/customers/"
    )

    client = bigquery.Client()
    sql = """
        CREATE OR REPLACE EXTERNAL TABLE
        `gp-ct-sbox-con-gcp07f-de.ap_ecom_bronze.customers_bronze`
        (
            customer_id STRING,
            first_name STRING,
            last_name STRING,
            email STRING,
            phone STRING,
            street_address STRING,
            city STRING,
            state STRING,
            zip_code STRING,
            country STRING,
            updated_at TIMESTAMP
        )
        WITH PARTITION COLUMNS (
            load_dt DATE
        )
        OPTIONS (
            format = 'PARQUET',
            uris = ['gs://ap-ecom-bronze/customers/*'],
            hive_partition_uri_prefix = 'gs://ap-ecom-bronze/customers'
        )
        """

    client.query(sql).result()
    print(f"successfully created external table {table_name}")