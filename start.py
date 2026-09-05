from pyspark.sql import SparkSession
from google.cloud import bigquery


spark = (
    SparkSession.builder
        .appName("intialloads")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
)

# Read from GCS
df = spark.read.format('csv').option('header', True).load(
    "gs://ap-ecom-raw/orders/"
)


(df.write
    .mode("overwrite").format('parquet')
    .partitionBy("load_dt")
    .parquet("gs://ap-ecom-bronze/orders/"))


client = bigquery.Client()

sql = """
CREATE OR REPLACE EXTERNAL TABLE ap_ecom_bronze.orders
OPTIONS (
    format = 'PARQUET',
    uris = ['gs://ap-ecom-bronze/orders/*']
)
"""

client.query(sql).result()

print("External table created successfully")