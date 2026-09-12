from shared.spark_utils import init_spark


def app(table_name, load_date, reprocess_flag):
    spark = init_spark(app_name="customers_bronze")
    df = (
        spark.read.format("csv").option("header", True).load("gs://ap-ecom-raw/customers/")
    )


    df.write.mode("overwrite").format("parquet").partitionBy("load_dt").parquet(
        "gs://ap-ecom-bronze/customers/"
    )
