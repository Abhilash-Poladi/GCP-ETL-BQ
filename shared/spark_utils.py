from pyspark.sql import SparkSession

def init_spark(app_name):
    spark = (
        SparkSession.builder
            .appName(app_name)
            .config("spark.sql.sources.partitionOverwriteMode", "DYNAMIC")
            .getOrCreate()
    )
    return spark

