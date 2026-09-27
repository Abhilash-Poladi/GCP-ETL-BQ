from shared.spark_utils import init_spark
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from pyspark.sql.functions import col
from pyspark.sql.functions import *


def app(table_name, load_date, reprocess_flag):
    """
    Reads incremental data from Bronze (GCS) and appends it to the orders_history table in Silver (BigQuery).
    """
    spark = init_spark(app_name=f"{table_name}_silver_append")

     # 1. Read bronze data and select cols required, create cols needed, drop row level duplicates
    bronze_path = f"gs://ap-ecom-bronze/orders_bronze/"
    df_incremental = spark.read.parquet(bronze_path) \
        .filter(f"load_dt = '{load_date}'") \
        .select(col("order_id"), col("customer_id"), col("product_id"),
                col("order_ts"), col("quantity"), col("unit_price"), col("amount"),
                col("order_status"), col("updated_at"), col("load_dt")).withColumn("order_dt", to_date(col("order_ts"))) \
                .withColumn("updated_dt", col("updated_at")).dropDuplicates()
    
    # 2. read silver customers and fetch customer_sk 
    df_customers = spark.read.format("bigquery") \
        .option("table", "gp-ct-sbox-con-gcp07f-de.ap_ecom_silver.customers_silver") \
        .load() \
        .select(col("customer_id").alias("cust_id"), col("customer_sk"), col("eff_start_dt"), col("eff_end_dt"))

    df_incremental = df_incremental.join(df_customers,  
                    (df_incremental.customer_id == col("cust_id")) & 
                    (df_incremental.order_ts >= col("eff_start_dt")) & 
                    (df_incremental.order_ts < col("eff_end_dt")), 
                    "left") \
        .drop("cust_id", "eff_start_dt", "eff_end_dt")


    # 3. Compute the order_event_hash to detect functional changes
    df_incremental = df_incremental.withColumn(
        "order_event_hash",
        sha2(
            concat_ws("|", col("order_id"), col("customer_id"), col("product_id"), col("order_ts"), col("quantity"), col("unit_price"), col("amount"), col("order_status"), col("updated_at")),
            256
        )
    )

    # 4. Define BigQuery targets, sources, staging table
    project_id = 'gp-ct-sbox-con-gcp07f-de'
    dataset_id = "ap_ecom_silver"
    target_table_id = f"{project_id}.{dataset_id}.{table_name}" # This will be orders_history
    current_table_id = f"{project_id}.{dataset_id}.orders_current"
    staging_table_name = f"tmp_{table_name}_{load_date.replace('-', '')}"
    staging_table_id = f"{project_id}.{dataset_id}.{staging_table_name}"

    client = bigquery.Client()

    # 5, execute BQ Merge
    try:
        # 5.1 Check if the target table exists
        client.get_table(target_table_id)

        # 5.2 Write incremental data to a temporary staging table in BigQuery
        df_incremental.write.format("bigquery") \
            .option("table", staging_table_id) \
            .option("temporaryGcsBucket", "ap-ecom-temp") \
            .mode("overwrite") \
            .save()

        # 5.3 Execute merge query
        transaction_query = f"""
        INSERT INTO `{target_table_id}` (order_id, customer_id, product_id, order_ts, order_dt,quantity, unit_price, amount, order_status, updated_at,updated_dt, load_dt, order_event_hash)
        SELECT order_id, customer_id, product_id, order_ts, order_dt,quantity, unit_price, amount, order_status, updated_at, updated_dt, load_dt, order_event_hash 
        FROM `{staging_table_id}` S
        WHERE NOT EXISTS (
            SELECT 1 FROM `{target_table_id}` T 
            WHERE T.order_id = S.order_id AND T.order_event_hash = S.order_event_hash
        );
        """
        client.query(transaction_query).result()

        # 5.4 Clean up the temporary staging table
        client.delete_table(staging_table_id, not_found_ok=True)

    except NotFound:
        df_incremental.write.format("bigquery") \
            .option("table", target_table_id) \
            .option("partitionField", "updated_dt") \
            .option("partitionType", "DAY") \
            .option("temporaryGcsBucket", "ap-ecom-temp") \
            .mode("overwrite") \
            .save()

    spark.stop()