from shared.spark_utils import init_spark
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from pyspark.sql.functions import col
from shared.etl_configs import config
from pyspark.sql import functions as F


def app(table_name, load_date, reprocess_flag):
    """
    Reads incremental data from Bronze (GCS) and appends it to the orders_history table in Silver (BigQuery).
    """
    spark = init_spark(app_name=f"{table_name}_silver_append")

    # 1. Read the specific partition from the Bronze layer
    # Assuming the partition structure is gs://bucket/table/load_dt=YYYY-MM-DD
    bronze_path = f"gs://ap-ecom-bronze/orders_bronze/"
    
    df_incremental = spark.read.parquet(bronze_path) \
        .filter(f"load_dt = '{load_date}'") \
        .select(col("order_id"), col("customer_id"), col("product_id"),
                col("order_ts"), col("quantity"), col("unit_price"), col("amount"),
                col("order_status"), col("updated_at"), col("load_dt"))

    # 2. Compute the order_event_hash to detect functional changes
    df_incremental = df_incremental.withColumn(
        "order_event_hash",
        F.sha2(
            F.concat_ws("|", col("order_id"), col("customer_id"), col("product_id"), col("order_ts"), col("quantity"), col("unit_price"), col("amount"), col("order_status"), col("updated_at")),
            256
        )
    )

    # 2. Define BigQuery targets
    project_id = config.project_id
    dataset_id = "ecom_silver"
    target_table_id = f"{project_id}.{dataset_id}.{table_name}" # This will be orders_history
    current_table_id = f"{project_id}.{dataset_id}.orders_current"

    staging_table_name = f"tmp_{table_name}_{load_date.replace('-', '')}"
    staging_table_id = f"{project_id}.{dataset_id}.{staging_table_name}"

    client = bigquery.Client()

    try:
        # Check if the target table exists
        client.get_table(target_table_id)

        # 3. Write incremental data to a temporary staging table in BigQuery
        df_incremental.write.format("bigquery") \
            .option("table", staging_table_id) \
            .option("temporaryGcsBucket", "ap-ecom-temp") \
            .mode("overwrite") \
            .save()

        # 4. Execute atomic transaction to ensure idempotency without partial failures
        transaction_query = f"""
        BEGIN TRANSACTION;

        DELETE FROM `{target_table_id}` WHERE load_dt = '{load_date}';

        INSERT INTO `{target_table_id}` (order_id, customer_id, product_id, order_ts, quantity, unit_price, amount, order_status, updated_at, load_dt, order_event_hash)
        SELECT order_id, customer_id, product_id, order_ts, quantity, unit_price, amount, order_status, updated_at, load_dt, order_event_hash 
        FROM `{staging_table_id}` S
        WHERE NOT EXISTS (
            SELECT 1 FROM `{current_table_id}` C 
            WHERE C.order_id = S.order_id AND C.order_event_hash = S.order_event_hash
        );

        COMMIT TRANSACTION;
        """
        client.query(transaction_query).result()

        # 5. Clean up the temporary staging table
        client.delete_table(staging_table_id, not_found_ok=True)

    except NotFound:
        # If table doesn't exist, perform initial load directly (append mode is fine for history)
        df_incremental.write.format("bigquery") \
            .option("table", target_table_id) \
            .option("partitionField", "load_dt") \
            .option("partitionType", "DAY") \
            .option("temporaryGcsBucket", "ap-ecom-temp") \
            .mode("overwrite") \
            .save()

    spark.stop()