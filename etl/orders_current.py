from shared.spark_utils import init_spark
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from pyspark.sql.functions import col, to_date


def app(table_name, load_date, reprocess_flag):
    """
    Reads incremental data from Bronze (GCS) and merges it into Silver (BigQuery).
    """
    spark = init_spark(app_name=f"{table_name}_silver_merge")

    # 1. Read the specific partition from the Bronze layer
    # Assuming the partition structure is gs://bucket/table/load_dt=YYYY-MM-DD
    bronze_path = f"gs://ap-ecom-bronze/orders_bronze/"
    
    df_incremental = spark.read.parquet(bronze_path) \
        .filter(f"load_dt = '{load_date}'") \
        .select(col("order_id"), col("customer_id"), col("product_id"),
                col("order_ts"), col("quantity"), col("unit_price"), col("amount"),
                col("order_status"), col("updated_at"), col("load_dt")) \
        .withColumn("order_dt", to_date(col("order_ts")))

    # 2. Define BigQuery targets
    project_id = 'gp-ct-sbox-con-gcp07f-de'
    dataset_id = "ap_ecom_silver"
    target_table_id = f"{project_id}.{dataset_id}.{table_name}"
    staging_table_name = f"tmp_{table_name}_{load_date.replace('-', '')}"
    staging_table_id = f"{project_id}.{dataset_id}.{staging_table_name}"

    # 2.5. Load silver customers to fetch the point-in-time customer_sk (SCD2 lookup)
    df_customers = spark.read.format("bigquery") \
        .option("table", f"{project_id}.ap_ecom_silver.customers_silver") \
        .load() \
        .select(col("customer_id").alias("cust_id"), col("customer_sk"), col("eff_start_dt"), col("eff_end_dt"))

    df_incremental = df_incremental.join(
        df_customers,
        (df_incremental.customer_id == col("cust_id")) &
        (df_incremental.order_ts >= col("eff_start_dt")) &
        (df_incremental.order_ts < col("eff_end_dt")),
        "left"
    ).drop("cust_id", "eff_start_dt", "eff_end_dt")

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

        # 4. Execute the MERGE DML statement
        merge_query = f"""
        MERGE `{target_table_id}` T
        USING `{staging_table_id}` S
        ON T.order_id = S.order_id
        WHEN MATCHED THEN
          UPDATE SET
            T.customer_id = S.customer_id,
            T.customer_sk = S.customer_sk,
            T.product_id = S.product_id,
            T.order_ts = S.order_ts,
            T.quantity = S.quantity,
            T.unit_price = S.unit_price,
            T.amount = S.amount,
            T.order_status = S.order_status,
            T.updated_at = S.updated_at,
            T.load_dt = S.load_dt
        WHEN NOT MATCHED THEN
          INSERT (order_id, customer_id, customer_sk, product_id, order_ts, order_dt, quantity, unit_price, amount, order_status, updated_at, load_dt)
          VALUES (S.order_id, S.customer_id, S.customer_sk, S.product_id, S.order_ts, S.order_dt, S.quantity, S.unit_price, S.amount, S.order_status, S.updated_at, S.load_dt)
        """
        client.query(merge_query).result()

        # 5. Clean up the temporary staging table
        client.delete_table(staging_table_id, not_found_ok=True)

    except NotFound:
        # If table doesn't exist, perform initial load directly
        df_incremental.write.format("bigquery") \
            .option("table", target_table_id) \
            .option("partitionField", "order_dt") \
            .option("partitionType", "DAY") \
            .option("temporaryGcsBucket", "ap-ecom-temp") \
            .mode("overwrite") \
            .save()

    spark.stop() # Stop Spark session
