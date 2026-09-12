from shared.spark_utils import init_spark
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from shared.etl_configs import config


def app(table_name, load_date, reprocess_flag):
    """
    Reads incremental data from Bronze (GCS) and merges it into Silver (BigQuery).
    """
    spark = init_spark(app_name=f"{table_name}_silver_merge")

    # 1. Read the specific partition from the Bronze layer
    # Assuming the partition structure is gs://bucket/table/load_dt=YYYY-MM-DD
    bronze_path = f"gs://ap-ecom-bronze/orders_bronze/"
    
    df_incremental = spark.read.parquet(bronze_path).filter(f"load_dt = '{load_date}'")

    # 2. Define BigQuery targets
    project_id = config.project_id
    dataset_id = "ecom_silver"
    target_table_id = f"{project_id}.{dataset_id}.{table_name}"
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

        # 4. Execute the MERGE DML statement
        merge_query = f"""
        MERGE `{target_table_id}` T
        USING `{staging_table_id}` S
        ON T.order_id = S.order_id
        WHEN MATCHED THEN
          UPDATE SET
            T.order_status = S.order_status,
            T.update_dt = S.update_dt,
            T.amount = S.amount
        WHEN NOT MATCHED THEN
          INSERT (order_id, customer_id, order_date, order_status, amount, load_dt, update_dt)
          VALUES (S.order_id, S.customer_id, S.order_date, S.order_status, S.amount, S.load_dt, S.update_dt)
        """
        client.query(merge_query).result()

        # 5. Clean up the temporary staging table
        client.delete_table(staging_table_id, not_found_ok=True)

    except NotFound:
        # If table doesn't exist, perform initial load directly
        df_incremental.write.format("bigquery") \
            .option("table", target_table_id) \
            .option("temporaryGcsBucket", "ap-ecom-temp") \
            .mode("append") \
            .save()
