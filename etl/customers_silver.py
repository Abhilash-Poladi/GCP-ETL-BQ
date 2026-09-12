from shared.spark_utils import init_spark
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
#from shared.etl_configs import config
from pyspark.sql import functions as F
from pyspark.sql import Window



# deduplicate the data (if same change row is present multiple times)
# handle multiple updates for same customer in source

def app(table_name, load_date, reprocess_flag):

    spark = init_spark(app_name=f"{table_name}_silver_scd2")

    # 1. Read the specific partition from the Bronze layer
    bronze_path = f"gs://ap-ecom-bronze/customers/"
    #df_incremental = spark.read.parquet(bronze_path).filter(f"load_dt = '{load_date}'")
    df_incremental = spark.read.parquet(bronze_path)

    # 2. creating surrogate key for the batch
    df_processed = df_incremental.withColumn(
        "customer_sk",
        F.sha2(
            F.concat_ws(
                "|",
                F.coalesce(F.col("customer_id").cast("string"), F.lit("")),
                F.coalesce(F.col("first_name").cast("string"), F.lit("")),
                F.coalesce(F.col("last_name").cast("string"), F.lit("")),
                F.coalesce(F.col("email").cast("string"), F.lit("")),
                F.coalesce(F.col("phone").cast("string"), F.lit("")),
                F.coalesce(F.col("street_address").cast("string"), F.lit("")),
                F.coalesce(F.col("city").cast("string"), F.lit("")),
                F.coalesce(F.col("state").cast("string"), F.lit("")),
                F.coalesce(F.col("zip_code").cast("string"), F.lit("")),
                F.coalesce(F.col("country").cast("string"), F.lit("")),
                F.coalesce(F.col("updated_at").cast("string"), F.lit("")),
            ),
            256,
        ),
    )

    print(df_processed.count())

    # deduplicate the data one row per change
    df_processed = df_processed.dropDuplicates(["customer_sk"])

    print(df_processed.count())

    # handle special cases where multiple updates might come in the same batch
    window_end_dt = Window.partitionBy("customer_id").orderBy(F.col("updated_at"))
    window_is_active = Window.partitionBy("customer_id").orderBy(F.col("updated_at").desc())


    df_processed = (
        df_processed.withColumn("eff_start_dt", F.to_timestamp("updated_at"))
        .withColumn("immediate_eff_start_dt", F.lead("eff_start_dt", 1).over(window_end_dt))
        .withColumn("rn", F.row_number().over(window_is_active))
        .withColumn("is_active", F.when(F.col("rn") == 1, True).otherwise(False))
        .withColumn("eff_end_dt", F.coalesce(F.col("immediate_eff_start_dt"), 
                                             F.lit("9999-12-31 23:59:59").cast("timestamp")))
        .drop("immediate_eff_start_dt", "rn")
        )
    

    # 3. Define BigQuery targets
    project_id = 'gp-ct-sbox-con-gcp07f-de'
    dataset_id = "ap_ecom_silver"
    target_table_id = f"{project_id}.{dataset_id}.{table_name}"
    staging_table_name = f"tmp_{table_name}_{load_date.replace('-', '')}"
    staging_table_id = f"{project_id}.{dataset_id}.{staging_table_name}"

    client = bigquery.Client()

    try:
        # Check if the target table exists
        client.get_table(target_table_id)

        # 4. Write incremental data to a temporary staging table
        df_processed.write.format("bigquery").option("table", staging_table_id).option(
            "temporaryGcsBucket", "ap-ecom-temp"
        ).mode("overwrite").save()

        # 5. Execute SCD2 MERGE in a single atomic statement
        # Logic: We union the source with itself.
        # - The first part of the union matches existing 'current' records to expire them.
        # - The second part (with a null merge key) ensures new records are always inserted.
        merge_query = f"""
                MERGE INTO `{target_table_id}` T
                USING (
                    WITH new_batch_data AS (
                        SELECT * FROM `{staging_table_id}` S
                        WHERE NOT EXISTS (
                            SELECT 1 FROM `{target_table_id}` T 
                            WHERE T.customer_sk = S.customer_sk
                        )
                    ),
                    final_records AS (
                        SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY updated_at ASC) AS rn
                        FROM new_batch_data
                    )
                    SELECT customer_id AS merge_key, * FROM final_records where rn = 1
                    UNION ALL
                    SELECT NULL AS merge_key, * FROM final_records
                ) S
                ON
                T.customer_id = S.merge_key AND T.is_active = True
                WHEN MATCHED THEN UPDATE SET T.eff_end_dt = S.eff_start_dt, T.is_active = False
                WHEN NOT MATCHED and S.merge_key is NULL THEN INSERT (
                        customer_sk,
                        customer_id,
                        first_name,
                        last_name,
                        email,
                        phone,
                        street_address,
                        city,
                        state,
                        zip_code,
                        country,
                        updated_at,
                        eff_start_dt,
                        eff_end_dt,
                        is_active,
                        load_dt
                    )
                    VALUES (
                        S.customer_sk,
                        S.customer_id,
                        S.first_name,
                        S.last_name,
                        S.email,
                        S.phone,
                        S.street_address,
                        S.city,
                        S.state,
                        S.zip_code,
                        S.country,
                        S.updated_at,
                        S.eff_start_dt,
                        S.eff_end_dt,
                        S.is_active,
                        S.load_dt
                    )

                """

        client.query(merge_query).result()

        # 6. Clean up the temporary staging table
        client.delete_table(staging_table_id, not_found_ok=True)

    except NotFound:
        # Initial Load: If table doesn't exist, create it from the processed dataframe
        print(f"Table {table_name} not found. Performing initial load.")
        df_processed.write.format("bigquery").option("table", target_table_id).option(
            "temporaryGcsBucket", "ap-ecom-temp"
        ).mode("overwrite").save()
