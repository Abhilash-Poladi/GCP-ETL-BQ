from datetime import datetime

from airflow import DAG

from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocSubmitJobOperator,
    DataprocDeleteClusterOperator
)

PROJECT_ID = "gp-ct-sbox-con-gcp07f-de"
REGION = "us-central1"

CLUSTER_NAME = "orders-etl-cluster"

CLUSTER_CONFIG = {
    "master_config": {
        "num_instances": 1,
        "machine_type_uri": "n1-standard-2",
    },
    "worker_config": {
        "num_instances": 2,
        "machine_type_uri": "n1-standard-2",
    },
}

PYSPARK_JOB = {
    "reference": {
        "project_id": PROJECT_ID
    },
    "placement": {
        "cluster_name": CLUSTER_NAME
    },
    "pyspark_job": {
        "main_python_file_uri":
            "gs://ap-ecom-etl-code/live-code/run_etl.py",
        "python_file_uris": [
            "gs://ap-ecom-etl-code/live-code/GCP_ETL_BQ.zip"
        ],
        "args": [
            "--table_name", "customers_silver",
            "--load_date", "2026-09-01",
            "--reprocess_flag", "false"
        ]
    }
}

with DAG(
    dag_id="bronze_orders_load",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    create_cluster = DataprocCreateClusterOperator(
        task_id="create_cluster",
        project_id=PROJECT_ID,
        region=REGION,
        cluster_name=CLUSTER_NAME,
        cluster_config=CLUSTER_CONFIG,
    )

    run_spark_job = DataprocSubmitJobOperator(
        task_id="run_spark_job",
        project_id=PROJECT_ID,
        region=REGION,
        job=PYSPARK_JOB,
        retries=0
    )

    create_cluster >> run_spark_job