from datetime import datetime

from airflow import DAG

from airflow.models.param import Param
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

table_name = "{{ params.table_name }}"
load_date = "{{ params.load_date }}"
reprocess_flag = "{{ params.reprocess_flag }}"

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
            "--table_name", table_name,
            "--load_date", load_date,
            "--reprocess_flag", reprocess_flag
        ]
    }
}

with DAG(
    dag_id="dry_run",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    params={
        "table_name": Param("customers_bronze", type="string", description="The target table name"),
        "load_date": Param("2026-09-02", type="string", description="The load date in YYYY-MM-DD format"),
        "reprocess_flag": Param("false", type="string", description="Set to 'true' if you want to reprocess the data"),
    },
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