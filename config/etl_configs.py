from dataclasses import dataclass


@dataclass
class DataConfig:
    zip_file_name = 'GCP_ETL_BQ.zip'
    project_id = 'gp-ct-sbox-con-gcp07f-de'

config = DataConfig()



