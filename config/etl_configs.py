from dataclasses import dataclass


@dataclass
class DataConfig:
    zip_file_name = 'GCP_ETL_BQ.zip'


config = DataConfig()