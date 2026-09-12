import os
import zipfile
import importlib
import importlib.util
import sys

def trigger_etl_job(table_name, load_date, reprocess_flag):
    """
    Unzips dependencies localized by Dataproc and triggers the ETL job.
    """
    module_name = f"etl.{table_name}"
    cwd = os.getcwd()

    try:
        print(f"--- Attempting to import ETL module: {module_name} ---")
        etl_module = importlib.import_module(module_name)
    except ImportError as e:
        # If the ImportError is due to a dependency inside the module failing to load, reraise it
        if e.name != module_name:
            raise e
        raise ImportError(f"ETL module '{module_name}' could not be found in the current path.") from e

    if hasattr(etl_module, 'app'):
        etl_module.app(table_name=table_name, load_date=load_date, reprocess_flag=reprocess_flag)
    else:
        raise AttributeError(f"The module for {table_name} does not define an 'app' function.")



def extract_zip(file_name):
    """Extracts the given zip file to the current working directory."""
    cwd = os.getcwd()
    file_path = os.path.join(cwd, file_name)
    if os.path.exists(file_path):
        print(f"--- Extracting {file_name} to {cwd} ---")
        with zipfile.ZipFile(file_path, 'r') as zip_ref:
            zip_ref.extractall(cwd)
    else:
        print(f"Warning: Zip file {file_name} not found in {cwd}")
    