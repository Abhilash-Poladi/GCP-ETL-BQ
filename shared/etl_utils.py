import os
import zipfile
import importlib
import importlib.util
import sys

def trigger_etl_job(table_name, load_date, reprocess_flag):
    """
    Unzips dependencies localized by Dataproc and triggers the ETL job.
    """
    cwd = os.getcwd()
    
    # 1. Unzip any localized archives found in the CWD
    for file in os.listdir(cwd):
        if file.endswith(".zip"):
            print(f"--- Extracting localized dependency: {file} to {cwd} ---")
            try:
                with zipfile.ZipFile(file, 'r') as zip_ref:
                    zip_ref.extractall(cwd)
            except Exception as e:
                print(f"Warning: Failed to unzip {file}: {e}")

    
    try:
        print(f"--- Attempting to import ETL module: {module_name} ---")
        etl_module = importlib.import_module(module_name)
    except ImportError:
        print(f"--- Module {module_name} not found in path, attempting path-based load ---")
        
        # Try to locate the script in the local project structure or the unzipped CWD path
        project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        paths_to_check = [
            os.path.join(project_root, 'etl', f"{table_name}.py"),
            os.path.join(cwd, 'etl', f"{table_name}.py")
        ]
        
        script_path = next((p for p in paths_to_check if os.path.exists(p)), None)
        
        if not script_path:
            raise ImportError(f"Could not load ETL job for {table_name}. Checked paths: {paths_to_check}")

    #         spec = importlib.util.spec_from_file_location(table_name, script_path)
        etl_module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(etl_module)

    if hasattr(etl_module, 'app'):
        etl_module.app(load_date=load_date, reprocess_flag=reprocess_flag)
    else:
        raise AttributeError(f"The module for {table_name} does not define an 'app' function.")