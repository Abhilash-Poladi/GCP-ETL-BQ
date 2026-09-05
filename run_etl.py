import argparse
from shared.etl_utils import trigger_etl_job

def get_arguments():
    parser = argparse.ArgumentParser(description="ETL job for processing ecommerce orders.")
    
    parser.add_argument("--table_name", required=True)
    parser.add_argument("--load_date", required=True)
    parser.add_argument("--reprocess_flag", required=True)

    args = parser.parse_args()
    print(f"table_name: {args.table_name}")
    print(f"load_date: {args.load_date}")
    print(f"reprocess_flag: {args.reprocess_flag}")
    return args


if __name__ == '__main__':
    args = get_arguments()
    extract_zip()
    trigger_etl_job(args.table_name, args.load_date, args.reprocess_flag)



