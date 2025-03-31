from pg8000.native import identifier
from s3_read_function.s3_read_function import read_files_from_s3
from src.connection import connect_to_db, close_db_connection
from src.create_raw_tables import check_formatting_of_input, make_raw_tables
from utils.get_bucket import get_bucket_name
from src.create_dim_tables import make_dim_tables

# This function is for integration testing the
# lambda 2 handler for the dimensions tables

bucket_name = get_bucket_name("ingestion-bucket")

table_list = [
    "address",
    "counterparty",
    "currency",
    "department",
    "design",
    "payment",
    "payment_type",
    "purchase_order",
    "sales_order",
    "staff",
    "transaction",
    "dim_date",
    "dim_staff",
    "dim_location",
    "dim_currency",
    "dim_design",
    "dim_counterparty",
]

downloaded_files = read_files_from_s3(bucket_name)
lambda_local_db = connect_to_db()
for table in table_list:
    lambda_local_db.run(f"DROP TABLE IF EXISTS {identifier(table)}")
    print(f"DROPPED {table}")
if check_formatting_of_input(downloaded_files):
    table_names = make_raw_tables(lambda_local_db, downloaded_files)[0]
    dim_value_rows = make_dim_tables(lambda_local_db, table_names)
close_db_connection(lambda_local_db)
