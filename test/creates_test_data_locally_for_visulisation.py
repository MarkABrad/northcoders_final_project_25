from src.connection import connect_to_db, close_db_connection
from utils.utils_for_ingestion import reformat_data_to_json
import json

# for testing:


def table_reformat():
    list_of_tables = [
        "address",
        "staff",
        "department",
        "design",
        "counterparty",
        "sales_order",
        "transaction",
        "payment",
        "purchase_order",
        "payment_type",
        "currency",
    ]
    for table in list_of_tables:
        db = connect_to_db()
        format_data = reformat_data_to_json(table)

        with open(f"./json_data/json-{table}.json", "w") as file:
            json.dump({f"{table}": format_data}, file, indent=4)
    close_db_connection(db)
# table_reformat()
