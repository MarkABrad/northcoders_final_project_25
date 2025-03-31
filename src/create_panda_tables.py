import pandas as pd
from s3_read_function.s3_read_function import read_files_from_s3
from utils.get_bucket import get_bucket_name
from src.fact_sales_order import connect_to_db
from dotenv import load_dotenv
from pprint import pprint
import json
from sqlalchemy import text


bucket_name = get_bucket_name("ingestion-bucket")
"""
This function is to set out an alternative way to stage the data from read files from s3 using Pandas DataFrames in case Joe can't debug his utils

"""

load_dotenv()
db_connection = connect_to_db()


def create_pandas_table(db_connection, input_data):
    # extract the data from s3_read_function

    # store return pandas dataframe in dictionary
    pandas_df_dict = {}

    # iterate through input_data keys and values and create dataframes for each one

    for key, value in input_data.items():
        try:
            df = pd.DataFrame(json.loads(value))
            pandas_df_dict[key] = df
        except Exception as e:
            print(f"Error creating DataFrame: {e}")

        # print(key, value)

    pprint(pandas_df_dict)
    return pandas_df_dict


# Function to load pandas data into temporary sql tables


def staging_tables(db_connection, pandas_df_dict):
    # iterate through the keys and values of pandas_df_dict and convert tables into sql

    try:
        for key, value in pandas_df_dict.items():
            # run queries into local database
            value.to_sql(
                f"{key}_staging_table", db_connection, if_exists="replace", index=False
            )
            print(f"Successfully loaded {key} table into local database")
    except Exception as e:
        print(f"Error loading {key} into local database: {e}")
        raise


# Function to extract data from staging tables and load into dim_tables


def create_dim_tables(db_connection):
    # set variable containing list of dim_table creation queries
    dim_table_queries = {
        "dim_date": """CREATE TABLE "dim_date" (
            "date_id" date PRIMARY KEY NOT NULL,
            "year" int NOT NULL,
            "month" int NOT NULL,
            "day" int NOT NULL,
            "day_of_week" int NOT NULL,
            "day_name" varchar NOT NULL,
            "month_name" varchar NOT NULL,
            "quarter" int NOT NULL
            );
            """,
        "dim_staff": """CREATE TABLE "dim_staff" (
            "staff_id" int PRIMARY KEY NOT NULL,
            "first_name" varchar NOT NULL,
            "last_name" varchar NOT NULL,
            "department_name" varchar NOT NULL,
            "location" varchar NOT NULL,
            "email_address" email_address NOT NULL
            );
            """,
        "dim_location": """CREATE TABLE "dim_location" (
            "location_id" int PRIMARY KEY NOT NULL,
            "address_line_1" varchar NOT NULL,
            "address_line_2" varchar,
            "district" varchar,
            "city" varchar NOT NULL,
            "postal_code" varchar NOT NULL,
            "country" varchar NOT NULL,
            "phone" varchar NOT NULL
            );
            """,
        "dim_currency": """CREATE TABLE "dim_currency" (
            "currency_id" int PRIMARY KEY NOT NULL,
            "currency_code" varchar NOT NULL,
            "currency_name" varchar NOT NULL
            );
            """,
        "dim_design": """CREATE TABLE "dim_design" (
            "design_id" int PRIMARY KEY NOT NULL,
            "design_name" varchar NOT NULL,
            "file_location" varchar NOT NULL,
            "file_name" varchar NOT NULL
            );
            """,
        "dim_counterparty": """CREATE TABLE "dim_counterparty" (
            "counterparty_id" int PRIMARY KEY NOT NULL,
            "counterparty_legal_name" varchar NOT NULL,
            "counterparty_legal_address_line_1" varchar NOT NULL,
            "counterparty_legal_address_line_2" varchar,
            "counterparty_legal_district" varchar,
            "counterparty_legal_city" varchar NOT NULL,
            "counterparty_legal_postal_code" varchar NOT NULL,
            "counterparty_legal_country" varchar NOT NULL,
            "counterparty_legal_phone_number" varchar NOT NULL
            );
            """,
    }

    for table_name, query in dim_table_queries.items():
        try:
            with db_connection.connect() as conn:
                conn.execute(text(query))
                print(f"{table_name} successfully created")
        except Exception as e:
            print(f"Error creating {table_name}: {e}")


db_tables = create_pandas_table(db_connection, read_files_from_s3(bucket_name))

staging_tables(db_connection, db_tables)

create_dim_tables(db_connection)


def lambda_handler(event, context):
    print("Data read from bucket")
    try:
        dim_table = create_dim_tables()
        pandas_table = create_pandas_table()
        if "error" in dim_table:
            return {"status": "error", "message": dim_table["error"]}
        if "error" in pandas_table:
            return {"status": "error", "message": pandas_table["error"]}
        return {"status": "success", "data": dim_table and pandas_table}
    except Exception as e:
        return {"status": "error", "message": e}
