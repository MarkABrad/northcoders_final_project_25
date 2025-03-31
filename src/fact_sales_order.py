import pandas as pd
from pprint import pprint
from s3_read_function.s3_read_function import read_files_from_s3
from utils.get_bucket import get_bucket_name
import os
from dotenv import load_dotenv
import logging
from sqlalchemy import create_engine
import json
from sqlalchemy import text

# from src.create_temporary_tables import make_temporary_tables
# from src.create_dim_tables import *

"""""
This function reads from the S3 ingress bucket and converts the raw data tables into the fact_sales_order star schema

This function will run parallel to Joe's dim table conversion function

The schema should appear as follows:

fact_sales_order
dim_staff
dim_location
dim_design
dim_date
dim_currency
dim_counterparty

""" ""

# def connect_to_local_db():
#     return pg8000.connect(
#         user="local_user",
#         password="password",
#         host="localhost",
#         port=5432,
#         database="local_schema"
#     )

load_dotenv()


def connect_to_db():
    return create_engine(
        f"postgresql+psycopg2://{os.getenv('PG_USER')}:{os.getenv('PG_PASSWORD')}@{os.getenv('PG_HOST')}:{os.getenv('PG_PORT')}/{os.getenv('PG_DATABASE')}"
    )


# def connect_to_db():
#     return pg8000.connect(
#         user=os.getenv("PG_USER"),
#         password=os.getenv("PG_PASSWORD"),
#         database=os.getenv("PG_DATABASE"),
#         host=os.getenv("PG_HOST"),
#         port=int(os.getenv("PG_PORT")),
#     )

engine = connect_to_db()
if engine:
    print("Connected successfully")


def create_fact_sales_order_table(engine):
    """
    Creates the fact_sales_order table
    """
    create_table_query = text(
        """
    CREATE TABLE IF NOT EXISTS "fact_sales_order" (
        "sales_record_id" SERIAL PRIMARY KEY,
        "sales_order_id" int NOT NULL,
        "created_date" date NOT NULL,
        "created_time" time NOT NULL,
        "last_updated_date" date NOT NULL,
        "last_updated_time" time NOT NULL,
        "sales_staff_id" int NOT NULL,
        "counterparty_id" int NOT NULL,
        "units_sold" int NOT NULL,
        "unit_price" numeric(10, 2) NOT NULL,
        "currency_id" int NOT NULL,
        "design_id" int NOT NULL,
        "agreed_payment_date" date NOT NULL,
        "agreed_delivery_date" date NOT NULL,
        "agreed_delivery_location_id" int NOT NULL
        );

    ALTER TABLE "fact_sales_order" ADD FOREIGN KEY ("created_date") REFERENCES "dim_date" ("date_id");

    ALTER TABLE "fact_sales_order" ADD FOREIGN KEY ("last_updated_date") REFERENCES "dim_date" ("date_id");

    ALTER TABLE "fact_sales_order" ADD FOREIGN KEY ("sales_staff_id") REFERENCES "dim_staff" ("staff_id");

    ALTER TABLE "fact_sales_order" ADD FOREIGN KEY ("counterparty_id") REFERENCES "dim_counterparty" ("counterparty_id");

    ALTER TABLE "fact_sales_order" ADD FOREIGN KEY ("currency_id") REFERENCES "dim_currency" ("currency_id");

    ALTER TABLE "fact_sales_order" ADD FOREIGN KEY ("design_id") REFERENCES "dim_design" ("design_id");

    ALTER TABLE "fact_sales_order" ADD FOREIGN KEY ("agreed_payment_date") REFERENCES "dim_date" ("date_id");

    ALTER TABLE "fact_sales_order" ADD FOREIGN KEY ("agreed_delivery_date") REFERENCES "dim_date" ("date_id");

    ALTER TABLE "fact_sales_order" ADD FOREIGN KEY ("agreed_delivery_location_id") REFERENCES "dim_location" ("location_id");
    """
    )
    try:

        with engine.connect() as conn:
            conn.execute(create_table_query)
            conn.commit()
        logging.info("`fact_sales_order` table created successfully.")
        # database_connection.run(create_table_query)
        # cursor = database_connection.cursor()

        # # Execute the query
        # cursor.execute(create_table_query)

        # # Commit the transaction
        # database_connection.commit()
        # print("`fact_sales_order` created successfully.")
    except Exception as e:
        logging.error(f"Error creating table: {e}")
        # test 3 failed as I was only printing the exception rather than raising it - changed to raise
        raise


# db_url = "postgresql+psycopg2://benmorgan:password@localhost:5432/local_schema"
# engine = create_engine(db_url)
# 0. AWS configuration variables set to the s3 ingestion bucket and variables for db_host, db_name, db_user,db_password
# 1. read the raw data from the ingestion bucket. Possibly a bot3 client.get
def transform_fact_data(engine, raw_data):
    print(raw_data, "<<<<<< This is the raw data")
    # use pandas to convert raw json data to staging dataframe
    for file_key, data_list in raw_data.items():
        print(file_key)

    # variable that can be returned ready to be passed into s3 write L2 util function
    transformed_data = []
    # 2. query string variable that contains the sql necessary for schema conversion - possible separate function to loading using pandas
    try:

        logging.info(f"raw_data: {raw_data}")

        # pprint(f"raw_data: {raw_data}")
        # print(type(raw_data))

        for file_key, data_list in raw_data.items():
            # if file_key == "sales_order":
            print(
                file_key,
                data_list,
                ">>>>> These are the files and keys for sales orders",
            )
            print(type(data_list))

            if not data_list:
                logging.error(f"Skipping file {file_key}: Empty data list received.")
                continue

            # Check if data_list is a valid JSON string
            # if file_key == "sales_order":
            #     print(data_list)
            try:
                # Try loading the data if it's a valid JSON string
                df = pd.DataFrame(json.loads(data_list))
                pprint(df)
            except json.JSONDecodeError:
                logging.error(f"Skipping file {file_key}: Invalid JSON format.")
                continue

            # if not isinstance(data_list, list) or not all(isinstance(item, dict) for item in data_list):
            #     raise ValueError(f"Data for {file_key} is not in the expected list of dictionaries format.")

            # print(f"Data List: {data_list[:5]}")

            source_file = file_key  # raw_data[0].get("source_file", "unknown_source.json") if raw_data else "unknown_source.json"

            # # pprint(json.loads(data_list))

            # df = pd.DataFrame(json.loads(data_list))
            # pprint(df)
            logging.info(f"Processing file: {file_key}")
            df["source_file"] = source_file

            # expected_columns = [
            #     "sales_order_id",
            #     "created_date",
            #     "created_time",
            #     "last_updated_date",
            #     "last_updated_time",
            #     "sales_staff_id",
            #     "counterparty_id",
            #     "units_sold",
            #     "unit_price",
            #     "currency_id",
            #     "design_id",
            #     "agreed_payment_date",
            #     "agreed_delivery_date",
            #     "agreed_delivery_location_id",
            # ]

            # if not set(expected_columns).issubset(df.columns):
            #     missing_cols = list(set(expected_columns) - set(df.columns))
            #     logging.warning(f"Skipping file {file_key}: Missing columns {missing_cols}")
            #     continue

            # Load data into staging table
            # with engine.connect() as conn:
            df.to_sql(
                "staging_fact_sales_order", engine, if_exists="append", index=False
            )
            logging.info(f"Loaded {len(df)} records into staging_fact_sales_order.")

            # if not df.empty:
            #     df.to_sql("staging_fact_sales_order", engine, if_exists="replace", index=False)
            #     logging.info("Raw JSON data loaded into `staging_fact_sales_order`.")
            # else:
            #     logging.warning("No data to load into `staging_fact_sales_order`.")

            # 3. The query string should join the fact table to the ids of each respective dim tables
            transformation_query = text(
                """
            INSERT INTO fact_sales_order (
                sales_order_id, 
                created_date, 
                created_time, 
                last_updated_date, 
                last_updated_time, 
                sales_staff_id, 
                counterparty_id,
                units_sold,
                unit_price,
                currency_id,
                design_id,
                agreed_payment_date,
                agreed_delivery_date,
                agreed_delivery_location_id
            )
            SELECT
                s.sales_order_id,
                d1.date_id AS created_date,
                TO_CHAR(s.created_at::TIMESTAMP, 'HH24:MI:SS')::TIME AS created_time,
                d2.date_id AS last_updated_date,
                TO_CHAR(s.last_updated::TIMESTAMP, 'HH24:MI:SS')::TIME AS last_updated_time,
                staff.staff_id,
                cp.counterparty_id,
                s.units_sold,
                s.unit_price,
                curr.currency_id,
                des.design_id,
                d3.date_id AS agreed_payment_date,
                d4.date_id AS agreed_delivery_date,
                loc.location_id
            FROM staging_fact_sales_order s
            LEFT JOIN dim_date d1 ON DATE(s.created_at) = d1.date_id
            LEFT JOIN dim_date d2 ON DATE(s.last_updated) = d2.date_id
            LEFT JOIN dim_date d3 ON DATE(s.agreed_payment_date) = d3.date_id
            LEFT JOIN dim_date d4 ON DATE(s.agreed_delivery_date) = d4.date_id
            LEFT JOIN dim_staff staff ON s.staff_id = staff.staff_id
            LEFT JOIN dim_counterparty cp ON s.counterparty_id = cp.counterparty_id
            LEFT JOIN dim_currency curr ON s.currency_id = curr.currency_id
            LEFT JOIN dim_design des ON s.design_id = des.design_id
            LEFT JOIN dim_location loc ON s.agreed_delivery_location_id = loc.location_id
            WHERE s.sales_order_id IS NOT NULL
            AND d1.date_id IS NOT NULL
            AND d4.date_id IS NOT NULL
            RETURNING *;

            """
            )
            # 4. The fact sales order table data should be returned ready to be passed into lambda 3 that loads into the processed bucket
            with engine.connect() as conn:
                result = conn.execute(transformation_query)
                print(result, "<<<<< This is the result")
                fetched_data = result.mappings().all()  # Converts result to a list
                print(fetched_data)
                logging.info(f"Transformed rows count: {len(fetched_data)}")

                if fetched_data:
                    transformed_data.extend([dict(row) for row in fetched_data])
                    conn.commit()
                else:
                    logging.warning(
                        "No rows were returned from the transformation query."
                    )

    except Exception as e:
        logging.error(f"Error during transformation: {e}")
        raise
    finally:
        logging.info("Transformation process completed.")

    print(transformed_data)
    transformed_df = pd.DataFrame(transformed_data)
    print("<<<< This is the final Dataframe")
    pprint(transformed_df)
    return transformed_df


if __name__ == "__main__":
    # temp_output = make_temporary_tables(engine, read_files_from_s3(get_bucket_name("ingestion-bucket")))

    # Step 2: Populate the dimension tables (dim_date, dim_staff, etc.)
    # put_info_into_dims_schema(engine, set_up_dims_table(engine, temp_output))

    # Step 3: Ensure fact_sales_order table exists and is ready for data insertion
    create_fact_sales_order_table(engine)

    # Step 4: Transform and load data into the fact_sales_order table
    transform_fact_data(engine, read_files_from_s3(get_bucket_name("ingestion-bucket")))
    # close_db_connection(data_conn)
