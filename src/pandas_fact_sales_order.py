import pandas as pd
from utils.get_bucket import get_bucket_name
import logging
from pprint import pprint
from src.dimensions_dependents_set_up import (
    create_pandas_raw_tables,
    create_pandas_empty_dim_tables,
)
from src.dimensions_data_manipulation import populate_dim_dfs
from src.s3_read_from_ingestion_bucket import read_files_from_s3

"""
This function is a new alternative for the fact_sales_order data. We originally tried to do this using sql and local postgres databases but realised this wouldn't work in Lambda

This approach uses purely Pandas to achieve the necessary transformations

This function relies on Dataframes having already been created as there is no longer a database to reference this from
"""


def transform_fact_data(raw_data, dim_tables_dict):
    """
    Transforms raw sales_order data into the fact_sales_order schema.

    Args:
        raw_data (dict): Dictionary containing raw sales_order data.
        dim_tables_dict (dict): Dictionary containing dimension tables as Pandas DataFrames.

    Returns:
        pd.DataFrame: Transformed fact_sales_order data.
    """
    try:
        # Ensure sales_order exists in raw data
        if "sales_order" not in raw_data or not raw_data["sales_order"]:
            logging.warning("No sales_order data found in raw data")
            return pd.DataFrame()

        df = pd.DataFrame(raw_data["sales_order"])

        # Convert date-time columns
        df["created_date"] = pd.to_datetime(df["created_at"]).dt.date
        df["created_time"] = pd.to_datetime(df["created_at"]).dt.time
        print(df, "<<<<<DFFFFF")
        df["last_updated_date"] = pd.to_datetime(df["last_updated"]).dt.date
        df["last_updated_time"] = pd.to_datetime(df["last_updated"]).dt.time

        # Extract dimension tables from the dictionary
        dim_date = dim_tables_dict.get("dim_date", pd.DataFrame())
        dim_staff = dim_tables_dict.get("dim_staff", pd.DataFrame())
        dim_counterparty = dim_tables_dict.get("dim_counterparty", pd.DataFrame())
        dim_currency = dim_tables_dict.get("dim_currency", pd.DataFrame())
        dim_design = dim_tables_dict.get("dim_design", pd.DataFrame())
        dim_location = dim_tables_dict.get("dim_location", pd.DataFrame())

        # Ensure dim_date has correct format
        if not dim_date.empty and "date_id" in dim_date.columns:
            dim_date["date_id"] = pd.to_datetime(dim_date["date_id"]).dt.date

        # Merge sales_order with dimension tables
        df = df.merge(
            dim_date,
            left_on="created_date",
            right_on="date_id",
            how="left",
            suffixes=("", "_created"),
        )
        df = df.merge(
            dim_date,
            left_on="last_updated_date",
            right_on="date_id",
            how="left",
            suffixes=("", "_updated"),
        )
        df = df.merge(
            dim_date,
            left_on="agreed_payment_date",
            right_on="date_id",
            how="left",
            suffixes=("", "_payment"),
        )
        df = df.merge(
            dim_date,
            left_on="agreed_delivery_date",
            right_on="date_id",
            how="left",
            suffixes=("", "_delivery"),
        )
        # Merge with other dimension tables
        df = df.merge(dim_staff, left_on="staff_id", right_index=True, how="left")
        df = df.merge(
            dim_counterparty,
            left_on="counterparty_id",
            right_on="counterparty_id",
            how="left",
        )
        df = df.merge(
            dim_currency, left_on="currency_id", right_on="currency_id", how="left"
        )
        df = df.merge(dim_design, left_on="design_id", right_on="design_id", how="left")
        df = df.merge(
            dim_location,
            left_on="agreed_delivery_location_id",
            right_index=True,
            how="left",
        )
        # Select and rename final columns
        df = df[
            [
                "sales_order_id",
                "created_date",
                "created_time",
                "last_updated_date",
                "last_updated_time",
                "staff_id",
                "counterparty_id",
                "units_sold",
                "unit_price",
                "currency_id",
                "design_id",
                "agreed_payment_date",
                "agreed_delivery_date",
                "agreed_delivery_location_id",
            ]
        ]
        df.set_index("sales_order_id", inplace=True)

        # # Optimize memory usage
        # df["sales_order_id"] = df["sales_order_id"].astype("int32")
        # df["staff_id"] = df["staff_id"].astype("int16")
        # df["counterparty_id"] = df["counterparty_id"].astype("int16")
        # df["units_sold"] = df["units_sold"].astype("int32")
        # df["unit_price"] = df["unit_price"].astype("float32")
        # df["currency_id"] = df["currency_id"].astype("int8")
        # df["design_id"] = df["design_id"].astype("int16")
        # df["agreed_delivery_location_id"] = df["agreed_delivery_location_id"].astype("int16")

        fact_sales_order_df = df.drop_duplicates()
        logging.info(f"Transformed sales_order data shape: {fact_sales_order_df.shape}")
        pprint(fact_sales_order_df)

        dim_tables_dict["fact_sales_order"] = fact_sales_order_df

        print(dim_tables_dict)
        return dim_tables_dict

    except Exception as e:
        logging.error(f"Error during sales_order transformation: {e}")
        raise


if __name__ == "__main__":
    bucket_name = get_bucket_name("ingestion-bucket")
    import_files = read_files_from_s3(bucket_name)
    info_df_dict = create_pandas_raw_tables(import_files)
    empty_dim_tables = create_pandas_empty_dim_tables()
    dim_dataframes = populate_dim_dfs(info_df_dict, empty_dim_tables)
    transform_fact_data(import_files, dim_dataframes)
