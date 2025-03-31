import pandas as pd
from src.dim_date_function import extract_date_info_from_dim_date
from pprint import pprint
from src.dimensions_dependents_set_up import (
    create_pandas_raw_tables,
    create_pandas_empty_dim_tables,
)
from src.get_currency_name import add_currency_names
from src.s3_read_from_ingestion_bucket import read_files_from_s3
from utils.get_bucket import get_bucket_name

# This file manipulates the data into dimension table format.


# Populates dim_tables
# Takes input of raw ingestion bucket
def populate_dim_dfs(input_info_df, df_dim_dictionary):
    print(input_info_df["currency"], "<<<<<< This is the raw currency data frame")
    # setup output dictionary
    filled_dim_tables = {}
    # dependent tables each dimension table relies on
    dimensions_dependencies = {
        "dim_date": ["sales_order"],
        "dim_staff": ["staff", "department"],
        "dim_location": ["address"],
        "dim_currency": ["currency"],
        "dim_design": ["design"],
        "dim_counterparty": ["counterparty", "address"],
    }
    # iterate through the keys of the dictionary
    for table_name, dataframe in df_dim_dictionary.items():
        print("Processing TABLE:", table_name)
        if table_name == "dim_date":
            date_columns = [
                "created_at"
            ]  # , "last_updated", "agreed_delivery_date", "agreed_payment_date"
            all_dates = set()
            # print("DATE>>>>",dates[date])
            for col in date_columns:
                all_dates.update(input_info_df["sales_order"][col].dropna().unique())

            dim_date_df = pd.DataFrame(
                [extract_date_info_from_dim_date(date) for date in all_dates]
            )
            filled_dim_tables[table_name] = dim_date_df
        # run currency operations
        elif table_name == "dim_currency":
            dim_currency_df = input_info_df[
                "currency"
            ].reset_index()  # Fix: Reset index
            dim_currency_df = dim_currency_df[
                ["currency_id", "currency_code"]
            ].copy()  # Extract needed columns

            dim_currency_df["currency_name"] = None  # Placeholder if not available

            print("dim_currency_df before adding names:\n", dim_currency_df.head())

            dim_currency_df = add_currency_names(
                dim_currency_df, input_info_df["currency"]
            )

            filled_dim_tables[table_name] = dim_currency_df

        elif table_name == "dim_design":
            dim_design_df = (
                input_info_df["design"]
                .reset_index()[
                    ["design_id", "design_name", "file_location", "file_name"]
                ]
                .copy()
            )
            filled_dim_tables[table_name] = dim_design_df

        # process all the other dim tables
        elif table_name in ["dim_staff", "dim_location"]:
            # iterate through the input data staff information
            dim_table = df_dim_dictionary[table_name]
            no_of_dependencies = len(dimensions_dependencies[table_name])
            first_dependency = dimensions_dependencies[table_name][0]
            raw_info_df = input_info_df[first_dependency]

            if no_of_dependencies > 1:
                for i in range(1, no_of_dependencies):
                    merging_dependency = dimensions_dependencies[table_name][i]
                    left_join_key = input_info_df[merging_dependency].index.name
                    merged_df = raw_info_df.merge(
                        input_info_df[merging_dependency],
                        how="left",
                        left_on=left_join_key,
                        right_index=True,
                    )
                    dim_table = pd.concat(
                        [dim_table, merged_df], ignore_index=False, join="inner"
                    )
            else:
                dim_table = pd.concat(
                    [dim_table, raw_info_df], ignore_index=False, join="inner"
                )

            filled_dim_tables[table_name] = dim_table

        elif table_name == "dim_counterparty":
            # Ensure both counterparty and address exist in input_info_df
            if "counterparty" in input_info_df and "address" in input_info_df:
                counterparty_df = input_info_df["counterparty"].reset_index()
                address_df = input_info_df["address"]

                # print("Counterparty Columns:", counterparty_df.columns)  # Debugging
                # print("Address Columns:", address_df.columns)

                # Merge counterparty with address on legal_address_id
                merged_df = counterparty_df.merge(
                    address_df,
                    left_on="legal_address_id",  # Ensure this column exists in counterparty
                    right_on="address_id",
                    how="left",
                )

                # Rename columns to match dim_counterparty schema
                merged_df = merged_df.rename(
                    columns={
                        "counterparty_id": "counterparty_id",
                        "counterparty_legal_name": "counterparty_legal_name",
                        "address_line_1": "counterparty_legal_address_line_1",
                        "address_line_2": "counterparty_legal_address_line_2",
                        "district": "counterparty_legal_district",
                        "city": "counterparty_legal_city",
                        "postal_code": "counterparty_legal_postal_code",
                        "country": "counterparty_legal_country",
                        "phone": "counterparty_legal_phone_number",
                    }
                )

                # Select only the required columns
                required_columns = [
                    "counterparty_id",
                    "counterparty_legal_name",
                    "counterparty_legal_address_line_1",
                    "counterparty_legal_address_line_2",
                    "counterparty_legal_district",
                    "counterparty_legal_city",
                    "counterparty_legal_postal_code",
                    "counterparty_legal_country",
                    "counterparty_legal_phone_number",
                ]

                merged_df = merged_df[required_columns]  # Explicit column selection

                # Store transformed DataFrame
                filled_dim_tables[table_name] = merged_df

            else:
                raise ValueError(
                    "Missing counterparty or address data in input_info_df"
                )

        else:
            # raise exception if dimension table name not valid
            raise Exception(f"Invalid dimension table name: {table_name}")

    pprint(filled_dim_tables)
    return filled_dim_tables


if __name__ == "__main__":
    bucket_name = get_bucket_name("ingestion-bucket")
    import_files = read_files_from_s3(bucket_name)
    info_df_dict = create_pandas_raw_tables(import_files)
    empty_dim_tables = create_pandas_empty_dim_tables()
    populate_dim_dfs(info_df_dict, empty_dim_tables)
