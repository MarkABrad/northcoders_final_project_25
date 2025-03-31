import pandas as pd
import json

# This file contains code using pandas to create dataframes from the raw ingestion bucket.


# NOTICE
# This function is only used to allow local testing
# without the need for the s3 get function
# For actual implementation, use s3_get function
def load_local_files_for_testing():
    output_dict = {}
    file_list = [
        "address",
        "counterparty",
        "currency",
        "department",
        "design",
        "payment",
        "sales_order",
        "staff",
    ]
    for file in file_list:
        with open(f"json_data/json-{file}.json", "r") as f:
            import_dict = json.load(f)[file]
        output_dict[file] = import_dict
    return output_dict


# Create tables using the raw data from the ingestion bucket
# Returns a dictionary with keys of <table_name>
# Note for testing this currently retrieves information from local files.
def create_pandas_raw_tables(import_dict):
    info_df_dict = {}

    for file in import_dict:
        info_df = pd.DataFrame(import_dict[file])
        info_df.set_index(list(info_df)[0], inplace=True)
        info_df_dict[file] = info_df

    return info_df_dict


# Creates empty dim tables with data types set.
# Returns a dictionary with keys of dim_<table_name>
def create_pandas_empty_dim_tables():
    dim_date_df = pd.DataFrame(
        {
            "date_id": pd.Series(dtype="datetime64[ns]"),
            "year": pd.Series(dtype="int"),
            "month": pd.Series(dtype="int"),
            "day": pd.Series(dtype="int"),
            "day_of_week": pd.Series(dtype="int"),
            "day_name": pd.Series(dtype="str"),
            "month_name": pd.Series(dtype="str"),
            "quarter": pd.Series(dtype="int"),
        }
    )
    # The index column is set to the ID value
    dim_date_df.set_index("date_id", inplace=True)

    dim_staff_df = pd.DataFrame(
        {
            "staff_id": pd.Series(dtype="int"),
            "first_name": pd.Series(dtype="str"),
            "last_name": pd.Series(dtype="str"),
            "department_name": pd.Series(dtype="str"),
            "location": pd.Series(dtype="str"),
            "email_address": pd.Series(dtype="str"),
        }
    )
    dim_staff_df.set_index("staff_id", inplace=True)

    dim_location_df = pd.DataFrame(
        {
            "location_id": pd.Series(dtype="int"),
            "address_line_1": pd.Series(dtype="str"),
            "address_line_2": pd.Series(dtype="str"),
            "district": pd.Series(dtype="str"),
            "city": pd.Series(dtype="str"),
            "postal_code": pd.Series(dtype="str"),
            "country": pd.Series(dtype="str"),
            "phone": pd.Series(dtype="str"),
        }
    )
    dim_location_df.set_index("location_id", inplace=True)

    dim_currency_df = pd.DataFrame(
        {
            "currency_id": pd.Series(dtype="int"),
            "currency_code": pd.Series(dtype="str"),
            "currency_name": pd.Series(dtype="str"),
        }
    )
    dim_currency_df.set_index("currency_id", inplace=True)

    dim_design_df = pd.DataFrame(
        {
            "design_id": pd.Series(dtype="int"),
            "design_name": pd.Series(dtype="str"),
            "file_location": pd.Series(dtype="str"),
            "file_name": pd.Series(dtype="str"),
        }
    )
    dim_design_df.set_index("design_id", inplace=True)

    dim_counterparty_df = pd.DataFrame(
        {
            "counterparty_id": pd.Series(dtype="int"),
            "counterparty_legal_name": pd.Series(dtype="str"),
            "counterparty_legal_address_line_1": pd.Series(dtype="str"),
            "counterparty_legal_address_line_2": pd.Series(dtype="str"),
            "counterparty_legal_district": pd.Series(dtype="str"),
            "counterparty_legal_city": pd.Series(dtype="str"),
            "counterparty_legal_postal_code": pd.Series(dtype="str"),
            "counterparty_legal_country": pd.Series(dtype="str"),
            "counterparty_legal_phone_number": pd.Series(dtype="str"),
        }
    )
    dim_counterparty_df.set_index("counterparty_id", inplace=True)

    df_dim_dictionary = {
        "dim_date": dim_date_df,
        "dim_staff": dim_staff_df,
        "dim_location": dim_location_df,
        "dim_currency": dim_currency_df,
        "dim_design": dim_design_df,
        "dim_counterparty": dim_counterparty_df,
    }

    print(df_dim_dictionary)
    # return dictionary of dataframes
    return df_dim_dictionary


if __name__ == "__main__":
    import_files = load_local_files_for_testing()
    info_df_dict = create_pandas_raw_tables(import_files)
    empty_dim_tables = create_pandas_empty_dim_tables()
