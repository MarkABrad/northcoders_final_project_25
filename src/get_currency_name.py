import json

with open("json_data/json-currency.json", "r") as file:
    try:
        data = json.load(file)
    except json.JSONDecodeError:
        print("Error: Failed to load JSON file.")
        data = {"currency": []}


def get_currency_details(currency_id):
    if not isinstance(currency_id, int):
        return "Error: Currency ID must be an integer"

    for currency in data["currency"]:
        if currency_id == currency["currency_id"]:
            currency_code = currency["currency_code"]
            break

    else:
        print(f"currency id {currency_id} is not in our system")
        return f"Currency ID {currency_id} is not in our system"

    currency_dict = {
        "GBP": "British pound sterling",
        "USD": "United States dollar",
        "EUR": "The euro",
    }

    currency_name = currency_dict.get(currency_code, "Unknown currency code")
    return {
        "currency_id": currency_id,
        "currency_code": currency_code,
        "currency_name": currency_name,
    }


def add_currency_names(merged_df, currency_df):
    """
    Adds currency names to the dim_currency DataFrame by mapping currency codes to names.

    Parameters:
    - merged_df: DataFrame containing currency_id and currency_code.
    - currency_df: Raw currency DataFrame containing currency_id and currency_code.

    Returns:
    - DataFrame with an additional currency_name column.
    """

    # Debug: Check initial structure
    print("\nBefore modification in add_currency_names():")
    print("merged_df columns:", merged_df.columns.tolist())
    print("currency_df columns:", currency_df.columns.tolist())

    # Ensure currency_code exists in merged_df before proceeding
    if "currency_code" not in merged_df.columns:
        print("⚠️ Warning: 'currency_code' missing in merged_df! Attempting to fix...")
        if "currency_id" in merged_df.columns and "currency_id" in currency_df.columns:
            merged_df = merged_df.merge(
                currency_df[["currency_id", "currency_code"]],
                on="currency_id",
                how="left",
            )
            print("✅ 'currency_code' successfully added back!")

    # Ensure currency_id exists before merging
    if "currency_id" not in currency_df.columns:
        print("⚠️ Warning: 'currency_id' missing in currency_df! Attempting to fix...")
        currency_df = currency_df.reset_index()
        print("✅ 'currency_id' recovered from index!")

    # Map currency names using a predefined dictionary (or use an external reference)
    currency_dict = {
        "GBP": "British Pound",
        "USD": "US Dollar",
        "EUR": "Euro",
        # Add more as needed...
    }

    # Debug: Check if currency_code exists before applying map()
    if "currency_code" in merged_df.columns:
        merged_df["currency_name"] = (
            merged_df["currency_code"]
            .map(currency_dict)
            .fillna("Unknown currency code")
        )
    else:
        raise KeyError("❌ Error: 'currency_code' still missing after attempted fix!")

    # Debug: Check final structure
    print("\nAfter modification in add_currency_names():")
    print("Final merged_df columns:", merged_df.columns.tolist())
    print(merged_df.head())

    return merged_df
