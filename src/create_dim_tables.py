from src.dim_date_function import extract_date_info_from_dim_date
from src.get_currency_name import get_currency_details
from pg8000.native import identifier


# Create dim tables if they don't exist
# populate dim tables with new values
# return new values inserted
def make_dim_tables(database_connection, table_names):
    dimension_value_rows = {}
    # create list of tables to be created from
    dim_tables_created = []
    # check dependent tables exist for each dim table
    for dim_table in dimensions_tables_creation:
        if all(
            dependent in table_names
            for dependent in list(dimensions_tables_creation[dim_table][1:])
        ):
            dim_tables_created.append(dim_table)
    if not dim_tables_created:
        raise Exception(
            "Not enough data to create any dim tables, please ensure adequate data has been provided"
        )
    # create dim tables that can be created
    print("DIM_TABLES_TO_BE_CREATED>>>>>", dim_tables_created)
    for table in dim_tables_created:
        print("TABLE>>>>>", table)
        database_connection.run(
            f"CREATE TABLE IF NOT EXISTS {identifier(table)} ({dimensions_tables_creation[table][0]});"
        )
        database_connection.run(f"SELECT * from {identifier(table)}")
        dim_column_headers = [c["name"] for c in database_connection.columns]
        if table == "dim_date":
            dates = database_connection.run(
                f"""
              SELECT {dimensions_insertion_queries[table]};
              """
            )
            for date_row in dates:
                for date in date_row:
                    date_values = extract_date_info_from_dim_date(date)
                    date_id = date_values["date_id"]
                    query = f"""INSERT INTO dim_date
                              (date_id,year,month,day,day_of_week,day_name,month_name,quarter)
                              VALUES (:date_id, {str(tuple(date_values.values())[1:]).replace("(", "")}
                              ON CONFLICT (date_id) DO NOTHING
                              RETURNING *;
                          """
                    date_result = database_connection.run(query, date_id=date_id)
                    print(date_result, "<<<<<date_result")
        elif table == "dim_currency":
            currencies = database_connection.run(
                f"""
              SELECT {dimensions_insertion_queries[table]};
              """
            )
            print(currencies, "<<<<<currencies")
            for currency in currencies:
                print(currency, "<<<<<currency")
                currency_values = get_currency_details(currency[0])
                currency_id = currency_values["currency_id"]
                print(currency_values, "currency_value")
                query = f"""INSERT INTO dim_currency
                              (currency_id,currency_code,currency_name)
                              VALUES (:currency_id, {str(tuple(currency_values.values())[1:]).replace("(", "")}
                              ON CONFLICT (currency_id) DO NOTHING
                              RETURNING *;
                          """
                currency_result = database_connection.run(
                    query, currency_id=currency_id
                )
                print(currency_result, "<<<<<currency_result")

        elif table not in [
            "dim_staff",
            "dim_location",
            "dim_design",
            "dim_counterparty",
        ]:
            raise Exception("Dimension table names requested are not valid")
        else:
            # Ensure the table is inserted correctly into the database
            table_id = dim_column_headers[0]
            dimension_value_rows[table] = database_connection.run(
                f"""
              INSERT INTO {identifier(table)}
              SELECT {dimensions_insertion_queries[table]}
              ON CONFLICT ({identifier(table_id)}) DO NOTHING
              RETURNING *;
              """
            )

    # Raise error if no dimension rows were inserted
    # if not dimension_value_rows:
    #         raise Exception("No rows outputted from any dimension table")
    print(dimension_value_rows)
    return dimension_value_rows


# This is a dictionary of lists
# key = dimensions table names
# value[0] = headers
# value[1:] = dependencies
# for use in python, but written in SQL.


dimensions_tables_creation = {
    "dim_date": [
        """
  "date_id" date PRIMARY KEY NOT NULL,
  "year" int NOT NULL,
  "month" int NOT NULL,
  "day" int NOT NULL,
  "day_of_week" int NOT NULL,
  "day_name" varchar NOT NULL,
  "month_name" varchar NOT NULL,
  "quarter" int NOT NULL
""",
        "sales_order",
    ],
    "dim_staff": [
        """
  "staff_id" int PRIMARY KEY NOT NULL,
  "first_name" varchar NOT NULL,
  "last_name" varchar NOT NULL,
  "department_name" varchar NOT NULL,
  "location" varchar NOT NULL,
  "email_address" varchar NOT NULL
""",
        "staff",
        "department",
    ],
    "dim_location": [
        """
  "location_id" int PRIMARY KEY NOT NULL,
  "address_line_1" varchar NOT NULL,
  "address_line_2" varchar,
  "district" varchar,
  "city" varchar NOT NULL,
  "postal_code" varchar NOT NULL,
  "country" varchar NOT NULL,
  "phone" varchar NOT NULL
""",
        "address",
    ],
    "dim_currency": [
        """
  "currency_id" int PRIMARY KEY NOT NULL,
  "currency_code" varchar NOT NULL,
  "currency_name" varchar NOT NULL
""",
        "currency",
    ],
    "dim_design": [
        """
  "design_id" int PRIMARY KEY NOT NULL,
  "design_name" varchar NOT NULL,
  "file_location" varchar NOT NULL,
  "file_name" varchar NOT NULL
""",
        "design",
    ],
    "dim_counterparty": [
        """
  "counterparty_id" int PRIMARY KEY NOT NULL,
  "counterparty_legal_name" varchar NOT NULL,
  "counterparty_legal_address_line_1" varchar NOT NULL,
  "counterparty_legal_address_line_2" varchar,
  "counterparty_legal_district" varchar,
  "counterparty_legal_city" varchar NOT NULL,
  "counterparty_legal_postal_code" varchar NOT NULL,
  "counterparty_legal_country" varchar NOT NULL,
  "counterparty_legal_phone_number" varchar NOT NULL
""",
        "counterparty",
        "address",
    ],
}

# This is a dictionary of lists
# key = dimensions table names
# value = SQL SELECT query
# for use in python, but written in SQL.
dimensions_insertion_queries = {
    "dim_date": """
created_at,last_updated,agreed_delivery_date,agreed_payment_date
FROM sales_order
""",
    "dim_staff": """
staff_id,first_name,last_name, department.department_name,department.location,email_address
FROM staff
JOIN department ON staff.department_id = department.department_id
""",
    "dim_location": """
address_id,address_line_1, address_line_2,district,city,postal_code,country,phone
FROM address
""",
    "dim_currency": """
currency_id, currency_code
FROM currency
""",
    "dim_design": """
design_id,design_name,file_location,file_name
FROM design
""",
    "dim_counterparty": """

counterparty_id,
counterparty_legal_name,
address.address_line_1,
address.address_line_2,
address.district,
address.city,
address.postal_code,
address.country,
address.phone
FROM counterparty
JOIN address
ON counterparty.legal_address_id = address.address_id
""",
}
