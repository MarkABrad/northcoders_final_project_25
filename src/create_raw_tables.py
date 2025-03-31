# BEFORE RUNNING, SET UP A LOCAL POSTGRESQL DATABASE
from pg8000.native import identifier, literal


# This function will check the input is in the correct format
# Dictionary containing list of dictionaries (multiple allowed)
def check_formatting_of_input(input_data):
    # check input is in correct format
    if isinstance(input_data, dict):
        for item in input_data:
            print(input_data[item], "<<<<<item")
            if isinstance(input_data[item], list):
                for inner_dict in input_data[item]:
                    if isinstance(inner_dict, dict):
                        return True
                    else:
                        raise TypeError(
                            f"Input is wrong type. Must be a dictionary, containing a list of dictionaries. {inner_dict} is {type(inner_dict)}"
                        )
            else:
                raise TypeError(
                    f"Input is wrong type. Must be a dictionary, containing a list of dictionaries. {item} is {type(item)}"
                )
    else:
        raise TypeError(
            f"Input is wrong type. Must be a dictionary, containing a list of dictionaries. {input_data} is {type(input_data)}"
        )


# This function will import incoming information into temporary tables
# Sourcing table headers and table name from input
# It will return a list of names of tables created
# and a list of lists containing column headers for each table in order
def make_raw_tables(database_connection, data):
    # create each table
    column_headers_list = []
    table_names = [*data]
    print(table_names, "<<<<TABLE_NAMES")
    if not table_names:
        raise Exception("No tables created")
    for table in table_names:
        print("TABLE>>>>>", table)
        # get column names
        if data[table]:
            # print(data[table],"<<<<DATA[TABLE]")
            # print(type(data[table]),"<<<<DATA[TABLE] type")
            column_names = list(data[table][0].keys())
            # add datatypes to column names
            column_names_with_types = []
            for name in column_names:
                if name.find("_id") >= 0:
                    column_names_with_types.append(name + " int")
                else:
                    column_names_with_types.append(name + " text")
            headers_string = str(tuple(column_names_with_types)).replace("'", "")
            database_connection.run(
                f"""
                CREATE TABLE IF NOT EXISTS {table} {headers_string};
                """
            )
            print(f"TABLE CREATED {table} with columns {headers_string}")
            # run a query to get headers
            database_connection.run(f"SELECT * FROM {table}")
            # list column headers
            column_headers = [c["name"] for c in database_connection.columns]
            column_headers_list.append(column_headers)
        else:
            print(f"No table created for {table}")
            # populate table with raw data
        for row in data[table]:
            row_id_header = list(row.keys())[0]
            database_connection.run(
                f"""
            INSERT INTO {table} ({row_id_header})
            VALUES (:row_id);
            """,
                row_id=row[row_id_header],
            )
            for key in row:
                if key != row_id_header:
                    database_connection.run(
                        f"""
                    UPDATE {identifier(table)}
                    SET {identifier(key)} = {literal(row[key])}
                    WHERE {identifier(row_id_header)} = {literal(row[row_id_header])}
                    """
                    )

    # return a list of column header lists
    return table_names, column_headers_list
