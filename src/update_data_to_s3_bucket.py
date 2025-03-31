from src.connection import connect_to_db, close_db_connection
from pg8000.native import literal, identifier
from datetime import datetime
import json
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def update_data_to_s3_bucket(
    s3_client,
    bucket_name,
    list_of_tables,
    reformat_data_to_json,
    get_file_contents_of_last_uploaded,
):

    db = connect_to_db()

    try:
        list_of_table_data_uploaded = []

        for table in list_of_tables():
            # get the file contents of the txt file that has the last json file uploaded for that table
            last_data_uploaded = get_file_contents_of_last_uploaded(
                s3_client, bucket_name, table
            )
            # set date to before the database was created
            last_updated_date = datetime.strptime(
                "1900-11-03T14:20:52.186000", "%Y-%m-%dT%H:%M:%S.%f"
            )
            # get the data from the db after the last_updated date in the last json file uploaded
            for row in last_data_uploaded:
                data_from_s3 = datetime.strptime(
                    row["last_updated"], "%Y-%m-%dT%H:%M:%S.%f"
                )
                if data_from_s3 > last_updated_date:
                    last_updated_date = data_from_s3
            db_run_column = db.run(
                f""" SELECT * FROM {identifier(table)}
                WHERE last_updated > {literal(last_updated_date)};"""
            )
            columns = [col["name"] for col in db.columns]
            additional_data_from_op_db = reformat_data_to_json(db_run_column, columns)
            # for the additional data, go through and create new json file for each last_updated, and when last one is updated,
            # create a new txt file to overright the current one with the json file with the latest data
            if additional_data_from_op_db:
                data_to_upload = [additional_data_from_op_db[0]]
                if len(additional_data_from_op_db) == 1:
                    date_updated = additional_data_from_op_db[0]["last_updated"]
                    year = date_updated.split("-")[0]
                    month = date_updated.split("-")[1]
                    day = date_updated.split("-")[2].split("T")[0]
                    time = date_updated.split("-")[2].split("T")[1]
                    object_key = f"{table}/{year}/{month}/{day}/{time}.json"
                    s3_client.put_object(
                        Bucket=bucket_name,
                        Key=object_key,
                        Body=json.dumps(data_to_upload),
                    )
                    s3_client.put_object(
                        Bucket=bucket_name,
                        Key=f"last_updated/{table}.txt",
                        Body=object_key,
                    )
                for i in range(1, len(additional_data_from_op_db)):
                    if i == len(additional_data_from_op_db) - 1:
                        data_to_upload.append(additional_data_from_op_db[i])
                        date_updated = additional_data_from_op_db[i]["last_updated"]
                        year = date_updated.split("-")[0]
                        month = date_updated.split("-")[1]
                        day = date_updated.split("-")[2].split("T")[0]
                        time = date_updated.split("-")[2].split("T")[1]
                        object_key = f"{table}/{year}/{month}/{day}/{time}.json"
                        s3_client.put_object(
                            Bucket=bucket_name,
                            Key=object_key,
                            Body=json.dumps(data_to_upload),
                        )
                        s3_client.put_object(
                            Bucket=bucket_name,
                            Key=f"last_updated/{table}.txt",
                            Body=object_key,
                        )
                    elif additional_data_from_op_db[i]["last_updated"] == (
                        additional_data_from_op_db[i - 1]["last_updated"]
                    ):
                        data_to_upload.append(additional_data_from_op_db[i])
                    elif additional_data_from_op_db[i]["last_updated"] != (
                        additional_data_from_op_db[i - 1]["last_updated"]
                    ):
                        date_updated = additional_data_from_op_db[i - 1]["last_updated"]
                        year = date_updated.split("-")[0]
                        month = date_updated.split("-")[1]
                        day = date_updated.split("-")[2].split("T")[0]
                        time = date_updated.split("-")[2].split("T")[1]
                        object_key = f"{table}/{year}/{month}/{day}/{time}.json"
                        s3_client.put_object(
                            Bucket=bucket_name,
                            Key=object_key,
                            Body=json.dumps(data_to_upload),
                        )
                        data_to_upload = [additional_data_from_op_db[i]]
                list_of_table_data_uploaded.append(table)

        if additional_data_from_op_db:
            logger.info("Ingestion bucket has been updated")
            return {
                "message": f"""data has been added to {bucket_name},
            in files {list_of_table_data_uploaded}"""
            }
    except TypeError as error:
        logger.error("Not passed contents of last updated!: {}".format(error))

    finally:
        close_db_connection(db)
