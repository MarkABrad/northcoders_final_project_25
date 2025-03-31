import boto3
import io
import os
from datetime import datetime
from utils.get_bucket import get_bucket_name
from src.dimensions_data_manipulation import populate_dim_dfs
from src.dimensions_dependents_set_up import (
    create_pandas_raw_tables,
    create_pandas_empty_dim_tables,
)
from src.s3_read_from_ingestion_bucket import read_files_from_s3
from src.pandas_fact_sales_order import transform_fact_data

# bucket_name = get_bucket_name("processed-bucket")

S3_BUCKET = os.getenv("S3_BUCKET", "processed-bucket20250312152305747100000002")

"""

This function should take the ouput of the transform data function (list of dictionaries) and write each piece of data to the s3 processed bucket

The files should be saved with a name that makes them easily comparable and traceable respective to themselves when they were in the s3 ingestion bucket

The files should be converted and saved in the s3 processed bucket in parquet format

"""


def write_schema_to_processed(data: dict):

    s3_client = boto3.client("s3")

    bucket_name = os.getenv("S3_BUCKET", S3_BUCKET)

    if not bucket_name:
        raise ValueError(f"{S3_BUCKET} environment variable is not set")

    # Set a variable key which dynamically changes the name of each of the pieces of processed data based on one of their unique elements

    try:
        # Convert the files into parquet format
        for table_name, dataframe in data.items():
            source_file = (
                data["source_file"].iloc[0]
                if "source_file" in dataframe.columns
                else "unknown_source.json"
            )

            timestamp = datetime.utcnow().strftime("%Y-%m-%d_%H-%M-%S")

            key = f"{table_name}/{source_file.replace('.json', '')}_{timestamp}.parquet"

            buffer = io.BytesIO()
            dataframe.to_parquet(buffer, engine="pyarrow", index=True)
            buffer.seek(0)

            # Write each file into the S3 processed bucket
            s3_client.put_object(
                Bucket=bucket_name,
                Key=key,
                Body=buffer.getvalue(),
                ContentType="application/octet-stream",
            )
            print(
                f"Data successfully written to s3 in parquet format: s3://{bucket_name}/{key}"
            )
    except Exception as e:
        print(f"Error writing to s3: {e}")
        raise


# Iterate over the list of dictionaries and dump each individual element into a json object

if __name__ == "__main__":
    bucket_name = get_bucket_name("ingestion-bucket")
    import_files = read_files_from_s3(bucket_name)
    info_df_dict = create_pandas_raw_tables(import_files)
    empty_dim_tables = create_pandas_empty_dim_tables()
    dim_dataframes = populate_dim_dfs(info_df_dict, empty_dim_tables)
    dim_and_fact_sales = transform_fact_data(import_files, dim_dataframes)
    write_schema_to_processed(dim_and_fact_sales)
