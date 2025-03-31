from pg8000.native import Connection
from password_manager.password_manager import get_db_credentials
import os

credentials = get_db_credentials(region_name="eu-west-2")
for key, value in credentials.items():
    os.environ[key] = str(value)


def connect_to_db():
    return Connection(
        user=os.getenv("user"),
        password=os.getenv("password"),
        database=os.getenv("database"),
        host=os.getenv("host"),
        port=int(os.getenv("port")),
    )


def close_db_connection(conn):
    conn.close()


conn = connect_to_db()
close_db_connection(conn)
