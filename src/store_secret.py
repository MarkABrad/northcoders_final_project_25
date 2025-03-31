import boto3
import json

client = boto3.client("secretsmanager", "eu-west-2")


def store_secret(client, secret_name, aws_access_key_id, aws_secret_access_key):
    secret_data = {
        "aws_access_key_id": aws_access_key_id,
        "aws_secret_access_key": aws_secret_access_key,
    }

    try:
        client.create_secret(Name=secret_name, SecretString=json.dumps(secret_data))
        return f"Secret {secret_name} saved"
    except Exception as err:
        print(err)
        return f"Error storing secret: {err}"
