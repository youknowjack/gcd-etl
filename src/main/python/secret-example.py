import boto3
import json
import logging

logging.basicConfig(level=logging.INFO)


def get_secret():
    secret_name = "comics.org"
    region_name = "us-east-1"
    session = boto3.session.Session()
    client = session.client(service_name='secretsmanager', region_name=region_name)
    get_secret_value_response = client.get_secret_value(SecretId=secret_name)
    if 'SecretString' in get_secret_value_response:
        secret = json.loads(get_secret_value_response['SecretString'])
        return (secret['username'], secret['password'])
    return None


get_secret()
