import boto3
import json
import sys
import pandas as pd
import awswrangler as wr
from botocore.exceptions import ClientError
from enum import Enum
import MySQLdb


SECRET_NAME = None
REGION_NAME = "us-east-1"
MYSQL_STG_TABLE = "tablename"
SQS_QUEUE_NAME = "queue_name"
MAX_FILE_PROCESSING_TIMEOUT = 120   #seconds

session = boto3.session.Session()
client = session.client(
    service_name='secretsmanager',
    region_name=REGION_NAME,
)
sqs = boto3.resource('sqs', region_name=REGION_NAME)
queue = sqs.get_queue_by_name(QueueName=SQS_QUEUE_NAME)

class FileType(Enum):
    CSV = "csv"
    CSV_TXT = "txt"
    PARQUET  = "parquet"
    CSV_TSV = "tsv"

def get_queue_message():
    
    message_bodies = []
    messages_to_delete = []
    for message in queue.receive_messages(MaxNumberOfMessages=1, VisibilityTimeout = MAX_FILE_PROCESSING_TIMEOUT):
        # process message body
        body = json.loads(message.body)
        message_bodies.append(body)
        # add message to delete
        messages_to_delete.append({
            'Id': message.message_id,
            'ReceiptHandle': message.receipt_handle
        })
    return (message_bodies, messages_to_delete)

        
def get_secret():
    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=SECRET_NAME
        )
    except ClientError as e:
        if e.response['Error']['Code'] == 'ResourceNotFoundException':
            print("The requested secret " + SECRET_NAME + " was not found")
        elif e.response['Error']['Code'] == 'InvalidRequestException':
            print("The request was invalid due to:", e)
        elif e.response['Error']['Code'] == 'InvalidParameterException':
            print("The request had invalid params:", e)
        elif e.response['Error']['Code'] == 'DecryptionFailure':
            print("The requested secret can't be decrypted using the provided KMS key:", e)
        elif e.response['Error']['Code'] == 'InternalServiceError':
            print("An error occurred on service side:", e)
        raise e    
    else:
        # Secrets Manager decrypts the secret value using the associated KMS CMK
        # Depending on whether the secret was a string or binary, only one of these fields will be populated
        text_secret_data = None
        binary_secret_data = None
        if 'SecretString' in get_secret_value_response:
            text_secret_data = get_secret_value_response['SecretString']
        else:
            binary_secret_data = get_secret_value_response['SecretBinary']
    return  text_secret_data  if text_secret_data is not None else binary_secret_data    

def read_file(bucket_name, key):
    '''Read s3 file and return pandas df'''
    lower_case_key = key.lower()
    if lower_case_key.endswith("csv") or lower_case_key.endswith("txt") or lower_case_key.endswith("tsv"):
        df = wr.s3.read_csv("s3://{}/{}".format(bucket_name, key))
    if lower_case_key.endswith("parquet"):
        df = wr.s3.read_parquet("s3://{}/{}".format(bucket_name, key))        
    return df

def get_bucket_name_s3_put_event(message):
    bucket_name = None
    s3_key = None
    return (bucket_name, s3_key)

def write_data_to_stg(df):
    '''Write data to mysql stg'''
    con = MySQLdb.connect() 
    secret = get_secret()
    df.write_frame(df, con=con, name=MYSQL_STG_TABLE, if_exists='replace', flavor='mysql')

def process_file(bucket_name, s3_key):
    df = read_file(bucket_name, s3_key)
    write_data_to_stg(df)

def start_process():
    while True:
        message = get_queue_message()
        if len(message[0])==0:
            break
        for item in message[0]:
            bucket_name, s3_key = get_bucket_name_s3_put_event(item)
        try:
            if bucket_name is not None and s3_key is not None:
                process_file(bucket_name, s3_key)
                delete_response = queue.delete_messages(Entries=message[1][0])
                print(delete_response)  
        except:
            pass    
        


def glue_main():
    try:
        from awsglue.utils import getResolvedOptions
        args = getResolvedOptions(sys.argv,
                          ['JOB_NAME'])
        start_process()

    except Exception as err:
        print(err)
        raise err
    
    

def lambda_handler(event, context):
    try:
        start_process()

    except Exception as err:
        print(err)
        raise err
