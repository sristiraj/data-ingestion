import sys
import boto3
import time
from retry import retry
import json
import datetime
from botocore.exceptions import ClientError
from awsglue.utils import getResolvedOptions
import pandas as pd
import awswrangler as wr
import psycopg2
from psycopg2 import OperationalError, errorcodes, errors

'''######################
Set the glue parameters as per the parameters list below
Set the glue retry limit to 0.
Set the glue concurrent run to 1.
Set the glue timeout to a proper value (30 minutes)
Change the timeout value in code for const variable GLUE_TIMEOUT
Set the additional python module dependency ["retry"]

######################'''

GLUE_TIMEOUT = 30 # In Seconds
REGION_NAME = "us-east-1"
## @params: [JOB_NAME]
print("Arguments:")
print(sys.argv)
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'JOB_RUN_ID', 'SQS_QUEUE_URL', 'QUARANTINE_LOCATION', "FILE_FORMAT", "FILE_DELIMITER", "FILE_HAS_HEADER", "FILE_SCHEMA_PATH", "TABLE_NAME", "DB_SECRET_NAME", "FILE_DATE_FORMAT", "FILE_TIMESTAMP_FORMAT", "FILE_QUOTE_CHAR", "events"])

sqs_queue_url = args.get("SQS_QUEUE_URL")
s3_quarantine_loc = args.get("QUARANTINE_LOCATION")
file_format = args.get("FILE_FORMAT")
file_delimiter = args.get("FILE_DELIMITER",",")
file_has_header = args.get("FILE_HAS_HEADER","yes")
file_schema_path = args.get("FILE_SCHEMA_PATH")
table_name = args.get("TABLE_NAME")
db_secret = args.get("DB_SECRET_NAME")
date_format = args.get("FILE_DATE_FORMAT")
timestamp_format = args.get("FILE_TIMESTAMP_FORMAT")
quote_char = args.get("FILE_QUOTE_CHAR")
event = args.get("events",{})
current_runid = args.get("JOB_RUN_ID","")
print("event triggered: "+ event)
print("internal run id: {}".format(current_runid))

class FileFormatException(Exception):
    pass

def get_secret(secret_name):
    # secret_name = "MySecretName"
    region_name = REGION_NAME

    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name,
    )

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        if e.response['Error']['Code'] == 'ResourceNotFoundException':
            print("The requested secret " + secret_name + " was not found")
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
        if 'SecretString' in get_secret_value_response:
            text_secret_data = get_secret_value_response['SecretString']
        else:
            binary_secret_data = get_secret_value_response['SecretBinary']

        # Your code goes here.
        
def is_valid_s3_path(s3_path):
    if s3_path.startswith("s3://"):
        return True
    else:
        return False
        
def get_bucket_from_path(s3_path):
    if not is_valid_s3_path(s3_path):
        print("S3 Path not in correct format. Use format s3://<bucket>/<path>: {}", s3_path)
        raise ValueError("S3 Path not in correct format. Use format s3://<bucket>/<path>: {}".format(s3_path))
        
    base_path = s3_path.replace("s3://","")
    if len(base_path.replace("/","")) < len(base_path):
        return base_path[:base_path.find("/")]
    else:
        return base_path

def get_key_from_path(s3_path):
    if not is_valid_s3_path(s3_path):
        print("S3 Path not in correct format. Use format s3://<bucket>/<path>: {}", s3_path)
        raise ValueError("S3 Path not in correct format. Use format s3://<bucket>/<path>: {}".format(s3_path))
    base_path = s3_path.replace("s3://","")
    if len(base_path.replace("/","")) < len(base_path):
        bucket = base_path[:base_path.find("/")]
        return base_path.replace("{}/".format(bucket),"")
    else:
        return None
        
def getJobName():
    return args["JOB_NAME"]

def getJobCurrentRun(job_name):
    return current_runid

@retry(Exception, tries=3, delay=2)
def checkPrevGlueJobRunningStatus():
    session = boto3.session.Session()
    glue_client = session.client('glue')
    try:
        job_name = getJobName()
        current_run_id = getJobCurrentRun(job_name)
        print("Current run ID: {}".format(current_run_id))
        status_details = glue_client.get_job_runs(JobName=job_name, MaxResults=10)
        running_status = []
        for status in status_details.get("JobRuns",[]):
            run_id = status.get("Arguments",{}).get("--JOB_RUN_ID","UNKNOWN")
            if status.get("JobRunState","UNKNOWN") in ["RUNNING","STARTING","WAITING"] and run_id!=current_run_id:
                running_status.append(status)
                
        #Only allow one run of glue job. For next job stop the processing
        if running_status is not None and len(running_status)>1:
            return True
        else:
            return False
        
    except Exception as e:
        raise e
        
def read_s3_object(bucket, key):
    s3 = boto3.resource('s3')
    obj = s3.Object(bucket, key)
    return obj.get()['Body'].read().decode('utf-8') 
    
def get_colnames_schema_file():
    bucket = get_bucket_from_path(file_schema_path)
    key = get_key_from_path(file_schema_path)
    schema_str = read_s3_object(bucket, key)
    schema_arr = schema_str.split(",")
    colnames = [c.strip(" ").split(" ")[0] for c in schema_arr if len(c.strip(" ").split(" "))>=2]
    return colnames


def show_psycopg2_exception(err):
    # get details about the exception
    err_type, err_obj, traceback = sys.exc_info()
    # get the line number when exception occured
    line_n = traceback.tb_lineno
    # print the connect() error
    print ("\npsycopg2 ERROR:", err, "on line number:", line_n)
    print ("psycopg2 traceback:", traceback, "-- type:", err_type)
    # psycopg2 extensions.Diagnostics object attribute
    print ("\nextensions.Diagnostics:", err.diag)
    # print the pgcode and pgerror exceptions
    print ("pgerror:", err.pgerror)
    print ("pgcode:", err.pgcode, "\n")


def connect(conn_params_dic):
    conn = None
    try:
        print('Connecting to the PostgreSQL...........')
        conn = psycopg2.connect(**conn_params_dic)
        print("Connection successful..................")

    except OperationalError as err:
        # passing exception to function
        show_psycopg2_exception(err)
        # set the connection to 'None' in case of error
        conn = None

    return conn

# Define function using cursor.executemany() to insert the dataframe
def write_to_post_gres(datafrm, table):
    conn_params_dic = {
    "host"      : "localhost",
    "database"  : "irisdb",
    "user"      : "postgres",
    "password"  : "Passw0rd"
    }

    # Creating a list of tupples from the dataframe values
    tpls = [tuple(x) for x in datafrm.to_numpy()]

    # dataframe columns with Comma-separated
    cols = ','.join(list(datafrm.columns))

    # SQL query to execute
    sql = "INSERT INTO %s(%s) VALUES(%%s,%%s,%%s,%%s,%%s)" % (table, cols)
    cursor = conn.cursor()
    try:
        cursor.executemany(sql, tpls)
        conn.commit()
        print("Data inserted using execute_many() successfully...")
    except (Exception, psycopg2.DatabaseError) as err:
        # pass exception to function
        show_psycopg2_exception(err)
        cursor.close()
        
def write_file(df):
    wr.s3.to_parquet(df, s3_quarantine_loc+"data/file.parquet")
    
def load_file(file_bucket, file_key):
    print("Started loading file s3://{}/{}".format(file_bucket, file_key))
    file_header = 1 if file_has_header.lower() == "yes" else 0
    colnames = get_colnames_schema_file()
    f = lambda s: datetime.datetime.strptime(s,date_format)
    try:
        df = pd.read_csv("s3://{}/{}".format(file_bucket, file_key),\
            sep=file_delimiter,\
            na_values=['null', 'none'],\
            skip_blank_lines=True,\
            engine="python",\
            names=colnames,\
            header=0,\
            skiprows=file_header,\
            date_parser=f,\
            quotechar=quote_char)
            
    except Exception as e:
        print("Error: Reading file from s3://{}/{}".format(file_bucket, file_key))
        raise FileFormatException("Error reading file "+str(e))
        
    write_file(df)

@retry(Exception, tries=2, delay=10)    
def s3_file_move_to_quarantine(file_bucket, file_key, target_path):
    client = boto3.client('s3')
    target_bucket = get_bucket_from_path(target_path)
    target_key = get_key_from_path(target_path)
    
    copy_source = {'Bucket': file_bucket, 'Key': file_key}
    client.copy_object(Bucket = target_bucket, CopySource = copy_source, Key = target_key.rstrip("/")+"/" + file_key)
    client.delete_object(Bucket = file_bucket, Key = file_key)

@retry(Exception, tries=2, delay=10)
def delete_sqs_message(sqs_queue_url, message_handle):
    session = boto3.session.Session()
    sqs = session.client('sqs')
    try:
        print("Deleting message with message handle {}".format(message_handle))
        # Delete received message from queue
        sqs.delete_message(
            QueueUrl=sqs_queue_url,
            ReceiptHandle=message_handle
        )
    except Exception as e:
        print("Error: Failed to delete processed file SQS message. Retrying...")
        raise e

def read_sqs():
    session = boto3.session.Session()
    sqs = session.client('sqs')
    response = sqs.receive_message(
        QueueUrl=sqs_queue_url,
        AttributeNames=[
            'SentTimestamp'
        ],
        MaxNumberOfMessages=10,
        MessageAttributeNames=[
            'All'
        ],
        VisibilityTimeout=GLUE_TIMEOUT, # Total estimated time to process files pertaining to SQS messages with S3 object paths. Use GLUE_TIMEOUT
        WaitTimeSeconds=0
    )
    return response
    
def process_files_from_sqs_message(event):
    '''Process SQS message and files based on put object event from S3 bucket'''
    
    print("Start process SQS  message")
    # Reading messages from SQS
    
    total_files_processed = 0
    
    # IF messages exists in SQS for new files
    if event is not None:
        
        messages = [event]
        file_bucket = ""
        file_key = ""
        for message in messages:
            print(message)
            print("========================")
            mesg = json.loads(message)["Records"][0]
            
            # print(message["Body"]["detail"]["bucket"])
            file_bucket = json.loads(mesg['body'])["detail"]["bucket"]["name"]
            file_key = json.loads(mesg['body'])["detail"]["object"]["key"]
            receipt_handle = mesg['receiptHandle']
            try:
                load_file(file_bucket, file_key)
                delete_sqs_message(sqs_queue_url, receipt_handle)
                
            except FileFormatException as e:
                s3_file_move_to_quarantine(file_bucket, file_key, s3_quarantine_loc)
                delete_sqs_message(sqs_queue_url, receipt_handle)
    
    return (file_bucket, file_key)
    
def main():        
    while(checkPrevGlueJobRunningStatus()):
        print("Waiting for glue job to complete.")
        time.sleep(120)
    file_bucket, file_key = process_files_from_sqs_message(event)

    print("Successfully completed processing s3://{}/{} file from SQS".format(file_bucket, file_key))
#============================
#Test code: Comment after testing
# checkPrevGlueJobRunningStatus()
# load_file("sr-eb-bucket","TestFile.csv")
#============================
# secret = get_secret(db_secret)
main()
