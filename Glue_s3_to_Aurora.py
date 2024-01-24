import sys
import boto3
import time
from retry import retry
import json
from botocore.exceptions import ClientError
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

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
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'SQS_QUEUE_URL', 'QUARANTINE_LOCATION', "FILE_FORMAT", "FILE_DELIMITER", "FILE_HAS_HEADER", "FILE_SCHEMA", "TABLE_NAME", "DB_SECRET_NAME", "FILE_DATE_FORMAT", "FILE_TIMESTAMP_FORMAT"])

sqs_queue_url = args.get("SQS_QUEUE_URL")
s3_quarantine_loc = args.get("QUARANTINE_LOCATION")
file_format = args.get("FILE_FORMAT")
file_delimiter = args.get("FILE_DELIMITER",",")
file_has_header = args.get("FILE_HAS_HEADER","yes")
file_schema = args.get("FILE_SCHEMA")
table_name = args.get("TABLE_NAME")
db_secret = args.get("DB_SECRET_NAME")
date_format = args.get("FILE_DATE_FORMAT")
timestamp_format = args.get("FILE_TIMESTAMP_FORMAT")

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

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
        print("S3 Path not in correct format. Use format s3://<bucket>/path")
        raise ValueError("S3 Path not in correct format. Use format s3://<bucket>/path")
        
    base_path = s3_path.replace("s3://","")
    if len(base_path.replace("/")) < len(base_path):
        return base_path[:base_path.find("/")]
    else:
        return base_path

def get_key_from_path(s3_path):
    if not is_valid_s3_path(s3_path):
        print("S3 Path not in correct format. Use format s3://<bucket>/path")
        raise ValueError("S3 Path not in correct format. Use format s3://<bucket>/path")
    base_path = s3_path.replace("s3://","")
    if len(base_path.replace("/")) < len(base_path):
        bucket = base_path[:base_path.find("/")]
        return base_path.replace("{}/".format(bucket),"")
    else:
        return None
        
def getJobName():
    return args["JOB_NAME"]

def getJobCurrentRun(job_name):
    return args["JOB_RUN_ID"]

@retry(Exception, tries=3, delay=2)
def checkGlueJobStatus():
    session = boto3.session.Session()
    glue_client = session.client('glue')
    try:
        job_name = getJobName()
        current_run_id = getJobCurrentRun(job_name)
        print("Current run ID: {}".format(current_run_id))
        status_details = glue_client.get_job_runs(JobName=job_name, MaxResults=10)
        running_status = []
        for status in status_details.get("JobRuns",[]):
            if status.get("JobRunState","UNKNOWN") in ["RUNNING","STARTING","WAITING"]:
                running_status.append(status)
                
        #Only allow one run of glue job. For next job stop the processing
        if running_status is not None and len(running_status)>=2 and running_status[0].get("Id") != current_run_id:
            glue_client.batch_stop_job_run(JobName=job_name, JobRunIds=[current_run_id])
        
    except Exception as e:
        raise e

def write_file(df):
    df.write.format("parquet").mode("append").save(s3_quarantine_loc+"data")
    
def load_file(file_bucket, file_key):
    file_header = True if file_has_header.lower() == "yes" else False
    try:
        df = spark.read.format(file_format).\
            option("header",file_header).\
            option("dateFormat","date_format").\
            option("timestampNTZFormat", timestamp_format).\
            schema(file_schema).\
            option("mode", "FAILFAST").load()
    except Exception as e:
        print("Error: Reading file from s3://{}/{}".format(file_bucket, file_key))
        raise FileFormatException("Error reading file")
        
    write_file(df)

@retry(Exception, tries=2, delay=10)    
def s3_file_move(file_bucket, file_key, target_path):
    s3_resource = boto3.client('s3')
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
    
def process_files_from_sqs():
    '''Read SQS and process files based on put object event from S3 bucket'''
    retry_new_messages = 0
    print("Start reading SQS for messages")
    # Reading messages from SQS
    response = read_sqs()
    total_files_processed = 0
    
    # IF messages exists in SQS for new files
    if response is not None and len(response.get('Messages',[]))>0:
        retry_new_messages = 1
        messages = response['Messages']
        file_bucket = ""
        file_key = ""
        for message in messages:
            print(message)
            print("========================")

            # print(message["Body"]["detail"]["bucket"])
            file_bucket = json.loads(message["Body"])["detail"]["bucket"]["name"]
            file_key = json.loads(message["Body"])["detail"]["object"]["key"]
            receipt_handle = message['ReceiptHandle']
            try:
                load_file(file_bucket, file_key)
                delete_sqs_message(sqs_queue_url, receipt_handle)
                total_files_processed += 1
            except FileFormatException as e:
                file_move(file_bucket, file_key)
                delete_sqs_message(sqs_queue_url, receipt_handle)
    if retry_new_messages == 1:
        print("Retrying for more messages.")
        total_files_processed+= process_files_from_sqs()
    return total_files_processed
    
def main():        
    checkGlueJobStatus()
    total_files_processed = process_files_from_sqs()

    print("Successfully completed processing {} files from SQS".format(total_files_processed))
# secret = get_secret(db_secret)
main()
job.commit()
