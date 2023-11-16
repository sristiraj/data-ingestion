import boto3 import botocore
import json
import os
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.resource('s3')

#Placeholder function to modify destination key based on source key if needed
def get_destination_key(source_key, destination_key):
    return destination_key
  
#Method to copy file from source to target bucket  
def copy_file_s3_to_s3(source_bucket, source_key, destination_bucket, destination_key):
    source = {'Bucket': source_bucket, 'Key': source_key}
        
    try:
        response = s3.meta.client.copy(source, destination_bucket, destination_key)
        logger.info("File copied to the destination bucket successfully!")
        
    except botocore.exceptions.ClientError as error:
        logger.error("There was an error copying the file to the destination bucket")
        print('Error Message: {}'.format(error))
        
    except botocore.exceptions.ParamValidationError as error:
        logger.error("Missing required parameters while calling the API.")
        print('Error Message: {}'.format(error))

#Lambda handler
def lambda_handler(event, context):
    logger.info("New files uploaded to the source bucket.")
    source_key = event['Records'][0]['s3']['object']['key'] 
    source_bucket = event['Records'][0]['s3']['bucket']['name']
    destination_bucket = os.environ['destination_bucket']
    destination_prefix = os.environ['destination_prefix']
    destination_key = get_destination_key(source_key, destination_prefix)
    copy_file_s3_to_s3(source_bucket, source_key, destination_bucket, destination_key)
    logger.info(f"File s3://{source_bucket}/{source_key} uploaded to destination s3 bucket s3://{destination_bucket}/{destination_key}")
    
    
