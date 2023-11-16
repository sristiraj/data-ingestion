import boto3 import botocore
import boto3
import json
import os
import logging
import sys

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.resource('s3')

def checkpoint_file_copy_success(checkpoint_bucket, checkpoint_key, source_bucket, source_key):
    pass

def checkpoint_file_copy_error(checkpoint_bucket, checkpoint_key, source_bucket, source_key):
    pass


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
    
    logger.info("New files uploaded to the source bucket. Starting lambda function.")
    source_key = event['Records'][0]['s3']['object']['key'] 
    source_bucket = event['Records'][0]['s3']['bucket']['name']
    try:
        destination_bucket = os.environ['destination_bucket']
        destination_prefix = os.environ['destination_prefix']
        destination_key = get_destination_key(source_key, destination_prefix)
        copy_file_s3_to_s3(source_bucket, source_key, destination_bucket, destination_key)
    except Exception as e:
        checkpoint_file_copy_error(checkpoint_bucket, checkpoint_key, source_bucket, source_key)
    
    checkpoint_file_copy_success(checkpoint_bucket, checkpoint_key, source_bucket, source_key)
    logger.info(f"File s3://{source_bucket}/{source_key} uploaded to destination s3 bucket s3://{destination_bucket}/{destination_key}")
    
    
def glue_handler():
    logger.info("New files uploaded to source bucket. Starting glue job.")
    from awsglue.utils import getResolvedOptions
    
    glue_client = boto3.client('glue')
    event_client = boto3.client('cloudtrail')
    
    args = getResolvedOptions(sys.argv, ['WORKFLOW_NAME', 'WORKFLOW_RUN_ID', 'DESTINATION_BUCKET', 'DESTINATION_PREFIX'])
    event_id = glue_client.get_workflow_run_properties(Name=args['WORKFLOW_NAME'],RunId=args['WORKFLOW_RUN_ID'])['RunProperties']['aws:eventIds'][1:-1]
    response = event_client.lookup_events(LookupAttributes=[{'AttributeKey': 'EventName', \
                                                         'AttributeValue': 'NotifyEvent'}], \
                                                         StartTime=(datetime.datetime.now() - datetime.timedelta(minutes=5)), \
                                                         EndTime=datetime.datetime.now())['Events']
    for i in range(len(response)):
        event_payload = json.loads(response[i]['CloudTrailEvent'])['requestParameters']['eventPayload']
        if event_payload['eventId'] == event_id:
            event = json.loads(event_payload['eventBody'])
            source_key = event['Records'][0]['s3']['object']['key'] 
            source_bucket = event['Records'][0]['s3']['bucket']['name'] 
            try:
                destination_bucket = args['DESTINATION_BUCKET']
                destination_key = get_destination_key(source_key, args['DESTINATION_PREFIX'])
                copy_file_s3_to_s3(source_bucket, source_key, destination_bucket, destination_key)
            except Exception as e:
                checkpoint_file_copy_error(checkpoint_bucket, checkpoint_key, source_bucket, source_key)
    
            checkpoint_file_copy_success(checkpoint_bucket, checkpoint_key, source_bucket, source_key)    
            logger.info(f"File s3://{source_bucket}/{source_key} uploaded to destination s3 bucket s3://{destination_bucket}/{destination_key}")
            
if __name__ == "__main__":
    glue_handler()
