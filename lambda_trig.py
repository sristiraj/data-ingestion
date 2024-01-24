import json
import sys
import boto3
import time
from botocore.exceptions import ClientError
import uuid


REGION_NAME = "us-east-1"
job_name = "delta_shell"
job_runid = str(uuid.uuid4())


class FileFormatException(Exception):
    pass


def checkGlueJobStatus():
    session = boto3.session.Session()
    glue_client = session.client('glue')
    try:
        
        status_details = glue_client.get_job_runs(JobName=job_name, MaxResults=10)
        running_status = []
        for status in status_details.get("JobRuns",[]):
            if status.get("JobRunState","UNKNOWN") in ["RUNNING","STARTING","WAITING"]:
                running_status.append(status)
                
        #Only allow one run of glue job. For next job stop the processing
        if running_status is not None and len(running_status)>=1 :
            return "RUNNING"
        else:
            return "NOT RUNNING"
        
    except Exception as e:
        raise e

        
def lambda_handler(event, context):
    print({"events":event})
    print({"context":context})
    #Start glue job trigger
    session = boto3.session.Session()
    glue_client = session.client('glue')
    try:
        job_run_id = glue_client.start_job_run(
            JobName=job_name,
            Arguments={"--events":json.dumps(event), "--JOB_RUN_ID": job_runid}
        )
        print("Glue job triggered with internal run id {} and glue run id {}".format(job_runid, job_run_id))
    except Exception as e:
        print(e)
        job_run_id = ""
        raise e
    return {
        'statusCode': 200,
        'body': json.dumps({"job_run_id":job_run_id, "event": event})
    }
