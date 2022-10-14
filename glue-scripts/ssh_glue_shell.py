import paramiko
import sys
import pandas as pd
import os
import io
import stat
from concurrent.futures import ThreadPoolExecutor,as_completed
from datetime import datetime
import boto3
import logging
from awsglue.utils import getResolvedOptions
import logging


MAX_THREADPOOL_EXEC=4
args = getResolvedOptions(sys.argv, ["JOB_NAME","REGION_NAME", "OUTPUT_BUCKET", "OUTPUT_PATH", "INPUT_PATH", "IS_INPUT_PATH_DIR", "HOSTNAME", "USERNAME", "PASSWORD"])
FILE_CHUNK = 1000

logger = logging.getLogger()
 

class RemoteSSHClient:
    '''
    Remote SSH client handling class
    '''
    def __init__(self,
            hostName: str,
            userName: str,
            password: str
        ):
            self.hostName = hostName
            self.userName = userName
            self.password = password

    def connection(self):
        ssh_client = paramiko.SSHClient()
        ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh_client.connect(hostname=self.hostName,username=self.userName,password=self.password)        
        logger.info("SSH Client connected")
        return ssh_client

    def read(self, filepath: str):
        ftp_client=self.connection().open_sftp()  
        with ftp_client.open(filepath, "rb") as fl:
            fl.prefetch()
            data = fl.read() 
        return data

    def reader(self, filepath: str):
        '''
        Open a file object buffer of SSH/SFTP and return file bytes data
        '''
        ftp_client=self.connection().open_sftp() 
        return ftp_client.open(filepath, "rb")

    def list_files(self, filepath: str):
        '''
        List all the files which are not directory in the given path and return list
        '''
        ftp_client=self.connection().open_sftp()  
        return [(f.filename,filepath+f.filename) for f in ftp_client.listdir_attr(filepath) if not stat.S_ISDIR(f.st_mode)]

class DestinationS3Client:
    '''
    Class defines necessary methods for writing data to s3 using boto3 session passed during initialization
    '''
    def __init__(self, session):
        self.boto3_session = session
        self.s3_client = session.client("s3")


    def upload_fileobj(self, fl, bucket_name, prefix):
        '''
        Upload file like object data to bucket and s3 prefix
        '''
        self.s3_client.upload_fileobj(fl, bucket_name, prefix)

    def upload_fileIO(self, data, bucket_name, prefix):
        '''
        Upload byte buffer to bucket and prefix
        '''
        buff2 = io.BytesIO(data)
        self.s3_client.upload_fileobj(buff2, bucket_name, prefix)

def main():
    '''
    Start of processing here
    '''

    session = boto3.Session()
    s3_client = DestinationS3Client(session)
    rc = RemoteSSHClient(args["HOSTNAME"], args["USERNAME"], args["PASSWORD"])
    
    if args.get("IS_INPUT_PATH_DIR",False) == "False":
        data = rc.read(args["INPUT_PATH"])
        s3_client.upload_fileIO(data, args["OUTPUT_BUCKET"], args["OUTPUT_PATH"])
    elif args.get("IS_INPUT_PATH_DIR",False)  == "True":    
        files = rc.list_files(args["INPUT_PATH"])
        logger.info(files)
        with ThreadPoolExecutor(max_workers=MAX_THREADPOOL_EXEC) as executors:
            futures_to_work = [executors.submit(s3_client.upload_fileobj, rc.reader(file[1]), args["OUTPUT_BUCKET"], os.path.join(args["OUTPUT_PATH"], file[0])) for file in files]
            for future in as_completed(futures_to_work):

                try:
                    data = future.result()
                    print(data)
                    logger.info(data)
                except Exception as exc:
                    print('%r generated an exception: %s' % (work, exc))


main()    
