from sqlite3 import IntegrityError
import paramiko
import sys
import pandas as pd
from pyspark.sql import SparkSession
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql.functions import  lit
from datetime import datetime
from awsglue.dynamicframe import DynamicFrame
import boto3
import logging


args = getResolvedOptions(sys.argv, ["JOB_NAME","REGION_NAME", "OUTPUT_PATH", "INPUT_PATH", "HOSTNAME", "USERNAME", "PASSWORD"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
AWS_REGION = args["REGION_NAME"]
file_chunk = 1000

class FileWithProgress:
     def __init__(self, fl):
         self.fl = fl
         self.size = fl.stat().st_size
         self.p = 0
     def read(self, blocksize):
         r = self.fl.read(blocksize)
         self.p += len(r)
         print(str(self.p) + " of " + str(self.size)) 
         return r
 

class RemoteClient:
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
        return ssh_client

    def readfile(self, ssh_client: paramiko.SSHClient, filepath: str, chunk: int):
        ftp_client=ssh_client.open_sftp()  
        with ftp_client.open(filepath, "rb") as fl:
            fl.prefetch()
            df = pd.read_csv(FileWithProgress(fl), sep=' ') 
        return df


def main():
    spark.conf.set("spark.sql.sources.partitionOverwriteMode","DYNAMIC")
    rc = RemoteClient(args["HOSTNAME"], args["USERNAME"], args["PASSWORD"])
    # rc = RemoteClient("172.18.0.1","test","test")
    conn = rc.connection()
    df = rc.readfile(conn, args["INPUT_PATH"], file_chunk)
    # df = rc.readfile(conn, "/home/ubuntu/hello.txt", file_chunk)
    print(df)
    sparkDF=spark.createDataFrame(df) 
    sparkDF = sparkDF.withColumn("partition_load_dt_tmstmp",lit(datetime.now().strftime("%Y%m%d_%H%M%S")))
    sparkDF.write.format("parquet").mode("overwrite").save(args["OUTPUT_PATH"])

if __name__=="__main__":
    main()    
