import sys
from pyspark.sql import SparkSession
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql.functions import  lit
from datetime import datetime
from awsglue.dynamicframe import DynamicFrame
import boto3
import logging


args = getResolvedOptions(sys.argv, ["JOB_NAME","REGION_NAME", "INPUT_S3_PATH","OUTPUT_TMP_PATH","OUTPUT_SCHEMA"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
AWS_REGION = args["REGION_NAME"]

class ProcessedDataSource(object):
    def __init__(self, params):
        self.params = params
        
    def read_source_data(self):
        df = spark.read.format("parquet").option("path",self.params["input_s3_path"]).load()
        return df
  
class ProcessedDataSink(object):
    def __init__(self, params):
        self.params = params
    
    
    def write_target_data(self, df):
        df_out = df.toDF(params["output_schema"])
        df_out.write.format("parquet").mode("overwrite").option("path",self.params["tmp_dir_path"]).save()
        df_temp = spark.read.format("parquet").option("path",self.params["tmp_dir_path"]).load()
        df_temp.write.format("parquet").mode("overwrite").option("path",self.params["input_s3_path"]).save()
        
if __name__ == "__main__":
    
    context = {"job_name":args["JOB_NAME"], "service_arn":"aws-s3-to-trusted-load", "module_name":"None", "job_type":"full"}
    print("Started Job {}".format(context["job_name"]))
    #Separate read and write params
    params = {"input_s3_path":args["INPUT_S3_PATH"],"tmp_dir_path":args["OUTPUT_TMP_PATH"],"output_s3_path":args:["OUTPUT_S3_PATH"],"output_schema":args["OUTPUT_SCHEMA"]}
    
    log_data = context
    
    log_data["event"]="Read Source from s3"
    print(log_data)
    #Read source data using sql and transform
    source = ProcessedDataSource(params).read_source_data()
    
    log_data["event"]="Write data to sink"
    print(log_data)
    #Write target data
    ProcessedDataSink(params).write_target_data(source)
 
