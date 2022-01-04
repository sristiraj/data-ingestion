import sys
from pyspark.sql import SparkSession
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql.functions import  lit
# from watcherlogger.logger import watcherlogger
from datetime import datetime
from awsglue.dynamicframe import DynamicFrame
import boto3
import logging


args = getResolvedOptions(sys.argv, ["JOB_NAME","INPUT_DATA_PATH","OUTPUT_TMP_PATH","REDSHIFT_DB_NAME","REDSHIFT_TABLE_NAME","SOURCE_FORMAT","SOURCE_FILE_DELIMITER","GLUE_CONN_NAME","OUTPUT_DATA_PATH"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

class TrustedDataSource(object):
    def __init__(self, params):
        self.params = params
        print(self.params)
    
    def _latest_s3_path(self, path):
        s3 = boto3.resource('s3')
        # Check for input data path format to be of s3 URI format and retrieve s3 bucket and s3 prefix for the data
        if path.startswith("s3://"):
            path = path.strip("/")
            s3_bucket_index = path.replace("s3://","").find("/")
            s3_bucket = path[5:s3_bucket_index+5 if s3_bucket_index != -1 else len(path)+5]
            s3_key = path[s3_bucket_index+6 if s3_bucket_index != -1 else len(path):]
        else:
            raise Exception("input data path format should be s3://..")
            
        s3_bucket = s3.Bucket(s3_bucket)
        if not s3_key:
            s3_objects = s3_bucket.objects.all()
        else:
            s3_objects = s3_bucket.objects.filter(Prefix=s3_key)
        
        return max(["s3://"+s3_obj.bucket_name+"/"+s3_obj.key for s3_obj in s3_objects])
            
        
    def read_source_data(self):
        print(self.params["input_schema_path"])
        df = spark.read.format(self.params["source_format"]).option("header","true").option("delimiter",self.params["source_file_delimiter"]).option("inferSchema","true").load(self.params["input_data_path"])
        return df
        
    def read_source_latest_partition_data(self):
        latest_partition = self._latest_s3_path(self.params["input_data_path"])
        df = spark.read.format(self.params["source_format"]).option("header","true").option("delimiter",self.params["source_file_delimiter"]).option("inferSchema","true").load(latest_partition)
        return df    
  
class ProcessedDataSink(object):
    def __init__(self, params):
        self.params = params
    
    def write_target_data(self, df):
        spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")
        # df = spark.createDataFrame(df.rdd,schema=schema)
        # df.write.format("parquet").mode("overwrite").option("path",self.params["output_data_path"]).saveAsTable(self.params["catalog_db"]+"."+self.params["catalog_table"])
        df.write.format("parquet").mode("overwrite").option("path",self.params["output_data_path"]).save()
    
    def write_target_data_jdbc(self, df):
        dyf = DynamicFrame.fromDF(df, glueContext, "dyf")
        datasink1 = glueContext.write_dynamic_frame.from_jdbc_conf(frame = dyf, catalog_connection = self.params["glue_conn_name"], connection_options = {"dbtable": self.params["catalog_table"], "database": self.params["catalog_db"]}, redshift_tmp_dir = self.params["output_tmp_path"], transformation_ctx = "datasink1")

if __name__ == "__main__":
    
    context = {"job_name":args["JOB_NAME"], "service_arn":"sample_loader", "module_name":"Sample", "job_type":"full"}
#     logger = watcherlogger().Builder().setLogLevel(logging.INFO).setStreamNamePrefix(context["module_name"]).getOrCreate()
    print("Started Job")
    #Separate read and write params
    read_params = {"input_data_path":args["INPUT_DATA_PATH"],"source_format":args["SOURCE_FORMAT"],"source_file_delimiter":args["SOURCE_FILE_DELIMITER"]}
    write_params = {"output_tmp_path":args["OUTPUT_TMP_PATH"],"catalog_db":args["REDSHIFT_DB_NAME"],"catalog_table":args["REDSHIFT_TABLE_NAME"], "output_data_path":args["OUTPUT_DATA_PATH"]}
    # write_params = {"output_tmp_path":args["OUTPUT_TMP_PATH"],"catalog_db":args["REDSHIFT_DB_NAME"],"catalog_table":args["REDSHIFT_TABLE_NAME"], "glue_conn_name":args["GLUE_CONN_NAME"]}
    log_data = context
    print("Read source")
    
    #Read source data using sql and transform
    source = TrustedDataSource(read_params).read_source_latest_partition_data()
    
    print("Writing target")
    #Write target data
    ProcessedDataSink(write_params).write_target_data(source)
    
