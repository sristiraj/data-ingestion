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


args = getResolvedOptions(sys.argv, ["JOB_NAME","REGION_NAME", "OUTPUT_TMP_PATH","GLUE_CONN_NAME","REDSHIFT_DB_NAME","REDSHIFT_TABLE_NAME","OUTPUT_S3_PATH","OUTPUT_SCHEMA"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
AWS_REGION = args["REGION_NAME"]

class ProcessedDataSource(object):
    def __init__(self, params):
        self.params = params
        
    def read_source_data_jdbc_query(self):
        import boto3
        client = boto3.client('glue', region_name=AWS_REGION)
        response = client.get_connection(Name=self.params["glue_conn_name"])
        connection_properties = response['Connection']['ConnectionProperties']
        URL = connection_properties['JDBC_CONNECTION_URL']
        url_list = URL.split("/")
    
        host = "{}".format(url_list[-2][:-5])
        port = url_list[-2][-4:]
        database = "{}".format(url_list[-1])
        user = "{}".format(connection_properties['USERNAME'])
        pwd = "{}".format(connection_properties['PASSWORD'])
        #Set redshift query params
        connection_redshift_options = {"url": f"jdbc:redshift://{host}:{port}/{database}".format(), "user": user, "password": pwd, "redshiftTmpDir":  self.params["tmp_dir_path"]} 
        query = "SELECT * FROM "+self.params["redshift_table_name"]
        connection_redshift_options["query"] = query
        df = glueContext.create_dynamic_frame_from_options(connection_type="redshift", connection_options=connection_redshift_options).toDF()
        return df
  
class ProcessedDataSink(object):
    def __init__(self, params):
        self.params = params
    
    
    def write_target_data_jdbc(self, df):
        df_out = df.toDF(params["output_schema"])
        df_out.write.format("parquet").mode("overwrite").option("path",self.params["output_s3_path"]).save()
        
if __name__ == "__main__":
    
    context = {"job_name":args["JOB_NAME"], "service_arn":"aws-redshift-to-trusted-load", "module_name":"None", "job_type":"full"}
    print("Started Job {}".format(context["job_name"]))
    #Separate read and write params
    params = {"catalog_db":args["REDSHIFT_DB_NAME"], "glue_conn_name":args["GLUE_CONN_NAME"], "redshift_table_name":args["REDSHIFT_TABLE_NAME"], "tmp_dir_path":args["OUTPUT_TMP_PATH"],"output_s3_path":args:["OUTPUT_S3_PATH"],"output_schema":args["OUTPUT_SCHEMA"]}
    
    log_data = context
    
    log_data["event"]="Read Source from redshift"
    print(log_data)
    #Read source data using sql and transform
    source = ProcessedDataSource(params).read_source_data_jdbc_query()
    
    log_data["event"]="Write data to sink"
    print(log_data)
    #Write target data
    ProcessedDataSink(params).write_target_data_jdbc(source)
 
