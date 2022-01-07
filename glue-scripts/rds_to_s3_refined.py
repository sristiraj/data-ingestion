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

AWS_REGION = "us-east-2"
args = getResolvedOptions(sys.argv, ["JOB_NAME","INPUT_TABLES","OUTPUT_TMP_PATH","GLUE_CONN_NAME","OUTPUT_DATA_PATH","OUT_FILE_FORMAT"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

class TrustedDataSource(object):
    def __init__(self, params):
        self.params = params
        print(self.params)
    
    
    def read_source_data_jdbc(self, tbl):
        print(self.params["input_schema_path"])
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
        df = spark.read.format("jdbc")..option("url", f"jdbc:sqlserver://{host}:{port};databaseName={database}".format()).option("user", user).option("password", pwd).option("query", "select * from {}".format(tbl)).load()
        return df
        
  
class ProcessedDataSink(object):
    def __init__(self, params):
        self.params = params
    
    def write_target_data(self, df, tbl):
        spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")
        # df = spark.createDataFrame(df.rdd,schema=schema)
        # df.write.format("parquet").mode("overwrite").option("path",self.params["output_data_path"]).saveAsTable(self.params["catalog_db"]+"."+self.params["catalog_table"])
        df.write.format(self.params["output_file_format"]).mode("overwrite").option("path",self.params["output_data_path"].strip("/")+"/"+tbl).save()
    
if __name__ == "__main__":
    
    context = {"job_name":args["JOB_NAME"], "service_arn":"sample_loader", "module_name":"Sample", "job_type":"full"}
#     logger = watcherlogger().Builder().setLogLevel(logging.INFO).setStreamNamePrefix(context["module_name"]).getOrCreate()
    print("Started Job")
    #Separate read and write params
    read_params = {"input_tables":args["INPUT_TABLES"],"glue_conn_name":args["GLUE_CONN_NAME"]}
    write_params = {"output_tmp_path":args["OUTPUT_TMP_PATH"],"output_file_format":args["OUTPUT_FILE_FORMAT"], "output_data_path":args["OUTPUT_DATA_PATH"]}
    # write_params = {"output_tmp_path":args["OUTPUT_TMP_PATH"],"catalog_db":args["REDSHIFT_DB_NAME"],"catalog_table":args["REDSHIFT_TABLE_NAME"], "glue_conn_name":args["GLUE_CONN_NAME"]}
    log_data = context
    print("Processing data sync")
    
    for tbl in read_params["INPUT_TABLES"].split(","):
      #Read source data using sql and transform
      source = TrustedDataSource(read_params).read_source_table()
    
      print(f"Writing target {tbl}".format())
      #Write target data
      ProcessedDataSink(write_params).write_target_data(source, tbl)
    print("End of data sync operation")
