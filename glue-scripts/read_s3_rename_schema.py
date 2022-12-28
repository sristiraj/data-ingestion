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
import json


log = logging.getLogger()
log.setLevel(logging.INFO)

handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
log.addHandler(handler)
log.info("check")

args = getResolvedOptions(sys.argv, ["REGION_NAME", "JOB_NAME", "INPUT_CONFIG_PATH", "OUTPUT_TMP_PATH"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
AWS_REGION = args["REGION_NAME"]

class JobUtil:
    def __init__(self):
        self._name = "JobUtil"
    def process_params(self, params):
        try:
            job_config_file = params["input_config_path"]
        except Exception as e:
            log.error("Exception in retrieving config file path from job params. Provide param for config file using --JOB_CONFIG_PATH")
            raise e
        try:
            config_json = json.loads(S3Util(job_config_file).read_s3_object())  
            # log.info(config_json)
        except Exception as e:
            log.error("Not a valid json in config file provided in {}".format(str(params)))
            raise e
        # log.info(config_json)    
        return config_json
        
class S3Util:
    """Unit class to perform read operation from s3 using s3 path in format s3://bucket/key"""
    def __init__(self,path):
        self.path=path
    
    def get_bucket_keys_from_path(self, path):
        s3_bucket_index = path.replace("s3://","").find("/")
        s3_bucket = path[5:s3_bucket_index+5]
        s3_key = path[s3_bucket_index+6:]
        return s3_bucket, s3_key
        
    def read_s3_object(self):
        s3 = boto3.client("s3")
        s3_bucket, s3_key = self.get_bucket_keys_from_path(self.path)
        data = s3.get_object(Bucket=s3_bucket, Key=s3_key)
        contents = data['Body'].read().decode("utf-8")
        # log.info(contents)
        return contents
        
class ProcessedDataSource(object):
    """Processed data source is used to read data from source"""
    def __init__(self, params):
        self.params = params
        
    def read_source_data(self):
        df = spark.read.format("parquet").option("inferSchema","true").option("mergeSchema","true").option("path",self.params["input_s3_path"]).load()
        return df
  
class ProcessedDataSink(object):
    def __init__(self, params):
        self.params = params
    
    
    def write_renamed_target_data(self, df):
        out_schema = list(self.params["output_schema_cols"])
        df_out = df.toDF(*out_schema)
        spark.conf.set("spark.sql.sources.partitionOverwriteMode","STATIC")
        df_out.write.format("parquet").mode("overwrite").option("path",self.params["tmp_dir_path"]).save()
        spark.conf.set("spark.sql.sources.partitionOverwriteMode","DYNAMIC")
        df_temp = spark.read.format("parquet").option("path",self.params["tmp_dir_path"]).load()
        df_temp.write.format("parquet").mode("overwrite").option("path",self.params["input_s3_path"]).save()
        base_path = self.params["input_s3_path"].rstrip("/").rfind("/")
        df_input = spark.read.format("parquet").option("mergeSchema","true").load(base_path)
        df_input_schema = df_input.schema.json()
        try:
            df_table = spark.table(self.params["input_db_name"]+"."+self.params["input_table_name"])
            df_table_schema =  df_temp.schema.json()
        except Exception as e:
            df_table = None
            df_table_schema = {}
        if df_table_schema != df_input_schema:
            ddl = spark.sparkContext._jvm.org.apache.spark.sql.types.DataType.fromJson(df_input_schema).toDDL()
            spark.sql("CREATE TABLE IF NOT EXISTS {0} ({1}) PARTITIONED BY ({2} string) STORED AS PARQUET LOCATION '{3}' ".format(self.params["input_db_name"]+"."+self.params["input_table_name"], ddl, self.params["partition_col_name"], base_path))
            spark.sql("MSCK REPAIR TABLE {}".format(self.params["input_db_name"]+"."+self.params["input_table_name"]))
        else:
            spark.sql("MSCK REPAIR TABLE {}".format(self.params["input_db_name"]+"."+self.params["input_table_name"]))
            
            
            
            
        
        return df_temp
    
    def write_target_data(self, df):
        spark.conf.set("spark.sql.sources.partitionOverwriteMode","DYNAMIC")
        df.write.format("parquet").mode("overwrite").option("path",self.params["curated_table_path"]).save()
        
        base_path = self.params["curated_table_path"].rstrip("/").rfind("/")
        df_input = spark.read.format("parquet").option("mergeSchema","true").load(base_path)
        df_input_schema = df_input.schema.json()
        try:
            df_table = spark.table(self.params["curated_db_name"]+"."+self.params["curated_table_name"])
            df_table_schema =  df_temp.schema.json()
        except Exception as e:
            df_table = None
            df_table_schema = {}
        if df_table_schema != df_input_schema:
            ddl = spark.sparkContext._jvm.org.apache.spark.sql.types.DataType.fromJson(df_input_schema).toDDL()
            spark.sql("CREATE TABLE IF NOT EXISTS {0} ({1}) PARTITIONED BY ({2} string) STORED AS PARQUET LOCATION '{3}' ".format(self.params["curated_db_name"]+"."+self.params["curated_table_name"], ddl, self.params["partition_col_name"], base_path))
            spark.sql("MSCK REPAIR TABLE {}".format(self.params["curated_db_name"]+"."+self.params["curated_table_name"]))
        else:
            spark.sql("MSCK REPAIR TABLE {}".format(self.params["curated_db_name"]+"."+self.params["curated_table_name"]))
            
       
        
        
    def exclude_cols(self, df):
        columns = df.columns    
        selected_columns = [c for c in columns if c.upper().startswith("STD")==False]
        df_selected = df.select(*selected_columns)
        return df_selected
        
if __name__ == "__main__":
    
    context = {"job_name":args["JOB_NAME"], "service_arn":"aws-s3-to-trusted-load", "module_name":"None", "job_type":"full"}
    log.info("Started Job {}".format(context["job_name"]))
    #Separate read and write params
    params = {"input_config_path":args["INPUT_CONFIG_PATH"],"tmp_dir_path":args["OUTPUT_TMP_PATH"]}
    config_params = JobUtil().process_params(params)

    config_params.update(params)
 

    log.info("Config values")
    log.info(config_params)   
    

    for table in config_params["tables"]:
        log_data = context
        table.update(params)
        log_data["event"]="Read Source from s3"
        log.info(log_data)
        #Read source data using sql and transform
        source = ProcessedDataSource(table).read_source_data()
        
        log_data["event"]="Write data to sink"
        log.info(log_data)
        #Write target data
        target = ProcessedDataSink(table).write_renamed_target_data(source)
        
        #select only standard cols
        curated = ProcessedDataSink(table).exclude_cols(target)
        
        #write curated
        log_data["event"]="Write data to curated"
        log.info(log_data)
        
        ProcessedDataSink(table).write_target_data(curated)        
        
        
 
