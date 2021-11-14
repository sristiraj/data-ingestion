import sys
from pyspark.sql import SparkSession
from pysparkutil.common.schema import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql.functions import *
from pyspark.sql.types import *
from watcherlogger.logger import watcherlogger
from datetime import datetime
import boto3
from botocore.errorfactory import ClientError


args = getResolvedOptions(sys.argv, ["JOB_NAME","INPUT_DATA_PATH","OUTPUT_DATA_PATH","GLUE_DB_NAME","GLUE_TABLE_NAME","INPUT_SCHEMA_PATH","OUTPUT_SCHEMA_PATH","SOURCE_FORMAT","TARGET_FORMAT","COMPOSITE_KEY","TARGET_PARTITON_KEY"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

class RawCleanDataSource(object):
    def __init__(self, params):
        self.params = params
        print(self.params)
        
    def read_source_data(self):
        schema_obj = Schema()
        print(self.params["input_schema_path"])
        schema_obj.set_location(self.params["input_schema_path"])
        
        schema = schema_obj.get_schema()
        print(schema)
        src_path = self.params["input_data_path"]
        
        #Get latest partition
        s3 = boto3.resource('s3')
        s3_bucket_index = src_path.replace("s3://","").find("/")
        s3_bucket = src_path[5:s3_bucket_index+5]
        s3_key = src_path[s3_bucket_index+6:]
        bucket = s3.Bucket(s3_bucket)
        objs = list(bucket.objects.filter(Prefix=s3_key))
        latest = lambda a, b: "s3://"+a+"/"+b[:b.rfind("/",0,len(b))]
        objs = [latest(obj.bucket_name, obj.key) for obj in objs]
        print(objs)
        latest_partition = ""
        for obj in objs:
            if obj > latest_partition:
                latest_partition = obj
            
        print(latest_partition)
        df = spark.read.format(self.params["source_format"]).option("header","true").schema(schema).load(latest_partition)
        print("read latest data")
        return df
    
    
class MergeDataSink(object):
    def __init__(self, params):
        self.params = params
    
    def merge_data(self, df):
        s3 = boto3.resource('s3')
        target_path = "exist"
        try:
            s3_bucket_index = self.params["output_data_path"].replace("s3://","").find("/")
            s3_bucket = self.params["output_data_path"][5:s3_bucket_index+5]
            s3_key = self.params["output_data_path"][s3_bucket_index+6:]
            print(s3_bucket)
            print(s3_key)
            bucket = s3.Bucket(s3_bucket)
            objs = list(bucket.objects.filter(Prefix=s3_key))
            if len(objs) <= 0:
                raise Exception("No path exist")
        except Exception:
            target_path = "notexist"
            print("not exist")
        if target_path ==  "exist":   
            df_target = spark.read.format(self.params["target_format"]).option("header","true").load(self.params["output_data_path"])    
        else:
            schema_obj = Schema()
            print(self.params["output_schema_path"])
            schema_obj.set_location(self.params["output_schema_path"])
        
            schema = schema_obj.get_schema()
            df_target = spark.createDataFrame(spark.sparkContext.emptyRDD(),schema)
            
        #Drop partition field from previous layer
        if "partition_load_dt_tmstmp" in df.columns:
            df = df.drop("partition_load_dt_tmstmp")
        #Drop audit fields and regenerate after merge operation is complete
        df = AuditFields().drop_audit_fields(df)
        
        #Drop partition field from previous layer
        if "partition_load_dt_tmstmp" in df_target.columns:
            df_target = df_target.drop("partition_load_dt_tmstmp")
        df_target = AuditFields().drop_audit_fields(df_target)

        df_anti = df_target.join(df, self.params["composite_key"].split(","), "leftanti")
        df_final = df_anti.unionByName(df)
        df_final = self.add_partition_source_data(df_final)
        return AuditFields().add_audit(df_final)

    def add_partition_source_data(self, df):
        return df.withColumn("partition_load_dt_tmstmp",lit(datetime.now().strftime("%Y%m%d_%H%M%S")))
  
    def write_target_data(self, df):
        schema_obj = Schema()
        schema_obj.set_location(self.params["output_schema_path"])
        schema = schema_obj.get_schema()
        df = self.merge_data(df)
        # df = spark.createDataFrame(df.rdd,schema=schema)
        df.write.format(self.params["target_format"]).mode("overwrite").option("path",self.params["output_data_path"]).saveAsTable(self.params["catalog_db"]+"."+self.params["catalog_table"])
    
    def write_target_data_partitioned(self, df):
        spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")
        schema_obj = Schema()
        schema_obj.set_location(self.params["output_schema_path"])
        schema = schema_obj.get_schema()
        df = self.merge_data(df)
        # df = spark.createDataFrame(df.rdd,schema=schema)
        df.write.partitionBy(self.params["target_partition_key"]).format(self.params["target_format"]).mode("overwrite").option("path",self.params["output_data_path"]).saveAsTable(self.params["catalog_db"]+"."+self.params["catalog_table"])
    
class AuditFields(object):
    def __init__(self):
        self.audit_fields = ["audit_created_tmstmp","audit_updated_tmstmp","audit_created_by","audit_updated_by","audit_data_source"]
    def add_audit_created_tmstmp(self,df,col_name):
        return df.withColumn(col_name, lit(datetime.now()))
    def add_audit_updated_tmstmp(self,df,col_name):
        return df.withColumn(col_name, lit(datetime.now()))  
    def add_audit_created_by(self,df,col_name):
        return df.withColumn(col_name, lit("glue"))
    def add_audit_updated_by(self,df,col_name):
        return df.withColumn(col_name, lit("glue"))  
    def add_audit_data_source(self, df, col_name):
        return df.withColumn(col_name, lit("slr"))  
    def add_audit(self, df):
        for col in self.audit_fields:
            col_name = "add_"+col
            fn = getattr(self, col_name)
            df = fn(df,col)
        return df    

    def drop_audit_fields(self, df):
        return df.drop(*self.audit_fields)    

if __name__ == "__main__":
    
    context = {"job_name":args["JOB_NAME"], "service_arn":"SLRLoadMetric", "module_name":"SLR", "job_type":"full"}
    logger = watcherlogger().Builder().setLogLevel(logging.INFO).setStreamNamePrefix(context["module_name"]).getOrCreate()
    print("Started Job")
    #Separate read and write params
    read_params = {"input_data_path":args["INPUT_DATA_PATH"],"input_schema_path":args["INPUT_SCHEMA_PATH"],"source_format":args["SOURCE_FORMAT"], "composite_key":args["COMPOSITE_KEY"]}
    write_params = {"output_data_path":args["OUTPUT_DATA_PATH"], "output_schema_path":args["OUTPUT_SCHEMA_PATH"],"catalog_db":args["GLUE_DB_NAME"],"catalog_table":args["GLUE_TABLE_NAME"],"target_format":args["TARGET_FORMAT"],"composite_key":args["COMPOSITE_KEY"],"target_partition_key":args["TARGET_PARTITON_KEY"]}
    log_data = context
    print("Read source")
    #Read source data
    source = RawCleanDataSource(read_params).read_source_data()
    
    #Write target data
    MergeDataSink(write_params).write_target_data_partitioned(source)
    
