import sys
from pyspark.sql import SparkSession
from pysparkutil.common.schema import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql.functions import *
from watcherlogger.logger import watcherlogger
from datetime import datetime


args = getResolvedOptions(sys.argv, ["JOB_NAME","INPUT_DATA_PATH","OUTPUT_DATA_PATH","GLUE_DB_NAME","GLUE_TABLE_NAME","INPUT_SCHEMA_PATH","OUTPUT_SCHEMA_PATH","SOURCE_FORMAT","TARGET_FORMAT","SOURCE_ARCHIVE_PATH"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

class RawDataSource(object):
    def __init__(self, params):
        self.params = params
        print(self.params)
        
    def read_source_data(self):
        schema_obj = Schema()
        print(self.params["input_schema_path"])
        schema_obj.set_location(self.params["input_schema_path"])
        
        schema = schema_obj.get_schema()
        print(schema)
        df = spark.read.format(self.params["source_format"]).option("delimiter","\t").option("header","true").schema(schema).load(self.params["input_data_path"])
        df = self.add_partition_raw_data(df)
        return df
    
    def add_partition_raw_data(self, df):
        return df.withColumn("partition_load_dt_tmstmp",lit(datetime.now().strftime("%Y%m%d_%H%M%S")))
        
class RawDataSink(object):
    def __init__(self, params):
        self.params = params
    def write_target_data(self, df):
        spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")
        schema_obj = Schema()
        schema_obj.set_location(self.params["output_schema_path"])
        schema = schema_obj.get_schema()
        # df = spark.createDataFrame(df.rdd,schema=schema)
        df.write.partitionBy("partition_load_dt_tmstmp").format(self.params["target_format"]).mode("overwrite").option("path",self.params["output_data_path"]).saveAsTable(self.params["catalog_db"]+"."+self.params["catalog_table"])
        
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

class ArchiveData(object):
    def __init__(self, params):
        self.params = params
    def archive_data(self):
        #Get latest partition
        print("Start archive source data")
        src_path = self.params["input_data_path"]
        s3 = boto3.resource('s3')
        s3_bucket_index = src_path.replace("s3://","").find("/")
        s3_bucket = src_path[5:s3_bucket_index+5]
        s3_key = src_path[s3_bucket_index+6:]
        print(s3_bucket)
        print(s3_key)
        bucket = s3.Bucket(s3_bucket)
        objs = list(bucket.objects.filter(Prefix=s3_key))
        print(objs)
        for obj in objs:
            bucket_name = obj.bucket_name
            key = obj.key
            print(bucket_name)
            print(key)
            s3.meta.client.copy({'Bucket':bucket_name,'Key':key}, bucket_name, 'archive/'+key)
            s3.Object(bucket_name, key).delete()
    
if __name__ == "__main__":
    
    context = {"job_name":args["JOB_NAME"], "service_arn":"SLRLoadMetric", "module_name":"SLR", "job_type":"full"}
    logger = watcherlogger().Builder().setLogLevel(logging.INFO).setStreamNamePrefix(context["module_name"]).getOrCreate()
    print("Started Job")
    #Separate read and write params
    read_params = {"input_data_path":args["INPUT_DATA_PATH"],"input_schema_path":args["INPUT_SCHEMA_PATH"],"source_format":args["SOURCE_FORMAT"],"archive_path":args["SOURCE_ARCHIVE_PATH"]}
    write_params = {"output_data_path":args["OUTPUT_DATA_PATH"], "output_schema_path":args["OUTPUT_SCHEMA_PATH"],"catalog_db":args["GLUE_DB_NAME"],"catalog_table":args["GLUE_TABLE_NAME"],"target_format":args["TARGET_FORMAT"]}
    log_data = context
    print("Read source")
    #Read source data
    source = RawDataSource(read_params).read_source_data()
    #Added audit fields
    source_with_audit = AuditFields().add_audit(source)
    
    #Write target data
    RawDataSink(write_params).write_target_data(source_with_audit)
    
    #Archive data complete
    ArchiveData(read_params).archive_data()
