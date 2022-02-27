import sys
from pyspark.sql import SparkSession
from pysparkutil.common.schema import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql.functions import *
# from watcherlogger.logger import watcherlogger
from datetime import datetime
from awsglue.dynamicframe import DynamicFrame


args = getResolvedOptions(sys.argv, ["JOB_NAME","INPUT_DATA_PATH","OUTPUT_TMP_PATH","REDSHIFT_DB_NAME","REDSHIFT_TABLE_NAME","INPUT_SCHEMA_PATH","SOURCE_FORMAT","SQL_PATH","GLUE_CONN_NAME","STG_TABLE","COMPOSITE_KEY"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

class TrustedDataSource(object):
    def __init__(self, params):
        self.params = params
        print(self.params)
        
    def read_source_data(self):
        schema_obj = Schema()
        print(self.params["input_schema_path"])
        schema_obj.set_location(self.params["input_schema_path"])
        
        schema = schema_obj.get_schema()
        print(schema)
        df = spark.read.format(self.params["source_format"]).option("header","true").schema(schema).load(self.params["input_data_path"])
        return df

class SQLTransform(object):
    def __init__(self, params):
        self.params = params
    def _schema_uri_type(self):
        if self.params["sql_path"].startswith("file://"):
            sql_path_type = "local"
        elif self.params["sql_path"].startswith("s3://"):
            sql_path_type = "s3"
        elif self.params["sql_path"].startswith("dbfs:/"):
            sql_path_type = "dbfs"
        else:
            raise InvalidSchemaLocation()
        return sql_path_type

    def _schema_data(self):
        location_type = self._schema_uri_type()
        if location_type == "local":
            with open(self.params["sql_path"], "rb") as f:
                data = f.read()
        elif location_type == "s3":
            s3 = boto3.client("s3")
            s3_bucket_index = self.params["sql_path"].replace("s3://","").find("/")
            s3_bucket = self.params["sql_path"][5:s3_bucket_index+5]
            s3_key = self.params["sql_path"][s3_bucket_index+6:]
            obj = s3.get_object(Bucket=s3_bucket, Key=s3_key)            
            data = obj["Body"].read().decode('utf-8') 
        elif location_type == "dbfs":
            with open(self.params["sql_path"].replace("dbfs:","/dbfs")) as f:
                data = f.read()
        else:
          data = None
        return data  

    def transform(self):
        sql_query = self._schema_data()
        print(sql_query)
        #df = spark.read.format("parquet").load(args["INPUT_DATA_PATH"])
        #df.createOrReplaceTempView("temptable")
        df = spark.sql(sql_query)
        print("Query successful")
        return df
        
class ProcessedDataSink(object):
    def __init__(self, params):
        self.params = params
    
    def write_target_data(self, df):
        spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")
        # df = spark.createDataFrame(df.rdd,schema=schema)
        df.write.partitionBy("partition_load_dt_tmstmp").format(self.params["target_format"]).mode("overwrite").option("path",self.params["output_data_path"]).saveAsTable(self.params["catalog_db"]+"."+self.params["catalog_table"])
    
    def write_target_data_jdbc(self, df):
        dyf = DynamicFrame.fromDF(df, glueContext, "dyf")
        datasink1 = glueContext.write_dynamic_frame.from_jdbc_conf(frame = dyf, catalog_connection = self.params["glue_conn_name"], connection_options = {"dbtable": self.params["catalog_table"], "database": self.params["catalog_db"]}, redshift_tmp_dir = self.params["output_tmp_path"], transformation_ctx = "datasink1")

class ProcessedIncrementalDataSink(object):
    def __init__(self, params):
        self.params = params
    
    def write_target_data_jdbc(self, df):
        dyf = DynamicFrame.fromDF(df, glueContext, "dyf")
        composite_key = self.params["composite_key"].split(",")
        update_join_clause=""
        print(composite_key)
        pre_update_query="begin;update {} t1  set current_indicator=0 , record_end_date = current_date select from {} t2 where ".format(self.params["catalog_table"],self.params["stg_table"])
        for key in composite_key:
            update_join_clause += "t1."+key+"="+"t2."+key+" and "
        update_join_clause +=  "1=1;end;"
        insert_stg_query = "begin;insert into {} select * from {};commit;truncate table {};end;".format(self.params["catalog_table"],self.params["stg_table"],self.params["stg_table"])
        pre_query=pre_update_query+update_join_clause+"end;"
        print(pre_query)
        datasink1 = glueContext.write_dynamic_frame.from_jdbc_conf(frame = dyf, catalog_connection = self.params["glue_conn_name"], connection_options = {"dbtable": self.params["stg_table"], "database": self.params["catalog_db"],"preactions":pre_query, "postactions":insert_stg_query}, redshift_tmp_dir = self.params["output_tmp_path"], transformation_ctx = "datasink1")
        
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
    
    context = {"job_name":args["JOB_NAME"], "service_arn":"LoadMetric", "module_name":"ABC", "job_type":"full"}
#     logger = watcherlogger().Builder().setLogLevel(logging.INFO).setStreamNamePrefix(context["module_name"]).getOrCreate()
    print("Started Job")
    #Separate read and write params
    read_params = {"input_data_path":args["INPUT_DATA_PATH"],"input_schema_path":args["INPUT_SCHEMA_PATH"],"source_format":args["SOURCE_FORMAT"],"sql_path":args["SQL_PATH"]}
    write_params = {"output_tmp_path":args["OUTPUT_TMP_PATH"],"catalog_db":args["REDSHIFT_DB_NAME"],"catalog_table":args["REDSHIFT_TABLE_NAME"], "glue_conn_name":args["GLUE_CONN_NAME"],"stg_table":args["STG_TABLE"],"composite_key":args["COMPOSITE_KEY"]}
    log_data = context
    print("Read source")
    
    #Read source data using sql and transform
    source_with_audit = SQLTransform(read_params).transform()
    #Write target data
    ProcessedIncrementalDataSink(write_params).write_target_data_jdbc(source_with_audit)
    
