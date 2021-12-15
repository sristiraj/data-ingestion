
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


args = getResolvedOptions(sys.argv, ["JOB_NAME","OUTPUT_TMP_PATH","REDSHIFT_DB_NAME","REDSHIFT_TABLE_NAME","SQL_PATH","GLUE_CONN_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session


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
        spark.catalog.setCurrentDatabase("data_mart")
        df = spark.sql(sql_query)
        return df
class ProcessedDataSink(object):
    def __init__(self, params):
        self.params = params
    def write_target_data(self, df):
        spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")
        schema_obj = Schema()
        schema_obj.set_location(self.params["output_schema_path"])
        schema = schema_obj.get_schema()
        # df = spark.createDataFrame(df.rdd,schema=schema)
        df.write.partitionBy("partition_load_dt_tmstmp").format(self.params["target_format"]).mode("overwrite").option("path",self.params["output_data_path"]).saveAsTable(self.params["catalog_db"]+"."+self.params["catalog_table"])
    def write_target_data_jdbc(self, df):
        client = boto3.client('glue', region_name="us-east-1")
        response = client.get_connection(Name=self.params["glue_conn_name"])
        connection_properties = response['Connection']['ConnectionProperties']
        URL = connection_properties['JDBC_CONNECTION_URL']
        url_list = URL.split("/")

        host = "{}".format(url_list[-2][:-5])
        port = url_list[-2][-4:]
        database = "{}".format(url_list[-1])
        user = "{}".format(connection_properties['USERNAME'])
        pwd = "{}".format(connection_properties['PASSWORD'])

        dyf = DynamicFrame.fromDF(df, glueContext, "dyf")
        datasink1 = glueContext.write_dynamic_frame.from_jdbc_conf(frame = dyf, catalog_connection = self.params["glue_conn_name"], connection_options = {"dbtable": self.params["catalog_table"], "database": self.params["catalog_db"]}, redshift_tmp_dir = self.params["output_tmp_path"], transformation_ctx = "datasink1")
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
#     logger = watcherlogger().Builder().setLogLevel(logging.INFO).setStreamNamePrefix(context["module_name"]).getOrCreate()
    print("Started Job")
    #Separate read and write params
    read_params = {"sql_path":args["SQL_PATH"]}
    write_params = {"output_tmp_path":args["OUTPUT_TMP_PATH"],"catalog_db":args["REDSHIFT_DB_NAME"],"catalog_table":args["REDSHIFT_TABLE_NAME"], "glue_conn_name":args["GLUE_CONN_NAME"]}
    log_data = context
    print("Read source")
    
    #Read source data using sql and transform
    source_with_audit = SQLTransform(read_params).transform()
    #Write target data
    ProcessedDataSink(write_params).write_target_data(source_with_audit)
