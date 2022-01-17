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


args = getResolvedOptions(sys.argv, ["JOB_NAME","OUTPUT_TMP_PATH","REDSHIFT_DB_NAME","REDSHIFT_TABLE_NAME","GLUE_CONN_NAME","REDSHIFT_STG_NAME","REDSHIFT_HOLIDAY_TABLE","SQL_PATH"])
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
        df = spark.sql(sql_query)
        print("Query successful")
        print("Total records {}".format(df.count()))
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
        post_query = '''truncate table {};insert into {}(date_wid,date,date_of_week,is_business_day,is_federal_holiday,holiday_description) select cast(stg.date_wid as bigint) as date_wid, stg.date, stg.date_of_week, 
                        case when lower(to_char(stg.date,'DAY')) = 'saturday' or  lower(to_char(stg.date,'DAY')) = 'sunday' or  federal_holiday.date is not null then 'N' else 'Y' end is_business_day,
                        case when federal_holiday.date is not null then 'Y' else 'N' end is_federal_holiday,
                        federal_holiday.holiday_description FROM 
                        {} as stg LEFT OUTER JOIN {} as federal_holiday ON stg.date=federal_holiday.date;commit;truncate table {};'''.format(self.params["catalog_table"], self.params["catalog_table"], self.params["catalog_stg_table"], self.params["catalog_holiday_table"], self.params["catalog_stg_table"])
        datasink1 = glueContext.write_dynamic_frame.from_jdbc_conf(frame = dyf, catalog_connection = self.params["glue_conn_name"], connection_options = {"dbtable": self.params["catalog_stg_table"], "database": self.params["catalog_db"],"postactions":post_query}, redshift_tmp_dir = self.params["output_tmp_path"], transformation_ctx = "datasink1")

if __name__ == "__main__":
    
    context = {"job_name":args["JOB_NAME"], "service_arn":"sample_loader", "module_name":"Sample", "job_type":"full"}
#     logger = watcherlogger().Builder().setLogLevel(logging.INFO).setStreamNamePrefix(context["module_name"]).getOrCreate()
    print("Started Job")
    #Separate read and write params
    read_params = {"sql_path":args["SQL_PATH"]}
    write_params = {"output_tmp_path":args["OUTPUT_TMP_PATH"],"catalog_db":args["REDSHIFT_DB_NAME"],"catalog_table":args["REDSHIFT_TABLE_NAME"], "catalog_stg_table":args["REDSHIFT_STG_NAME"],"glue_conn_name":args["GLUE_CONN_NAME"],"catalog_holiday_table":args["REDSHIFT_HOLIDAY_TABLE"]}
    # write_params = {"output_tmp_path":args["OUTPUT_TMP_PATH"],"catalog_db":args["REDSHIFT_DB_NAME"],"catalog_table":args["REDSHIFT_TABLE_NAME"], "glue_conn_name":args["GLUE_CONN_NAME"]}
    log_data = context
    print("Read source")
    
    #Read source data using sql and transform
    source = SQLTransform(read_params).transform()
    
    print("Writing target")
    #Write target data
    ProcessedDataSink(write_params).write_target_data_jdbc(source)
    
