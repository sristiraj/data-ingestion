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


args = getResolvedOptions(sys.argv, ["JOB_NAME","OUTPUT_TMP_PATH","REDSHIFT_DB_NAME","REDSHIFT_TABLE_NAME","GLUE_CONN_NAME","REDSHIFT_STG_NAME","REDSHIFT_HOLIDAY_TABLE","SQL_PATH","MONTH_SQL_PATH","REDSHIFT_MONTH_TABLE_NAME","REDSHIFT_MONTH_STG_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
logger = logging.getLogger(__name__)

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
    
    def write_target_data_jdbc(self, df, table, post_query):
        dyf = DynamicFrame.fromDF(df, glueContext, "dyf")

        print(f"Perform post stg load operation: {post_query}".format())
        datasink1 = glueContext.write_dynamic_frame.from_jdbc_conf(frame = dyf, catalog_connection = self.params["glue_conn_name"], connection_options = {"dbtable": self.params["redshift_table"], "database": self.params["redshift_db"],"postactions":post_query}, redshift_tmp_dir = self.params["output_tmp_path"], transformation_ctx = "datasink1")
    

if __name__ == "__main__":
    
    context = {"job_name":args["JOB_NAME"], "service_arn":"date lkp loader", "module_name":"AHBR", "job_type":"full"}
#     logger = watcherlogger().Builder().setLogLevel(logging.INFO).setStreamNamePrefix(context["module_name"]).getOrCreate()
    
    #Retrieve params
    month_dim = args["REDSHIFT_MONTH_TABLE_NAME"]
    stg_month_dim = args["REDSHIFT_MONTH_STG_NAME"]
    output_tmp_path = args["OUTPUT_TMP_PATH"]
    redshift_db = args["REDSHIFT_DB_NAME"]
    date_dim = args["REDSHIFT_TABLE_NAME"]
    stg_date_dim = args["REDSHIFT_STG_NAME"]
    glue_conn_name = args["GLUE_CONN_NAME"]
    federal_holiday = args["REDSHIFT_HOLIDAY_TABLE"]
    month_sparksql_s3_path = args["MONTH_SQL_PATH"]
    date_sparksql_s3_path = args["SQL_PATH"]
    
    print("Started Job")
    log_data = context
    log_data["status"] = "Start"
    
    logger.info(log_data)
    #Separate read and write params for month table load
    read_params_month = {"sql_path":month_sparksql_s3_path}
    write_params_month = {"output_tmp_path":output_tmp_path,"redshift_db":redshift_db,"redshift_table":stg_month_dim, "glue_conn_name":glue_conn_name}
    
    print("Read  month source")
    
    #Read source data using sql and transform
    source = SQLTransform(read_params_month).transform()
    
    date_load_post_query = f'''truncate table {month_dim};insert into {month_dim}(month_id, month_description, end_of_month_date, month_code) select stg.month_id, stg.month_description, stg.end_of_month_date, stg.month_code
    from {stg_month_dim} as stg;commit;truncate table {stg_month_dim};'''.format()

    print("Writing target")
    #Write target data
    ProcessedDataSink(write_params_month).write_target_data_jdbc(source, stg_month_dim, date_load_post_query)
    
    
    #Separate read and write params for date table load
    read_params_date = {"sql_path":date_sparksql_s3_path}
    write_params_date = {"output_tmp_path":output_tmp_path,"redshift_db":redshift_db,"redshift_table":stg_date_dim, "glue_conn_name":glue_conn_name}
    
    print("Loading complete for month table")
    
    print("Read  date source")
    
    #Read source data using sql and transform
    source = SQLTransform(read_params_date).transform()
    
    date_load_post_query = f'''truncate table {date_dim};insert into {date_dim}(date_id,date_wid,date,day_of_week,next_business_date, day_after_business_date, is_business_day,is_federal_holiday,holiday_description,month_id) select x.date_id, x.date_wid, x.date, x.day_of_week, x.next_business_date, 
(select min(c.date) from {stg_date_dim} c LEFT OUTER JOIN {federal_holiday} as fh2 ON c.date=fh2.date where 
  (case when lower(to_char(c.date,'DAY')) = 'saturday' or  lower(to_char(c.date,'DAY')) = 'sunday' or  fh2.date is not null then 'N' else 'Y' end ) = 'Y' and 
  c.date>x.next_business_date) day_after_business_date,
  x.is_business_day, x.is_federal_holiday, x.holiday_description,mnth.month_id from (
select cast(stg.date_id as bigint) as date_id, cast(stg.date_wid as bigint) as date_wid, stg.date, stg.day_of_week, 
(select min(b.date) from {stg_date_dim} b LEFT OUTER JOIN {federal_holiday} as fh1 ON b.date=fh1.date where 
  (case when lower(to_char(b.date,'DAY')) = 'saturday' or  lower(to_char(b.date,'DAY')) = 'sunday' or  fh1.date is not null then 'N' else 'Y' end ) = 'Y' and 
  b.date>stg.date) next_business_date,
case when lower(to_char(stg.date,'DAY')) = 'saturday' or  lower(to_char(stg.date,'DAY')) = 'sunday' or  federal_holiday.date is not null then 'N' else 'Y' end is_business_day,
case when federal_holiday.date is not null then 'Y' else 'N' end is_federal_holiday,
federal_holiday.holiday_description FROM 
{stg_date_dim} as stg LEFT OUTER JOIN {federal_holiday} as federal_holiday ON stg.date=federal_holiday.date) x left join {month_dim} mnth on to_char(x.date,'YYYYMM')=mnth.month_code;commit;truncate table {stg_date_dim};'''.format()

    print("Writing target")
    #Write target data
    ProcessedDataSink(write_params_date).write_target_data_jdbc(source, stg_date_dim, date_load_post_query)
    
    print("Loading complete for date table")
   
    log_data["status"] = "success"
    logger.info(log_data)
    
