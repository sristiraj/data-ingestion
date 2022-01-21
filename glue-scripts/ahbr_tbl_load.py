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


args = getResolvedOptions(sys.argv, ["JOB_NAME","REGION_NAME", "OUTPUT_TMP_PATH","REDSHIFT_DB_NAME","GLUE_CONN_NAME","AHBR_HEADER_TABLE","AHBR_DETAIL_TABLE","AHBR_MAIN_TABLE","FUND_DESC_TABLE","CONTRIB_SRC_GROUP_TABLE"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
AWS_REGION = args["REGION_NAME"]

class ProcessedDataSource(object):
    def __init__(self, params):
        self.params = params
        
    def read_source_data_jdbc_query(self, query):
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
        # df_header = spark.read.format("csv").option("header","true").load(input_data_path+"/header").fillna("")
        # df_detail = spark.read.format("csv").option("header","true").load(input_data_path+"/detail").fillna("")
        connection_redshift_options["query"] = query
        df = glueContext.create_dynamic_frame_from_options(connection_type="redshift", connection_options=connection_redshift_options).toDF()
        return df
  
class ProcessedDataSink(object):
    def __init__(self, params):
        self.params = params
    
    
    def write_target_data_jdbc(self, df, tbl_name):
        dyf = DynamicFrame.fromDF(df, glueContext, "dyf")
        datasink1 = glueContext.write_dynamic_frame.from_jdbc_conf(frame = dyf, catalog_connection = self.params["glue_conn_name"], connection_options = {"dbtable": tbl_name, "database": self.params["catalog_db"]}, redshift_tmp_dir = self.params["tmp_dir_path"], transformation_ctx = "datasink1")

if __name__ == "__main__":
    
    context = {"job_name":args["JOB_NAME"], "service_arn":"aws-redshift-to-redshift-load", "module_name":"AHBR", "job_type":"full"}
    print("Started Job")
    #Separate read and write params
    params = {"catalog_db":args["REDSHIFT_DB_NAME"], "glue_conn_name":args["GLUE_CONN_NAME"], "ahbr_main_table":args["AHBR_MAIN_TABLE"], "ahbr_header_table":args["AHBR_HEADER_TABLE"], "ahbr_detail_table":args["AHBR_DETAIL_TABLE"], "fund_desc_table":args["FUND_DESC_TABLE"], "contrib_src_grp_table":args["CONTRIB_SRC_GROUP_TABLE"], "tmp_dir_path":args["OUTPUT_TMP_PATH"]}
    
    query_header='''
    select distinct
cast(9999 as bigint) tsp_report_number,
cast(9999 as bigint) payroll_office_number,
cast(null as varchar(255)) requestor , 
cast(null as varchar(255)) participant_name ,
part_ssn  ,
cast(null as date) dob , 
plan_num plan , 
cast(null as varchar(255)) department_code  ,
cast(null as varchar(255)) personal_office_indicator  ,
cast(null as varchar(255)) employment_code , 
cast(null as date) employment_code_effective_date, 
cast(null as date) service_computation_date, 
cast(null as date) input_start_date, 
cast(null as date) input_end_date, 
cast(null as varchar(255)) auto_enrollment_code , 
cast(null as date) auto_enrollment_date 
from staging.ahbr_5001_title
where title is not null 
    '''
    
    query_detail = '''
    select part_ssn,
trade_date as post_date,
cast(null as date) as pay_date,
title as activity,
std_usage_code_2 as ae,
reversal_code as rv,
fnd.fund_short_name as fund,
cast(sum(case when upper(con_src_grp.group_name)='EMPLOYEE' THEN val_2 ELSE null END) as decimal(10,2)) as employee,
cast(sum(case when upper(con_src_grp.group_name)='AUTOMATIC' THEN val_2 ELSE null END) as decimal(10,2))  as automatic,
cast(sum(case when upper(con_src_grp.group_name)='MATCH' THEN val_2 ELSE null END)as decimal(10,2))  as matching,
cast(NVL(sum(case when upper(con_src_grp.group_name)='EMPLOYEE' THEN val_2 ELSE null END),0)+
NVL(sum(case when upper(con_src_grp.group_name)='AUTOMATIC' THEN val_2 ELSE null END),0) +
NVL(sum(case when upper(con_src_grp.group_name)='MATCH' THEN val_2 ELSE null END),0) as decimal(10,2))  as row_total
--cast(null as column_total,
--null as contribution_total_as_date
from staging.ahbr_5001_title stg left outer join datamart.lkup_fund_description fnd
on substring(stg.fund_id,1,2)=fnd.fund_pos_12 left outer join datamart.lkup_contribution_source_group con_src_grp
on substring(stg.fund_id,3,1)=con_src_grp.fund_pos_3
where 1=1
and stg.title is not null
group by 
part_ssn,
trade_date ,
cast(null as date) ,
title ,
std_usage_code_2 ,
reversal_code ,
fnd.fund_short_name 
    '''
    
    log_data = context
    
    log_data["event"]="Read Source for header"
    print(log_data)
    #Read source data using sql and transform
    source = ProcessedDataSource(params).read_source_data_jdbc_query(query_header)
    
    log_data["event"]="Write header"
    print(log_data)
    #Write target data
    ProcessedDataSink(params).write_target_data_jdbc(source, params["ahbr_header_table"])
    
    log_data["event"]="Read Source for detail"
    print(log_data)
    #Read source data using sql and transform
    source = ProcessedDataSource(params).read_source_data_jdbc_query(query_detail)
    
    log_data["event"]="Write detail"
    print(log_data)
    #Write target data
    ProcessedDataSink(params).write_target_data_jdbc(source, params["ahbr_detail_table"])
    
