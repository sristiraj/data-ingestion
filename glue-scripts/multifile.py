import pandas as pd
from io import StringIO
import re
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.transforms import Relationalize
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import  concat_ws, lit, col, trim, regexp_replace
import logging
import boto3



args = getResolvedOptions(sys.argv,["INPUT_DATA_PATH","OUTPUT_DATA_PATH","INPUT_FOLDER_PREFIX","OUTPUT_FOLDER_PREFIX"])
# prefixs = ['alight-legacy-dataload-2-wave2','alight-legacy-dataload-2']
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
logger = logging.getLogger()

input_data = args["INPUT_DATA_PATH"]

output_data = args["OUTPUT_DATA_PATH"]
input_prefix = args["INPUT_FOLDER_PREFIX"]
output_prefix = args["OUTPUT_FOLDER_PREFIX"]
logger.info("ARGUMENTS PASSED")
in_bucket = input_data

def fun(f):
  print(f)
  file_orig=f[0]
  filepath = f[0]
  data = f[1]
#   data = re.sub(r"\n,",",",data )
#   data = re.sub(r",\n",",",data )
  df = pd.read_csv(StringIO(data),header=[0],delimiter=",")
  df = df.replace(r'\n','', regex=True) 	
  s3 = boto3.resource('s3')
  outpath = filepath.replace(input_data+"/"+input_prefix,output_data+"/"+output_prefix)
  object = s3.Object(output_data, outpath.replace("s3://","").replace(output_data+"/",""))
  object.put(Body=df.to_string(index=False))
  return (filepath,data)



list_of_files = []
s3 = boto3.client('s3')
contents = s3.list_objects_v2(Bucket=in_bucket,Prefix=input_prefix)['Contents']
for index, file in enumerate(contents):
	filename = file['Key']
	if filename [-3:] in ['csv','txt']:
		path = f"s3://{input_data}/{filename}"
		list_of_files.append(path)
print(list_of_files)
rdd_all = sc.wholeTextFiles(",".join(list_of_files))
# rdd_out = rdd_all.foreach(fun)
rows = rdd_all.collect()
rdd_out = rdd_all.map(fun)  
