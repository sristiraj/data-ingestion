import sys
from pyspark.sql import SparkSession
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql.functions import *
from datetime import datetime



args = getResolvedOptions(sys.argv, ["JOB_NAME","INPUT_DATA_PATH","OUTPUT_DATA_PATH","INPUT_DATA_TYPE","OUTPUT_DATA_TYPE"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session


df_input = spark.read.format(args["INPUT_DATA_TYPE"]).option("inferSchema","true").load(args["INPUT_DATA_PATH"])
df_output = spark.read.format(args["OUTPUT_DATA_TYPE"]).option("inferSchema","true").load(args["OUTPUT_DATA_PATH"])
