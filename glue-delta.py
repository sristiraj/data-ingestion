import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
spark.conf.set("spark.sql.legacy.json.allowEmptyString.enabled", "true")
df=spark.read.option("multiLine","true").json("s3://athenatutorialkcftech/landing/KCFPacket-2.json")
df.show()
df.write.format("delta").mode("overwrite").save("s3://athenatutorialkcftech/conformed/collectorpacket3")

job.commit()
