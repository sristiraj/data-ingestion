
try:
    spark.sql("drop table if exists {}".format(tablename))
except Exception:
    pass
df = spark.read.option("mergeSchema","true").parquet(s3path)
df = df.drop("partition_col")
schema_json = df.schema.json()
ddl = spark.sparkContext._jvm.org.apache.spark.sql.types.DataType.fromJson(schema_json).toDDL()
print(ddl)
spark.sql("CREATE TABLE IF NOT EXISTS {0} ({1}) PARTITIONED BY (partition_col string) STORED AS PARQUET LOCATION '{2}' ".format(tablename, ddl, s3path))
spark.sql("MSCK REPAIR TABLE {}".format(tablename))
