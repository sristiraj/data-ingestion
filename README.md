Add glue scripts are stored under glue-scripts folder

Add below params to the glue jobs

--JOB_NAME - Glue job name

--INPUT_DATA_PATH - Input data path in s3
--SOURCE_FORMAT - Input file format like csv, parquet etc
--INPUT_SCHEMA_PATH - Input data schema file in avro schema format (file should end with .avsc)

--OUTPUT_DATA_PATH - Output data path in s3
--OUTPUT_SCHEMA_PATH - Output data schema file path in s3
--TARGET_FORMAT - Target format of data
--GLUE_DB_NAME - Glue database name within table creation
--GLUE_TABLE_NAME - Glue table name for the table creation

In case if target is partitioned:
--TARGET_PARTITION_KEY - Key of target partition

In case of jdbc source ingestion pass below params:

--INPUT_JDBC_URL
--INPUT_JDBC_SECRET

In case of jdbc target ingestion pass below params:

--OUTPUT_JDBC_URL
--OUTPUT_JDBC_SECRET

For trusted ingestion with merge approach:
--COMPOSITE_KEY - Key for joining and performing upsert