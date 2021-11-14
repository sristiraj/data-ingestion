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
