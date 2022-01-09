class TrustedDataSource(object):
    def __init__(self, params):
        self.params = params
        print(self.params)
    
    def read_source_data(self):
        print(self.params["input_schema_path"])
        rd = sc.textFile(self.params["input_data_path"])
        df=rd.zipWithIndex().toDF().filter("_2>0")
        df_data_column = df.select("_1")
        df_data_column.write.format("csv").mode("overwrite").option("header","false").save(self.params["input_data_path"].strip("/")+"_tmp")
        df = spark.read.format(self.params["source_format"]).option("header","true").option("delimiter",self.params["source_file_delimiter"]).option("inferSchema","true").load(self.params["input_data_path"].strip("/")+"_tmp")
        return df
        
    def read_source_latest_partition_data(self):
        latest_partition = self._latest_s3_path(self.params["input_data_path"])
        df = spark.read.format(self.params["source_format"]).option("header","true").option("delimiter",self.params["source_file_delimiter"]).option("inferSchema","true").load(latest_partition)
        return df   
