from pyspark.sql import SparkSession
from pyspark.sql.functions import *


class RDBMSReader(object):
    def __init__(self):
        self.name = "RBDMS Spark Reader"
    def get_data(self, spark: SparkSession):
        return  spark.createDataFrame([("1","hello"),("2","hi")],["id","name"])


def apply(ele):
    updatedData = []
    with open("file.txt","w+") as f:    
        for row in ele:
            f.write(",".join([row.id, row.name]))
            updatedData.append([row.id, row.name,"yipee"])
    return iter(updatedData)        


class SnowflakeWriter(object):
    def __init__(self, write_type:str):
        self.write_type = write_type
    
    def connection(self, conn):
        self.conn = conn
        return self

    def table(self, table_name:str):
        self.table_name=table_name
        return self 

    def _read(self):
        pass  


    def write(self, df):
        df.rdd.mapPartitions(apply).toDF(cols)
        pass

if __name__=="__main__":
    spark = SparkSession.builder.appName("sample").master("local[*]").getOrCreate()
    df = RDBMSReader().get_data(spark)
    df.show()
    sf = SnowflakeWriter(write_type="merge").table("public.sample1").convert(df)    
    sf.show()
