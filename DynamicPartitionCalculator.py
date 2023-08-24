# ####################################################Module Information################################################
#  Module Name         :   DynamicPartitionCalculator
#  Purpose             :   Calculates dynamic partition count to enable optimal partition file size split
#  Workstream          :   Commercial
#  Pre-requisites      :   Spark dataframe object to be written to file system
#  Last changed on     :   23 Aug 2023
#  Last changed by     :   Sristi Raj
#  Reason for change   :   Handle dynamic allocation in spark to calculate # spark cores
#  Author              :   TW
# ######################################################################################################################

import logging
import os
import math
import uuid


class DynamicPartitionCalculator(object):
    def calculate_number_of_partitions(self, spark, df, target_files_size_in_mb=128):
        '''
        Description: This function calculates number of partitions required to achive target file size
        
        Purpose: 
        #---------------------------------------
        #
        # Input:
        # 1. spark session
        # 2. dataframe for which number of partitions to be calculated
        # 3. target file size in mb to achive
        #
        # Output:
        # 1. number of partitions
        #----------------------------------------
        '''
        df_count = df.count()
        # Sampling factor to get sample of dataframe and estimate size of full dataframe. 
        # Sample should be in fractional value.
        # If original dataframe row count is greater than 1000000 then sample 0.06 else take whole dataframe 
        SAMPLING_FACTOR = 0.06 if df_count > 1000000 else 1.0
        df_sample = df.sample(SAMPLING_FACTOR)
        uui = str(uuid.uuid1())
        df_sample.write.format("parquet").mode("overwrite").save("_data/application/{}".format(uui))
        df_sample.cache()
        df_sample_count = df_sample.count()
        logging.info("Count of dataframe {}".format(df_count))
        logging.info("count of sampled dataframe {}".format(df_sample_count))

        # Property works in spark 2.4 until 3.2. For spark 3.3+, use of AQE and spark.sql.adaptive.advisoryPartitionSizeInBytes, spark.sql.adaptive.coalescePartitions.minPartitionSize recommended
        sc = spark.sparkContext
        Path = sc._gateway.jvm.org.apache.hadoop.fs.Path
        conf = sc._jsc.hadoopConfiguration()
        data_folder = Path("_data/application/{}".format(uui))
        fs = data_folder.getFileSystem(conf)
        df_size_in_bytes = fs.getContentSummary(Path("_data/application/{}".format(uui))).getLength()
        df_size_in_mbs = int(df_size_in_bytes / (1024 ** 2))
        logging.info("Total data frame size in MB: %s", df_size_in_mbs)

        # get number of partition based on total df size and data size per partition. 
        # If df size is less than target_files_size_in_mb then default no_of_partitions to 1
        no_of_partitions = math.ceil(df_size_in_mbs / target_files_size_in_mb) if (df_size_in_mbs / target_files_size_in_mb)>1 else 1
        logging.info("number of partitions %s for input df size %s MB to achieve target files size %s in MB", no_of_partitions, df_size_in_mbs, target_files_size_in_mb)
        total_executors = sc.getConf().get("spark.executor.instances", str(no_of_partitions))
        cores_per_executor = sc.getConf().get("spark.executor.cores", str(1))
        total_core = int(total_executors) * int(cores_per_executor)
        logging.info("Total executor cores: %s", total_core)
        cluster_load = no_of_partitions / total_core
        logging.info("cluster load as per current partitions is ", cluster_load)
        if (cluster_load % 1) == 0:
            logging.info("The number of partitions %s is exactly multiple of total core %s so we can use current no_of_partitions", no_of_partitions, cluster_load)
            return int(no_of_partitions)

 

        ideal_no_of_partitions = int(no_of_partitions + (total_core * ( 1 - cluster_load % 1)))
        logging.info("ideal_no_of_partitions is %s", ideal_no_of_partitions)
        return int(ideal_no_of_partitions)
    
    def _get_size_of_dir(self, dir_path, start_path = '.'):
        '''
            Description: Internal method
            Purpose: Calculate size in bytes of a sub directory in current directory
        '''
        total_size = 0
        for dirpath, dirnames, filenames in os.walk("{}/{}".format(dir_path, start_path)):
            for f in filenames:
                fp = os.path.join(dirpath, f)
                # skip if it is symbolic link
                if not os.path.islink(fp):
                    total_size += os.path.getsize(fp)

        return total_size

    def _test_partitioner_static_allocation(self):
        '''
            Description: Test method to calculate the count of partition
            
            Purpose:

            > Test by creating a sample dataframe.
            > Write to a sample target location.
            > Calculate file size of written data.
            > Check with count of partition received from calculate_number_of_partitions method.
        '''
        try:
            from pyspark.sql import SparkSession
            from pyspark import SparkConf
            from faker import Faker
            conf = SparkConf().\
                setAppName("TestDynamicAllocPartition").\
                set("spark.dynamicAllocation.enabled","false").\
                set("spark.executor.instances",2).\
                set("spark.executor.cores",1)
            spark = SparkSession.builder.config(conf=conf).getOrCreate()
            fake = Faker()
            data = [(fake.name(), fake.color_name()) for x in range(100000)]
            data1 = [(fake.name(), fake.color_name()) for y in range(1000)]
            df1 = spark.createDataFrame(data, ("name", "color_name"))
            df2 = spark.createDataFrame(data1, ("name1", "color_name1"))
            df = df1.crossJoin(df2)
            df.repartition(1).write.format("parquet").mode("overwrite").save("_data/dynamic_alloc_test")
            df = spark.read.format("parquet").load("_data/dynamic_alloc_test")
            sz = self._get_size_of_dir("_data/dynamic_alloc_test")/1024**2
            cnt = self.calculate_number_of_partitions(spark, df, target_files_size_in_mb=128)
            print(cnt)
            
            spark.stop()
        finally:
            try:
                spark.stop()    
            finally:
                pass    
    
    def _test_partitioner_dynamic_allocation(self):
        '''
            Description: Test method to calculate the count of partition
            
            Purpose:

            > Test by creating a sample dataframe.
            > Write to a sample target location.
            > Calculate file size of written data.
            > Check with count of partition received from calculate_number_of_partitions method.
        '''
        try:
            from pyspark.sql import SparkSession
            from pyspark import SparkConf
            from faker import Faker
            conf = SparkConf().\
                setAppName("TestDynamicAllocPartition").\
                set("spark.dynamicAllocation.enabled","true").\
                set("spark.dynamicAllocation.initialExecutors",1).\
                set("spark.dynamicAllocation.minExecutors",1).\
                set("spark.dynamicAllocation.maxExecutors",3)
            
            spark = SparkSession.builder.config(conf=conf).getOrCreate()
            fake = Faker()
            data = [(fake.name(), fake.color_name()) for x in range(100000)]
            data1 = [(fake.name(), fake.color_name()) for y in range(1000)]
            df1 = spark.createDataFrame(data, ("name", "color_name"))
            df2 = spark.createDataFrame(data1, ("name1", "color_name1"))
            df = df1.crossJoin(df2)
            df.repartition(1).write.format("parquet").mode("overwrite").save("_data/dynamic_alloc_test")
            df = spark.read.format("parquet").load("_data/dynamic_alloc_test")
            sz = self._get_size_of_dir("_data/dynamic_alloc_test")/1024**2
            cnt = self.calculate_number_of_partitions(spark, df, target_files_size_in_mb=128)
            print(cnt)
            spark.stop()
        finally:
            try:
                spark.stop()    
            finally:
                pass    

# Main block for unit testing
if __name__ == "__main__":
    if os.getenv("SPARK_RUN_ENVIRONMENT","NONLOCAL") == "LOCAL":
        'Description: For unit testing purpose only'
        dp = DynamicPartitionCalculator()
        dp._test_partitioner_static_allocation()
        dp._test_partitioner_dynamic_allocation()
        
    else:
        logging.error("Invalid call to dynamic partition calculator module. Use DynamicPartitionCalculator.calculate_number_of_partitions to get number of partitions")
        raise Exception
