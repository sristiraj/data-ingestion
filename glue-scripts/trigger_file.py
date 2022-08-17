from pyspark.sql.functions import spark_partition_id, input_file_name
from datetime import datetime
import boto3
import botocore


trigger_file_path = args["OUTPUT_DATA_PATH"]+"_trig"
index_file_path = args["OUTPUT_DATA_PATH"]+"_index"
spec_file_name = "SpecificationFileName"
run_invocation_id = "RunInvocationID"
extractdatetime = datetime.now().strftime("%Y%m%d%H%M%S")



def s3_md5sum(bucket_name, resource_name):
    try:
        md5sum = boto3.client('s3').head_object(
            Bucket=bucket_name,
            Key=resource_name
        )['ETag'][1:-1]
    except botocore.exceptions.ClientError:
        md5sum = None
        pass
    return md5sum
	
def create_meta_files(output_file_path):
	#Read output data to get output file details
	df = spark.read.parquet(output_file_path).withColumn("input_file",input_file_name())
	columns = df.columns
	#Group by file names and get count
	meta_data = df.groupBy("input_file").count().collect()
	session = boto3.Session()
	s3_client = session.client("s3")
	s3_resource = session.client("s3")
	
	#Trigger File storage bucket details
	trigger_bucket_index = trigger_file_path.replace("s3://","").replace("s3a://","").find("/")
	trigger_bucket = trigger_file_path[5:s3_bucket_index+5]
	trigger_key = trigger_file_path[s3_bucket_index+6:]
	
	#Index File storage bucket details
	index_data = []
	index_bucket_index = index_file_path.replace("s3://","").replace("s3a://","").find("/")
	index_bucket = index_file_path[5:s3_bucket_index+5]
	index_key = index_file_path[s3_bucket_index+6:]
	
	#For each file loop to get metadata
	for d in meta_data:
		trg_data={}
		index_data.append(d[0])
		trg_data["TriggerFileName"] = str(d[0])[str(d[0]).rfind("/")+1:]+".trg.json"
		trg_data["DataFileName"] = str(d[0])[str(d[0]).rfind("/")+1:]
		trg_data["NumberOfRows"] = d[1]
		trg_data["NumberOfColumns"] = columns
		trg_data["ExtractDateTime"] = extractdatetime
		trg_data["SpecificationFileName"] = spec_file_name
		trg_data["SpecificationFileVersion"] = "v01"
		trg_data["RunInvocationID"] = run_invocation_id
		trg_data["HistoryFlag"] = "N"
		
		partfile_bucket_index = d[0].replace("s3://","").replace("s3a://","").find("/")
		partfile_bucket = d[0][5:s3_bucket_index+5]
		partfile_key = d[0][s3_bucket_index+6:]
		trg_data["MD5HashKey"] = s3_md5sum(partfile_bucket, partfile_key)
		try:
			trg_data["DataFileSize"] = s3_client.get_bucket(partfile_bucket).lookup(partfile_key).size
		except Exception as e:
			trg_data["DataFileSize"] = None
			pass
		try:
			s3_resource.put_object(Body=trg_data, Bucket=trigger_bucket, Key=trigger_key+"/"+trg_data["TriggerFileName"])
		except Exception as e:
			pass
	
	if len(index_data)>0:
		s3_resource.put_object(Body="\n".join(index_data), Bucket=index_bucket, Key=index_key+"/"+spec_file_name+".index")
