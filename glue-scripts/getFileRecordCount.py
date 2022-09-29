import math 

def getS3ObjectSize(bucket, path):
  s3 = boto3.resource('s3')
  my_bucket = s3.Bucket(bucket)
  total_size = 0
  for obj in my_bucket.objects.filter(Prefix=path):
    total_size = total_size + obj.size

  return total_size

def recursiveDirSize(path):
  total = 0
  dir_files = dbutils.fs.ls(path)
  for file in dir_files:
    if file.isDir():
      total += recursiveDirSize(file.path)
    else:
      total = file.size
  return total

def recordInPartFiles(df, temp_location, expectedFileSizeBytes=2097152):
  _fraction = 0.01
  totalCount = df.count()
  print("Total count in dataframe {}".format(totalCount))
  df.sample(0.01).repartition(1).write.format("parquet").mode("overwrite").save(temp_location)
  sampleCount = df.sample(_fraction).count()
  print(sampleCount)
  
  #get bucket and path details
  s3_bucket_index = temp_location.replace("s3://","").find("/")
  s3_bucket = temp_location[5:s3_bucket_index+5]
  s3_key = temp_location[s3_bucket_index+6:]
  sizeOfSampleinBytes = getS3ObjectSize(s3_bucket, s3_key)
  recordPerFile = sampleCount*expectedFileSizeBytes/sizeOfSampleinBytes if sampleCount>0 else totalCount
  return math.floor(recordPerFile)


print(recordInPartFiles(df, "dbfs:/FileStore/tables/temp_loc/"))
