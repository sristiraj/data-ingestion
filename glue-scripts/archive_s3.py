def archive_data(input_path, processed_file_path, ctx):
        '''Archive files from interim path to archive path after load processing'''
        print("Start archive source data")
		#Get source bucket path details
        src_path = input_path
        s3 = boto3.resource('s3')
        s3_bucket_index = src_path.replace("s3://","").find("/")
        s3_bucket = src_path[5:s3_bucket_index+5]
        s3_key = src_path[s3_bucket_index+6:]
		#Get target bucket path details
		trg_path = processed_file_path
		trg_s3_bucket_index = trg_path.replace("s3://","").find("/")
        trg_s3_bucket = trg_path[5:s3_bucket_index+5]
        trg_s3_key = trg_path[s3_bucket_index+6:]
        #List all files in interim path
	bucket = s3.Bucket(s3_bucket)
        objs = list(bucket.objects.filter(Prefix=s3_key))
        print(objs)
        for obj in objs:
            bucket_name = obj.bucket_name
            key = obj.key
            print(bucket_name)
            print(key)
            if "." in key:
                s3.meta.client.copy({'Bucket':bucket_name,'Key':key}, trg_s3_bucket, trg_s3_key+key)
                s3.Object(bucket_name, key).delete()
