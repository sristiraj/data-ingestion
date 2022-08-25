cdc_column = "partition_load_dt_tmstmp"
checkpoint_val = datetime.now().strftime("%Y%m%d_%H%M%S")
checkpoint_file = "s3://gluejobssubramani001/checkpoint/participant_history.checkpoint"
#Checkpoint File storage bucket details
checkpoint_bucket_index = checkpoint_file.replace("s3://","").replace("s3a://","").find("/")
checkpoint_bucket = checkpoint_file[5:checkpoint_bucket_index+5]
checkpoint_key = checkpoint_file[checkpoint_bucket_index+6:]

class _checkpoint:
    
    @classmethod
    def write_checkpoint(cls):
        boto3.resource("s3").Object(checkpoint_bucket, checkpoint_key).put(Body='{"checkpoint_value":'+checkpoint_val+'}')
    @classmethod
    def read_checkpoint(cls):
        return boto3.resource("s3").Object(checkpoint_bucket, checkpoint_key).get()['Body'].read().decode("utf-8") 
    
