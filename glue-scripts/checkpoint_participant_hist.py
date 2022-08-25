#Add this class to ingestion script and call in sequence
#  before_val = _checkpoint.read_checkpoint() if not None else '20000101_000000'
#  _checkpoint.write_checkpoint()
#  after_val = _checkpoint.read_checkpoint()
# SQL query for ingestion should have CDC column which can take filter as partition_load_dt_tmstmp between {} and {}
# in SQL transform class 
# sql_query = sql_query.format(before_val["checkpoint_value"], after_val["checkpoint_value"])

class _checkpoint:
    
    checkpoint_val = datetime.now().strftime("%Y%m%d_%H%M%S")
    checkpoint_file = "s3://gluejobssubramani001/checkpoint/participant_history.checkpoint"
    #Checkpoint File storage bucket details
    checkpoint_bucket_index = checkpoint_file.replace("s3://","").replace("s3a://","").find("/")
    checkpoint_bucket = checkpoint_file[5:checkpoint_bucket_index+5]
    checkpoint_key = checkpoint_file[checkpoint_bucket_index+6:]
    
    @classmethod
    def write_checkpoint(cls):
        try:
            boto3.resource("s3").Object(_checkpoint.checkpoint_bucket, _checkpoint.checkpoint_key).put(Body='{"checkpoint_value":'+_checkpoint.checkpoint_val+'}')
        except:
            return None
    @classmethod
    def read_checkpoint(cls):
        try:
            val = boto3.resource("s3").Object(_checkpoint.checkpoint_bucket, _checkpoint.checkpoint_key).get()['Body'].read().decode("utf-8") 
        except:
            val = None
        return val    
