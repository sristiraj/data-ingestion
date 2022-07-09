def get_status_of_job_running(job_name):
   session = boto3.session.Session()
   glue_client = session.client('glue')
   run_status  = False
   try:
      response = glue_client.get_job_runs(JobName=job_name)
      for res in response['JobRuns']:
         if res.get("JobRunState").lower()  == "running":
            run_status = True

   except ClientError as e:
      raise Exception("boto3 client error in get_status_of_job_all_runs: " + e.__str__())
   except Exception as e:
      raise Exception("Unexpected error in get_status_of_job_all_runs: " + e.__str__())
   return run_status
