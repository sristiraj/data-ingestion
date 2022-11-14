import json
import boto3
import os
import pprint

def lambda_handler(event, context):
    
    next_token = ""
    client = boto3.client('glue',region_name='us-east-1')
    crawler_tables = []
    db_name = os.environ.get("db_name","default")
    pp = pprint.PrettyPrinter(indent=1)
    while True:
      response = client.get_tables(DatabaseName = 'default', NextToken = next_token)
      # print(response)
      for tables in response['TableList']:
        for idx, columns in enumerate(tables['StorageDescriptor']['Columns']):
            crawler_tables.append({"db_name":db_name, "table_name": tables['Name'], "column_name": columns['Name'], "datatype":tables['StorageDescriptor']['Columns'][idx]["Type"]})
      next_token = response.get('NextToken')
      if next_token is None:
        break
    
    newlist = sorted(crawler_tables, key=lambda d: d['table_name']) 
    
    for det in newlist:
      print(det)
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
