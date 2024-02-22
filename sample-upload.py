import requests
import json

filename_to_upload = 'Operator information _ Flydrone.pdf'
url = requests.get("https://mij790peqi.execute-api.us-east-1.amazonaws.com/dev/geturl?ecode=abc")
url = json.loads(url.content)
#with open(filename_to_upload, 'rb') as file_to_upload:
files = {'file': open(filename_to_upload, 'rb')}
upload_response = requests.post(url['url'], 
                                    data=url['fields'], 
                                    files=files)

print(upload_response.text)
