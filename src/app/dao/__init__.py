from google.cloud import bigquery
from google.oauth2 import service_account

project_id = 'calcium-sunbeam-341614'
service_accout_file = 'C:/Users/siddhant vijay singh/Downloads/sushant-calcium-sunbeam-341614-3db196fe803d.json'

service_creds = service_account.Credentials.from_service_account_file(service_accout_file)
bq_client = bigquery.Client(project=project_id, credentials=service_creds)
