# import libraries
# from util import get_redshift_connection, execute_sql, list_files_in_folder
import pandas as pd
import ast
import requests
import json
import boto3
from datetime import datetime
from io import StringIO
import io
import psycopg2
import ast
from dotenv import dotenv_values
dotenv_values()

# # Get the credentials from environment variable file
config = dotenv_values('.env')
# print(config)

# Create a boto3 s3 client for bucket operations
s3_client = boto3.client('s3')
s3_resource = boto3.resource('s3')

url = "https://jsearch.p.rapidapi.com/search"
querystring = {"query":"Data Engineer in Texas, USA","page":"1","num_pages":"1", "query":"Data Analyst in London, UK","page":"1","num_pages":"1"}
headers = {
	"X-RapidAPI-Key": "a2e6047782msh741065a6524408ap1f6bc1jsnaf656c8700f3",
	"X-RapidAPI-Host": "jsearch.p.rapidapi.com"
}
params = {
    "category": "Data Engineer, Data Analyst",
    "location": "UK, Canada, US",
}
response = requests.get(url, headers=headers, params=querystring) # getting info from API
# print(response.json())

# raw_data = response.json()
# columns = ['employer_website','job_id','job_employment_type','job_title','job_apply_link','job_description', 'job_city', 'job_country', 'job_posted_at_timestamp', 'employer_company_type']
# extract_data = pd.DataFrame(raw_data)[columns]
# # print(extract_data)

# def get_data_from_api():
#     url = config.get('url')
#     headers = ast.literal_eval(config.get('headers')) #returns from dict to json
#     querystring = ast.literal_eval(config.get('querystring')) #returns from dict to json
#     try:
#         # Send request to Rapid API and return the response as a Json object
#         response = requests.get(url, headers=headers, params=querystring).json()
#         print(response.json())
#     except ConnectionError:
#         print('Unable to connect to the URL endpoint')
raw_data = response.json()
# job_data = response.get('data').json().get('jsearch').json()
columns = ["employer_website", "job_id", "job_employment_type", "job_title", 
           "job_apply_link" ,"job_description", "job_city", "job_country", 
           "job_posted_at_timestamp", "employer_company_type"]
df1 = pd.DataFrame(columns)
df2 = pd.DataFrame(raw_data)
extract_data = pd.concat([df1, df2], axis=1)
# extract_data = pd.DataFrame(raw_data)[columns]
print(extract_data)


    # data = get_data_from_api()
    # print(raw_data.head(20))

# def read_from_s3(bucket_name, path):
#     objects_list = s3_client.list_objects(Bucket = bucket_name, Prefix = path) # List the objects in the bucket
#     file = objects_list.get('Contents')[1]
#     key = file.get('Key') # Get file path or key
#     obj = s3_client.get_object(Bucket = bucket_name, Key= key)
#     data = pd.read_csv(io.BytesIO(obj['Body'].read()))
#     return data

# def read_multi_files_from_s3(bucket_name, prefix):
#     objects_list = s3_client.list_objects(Bucket = bucket_name, Prefix = prefix) # List the objects in the bucket
#     files = objects_list.get('Contents')
#     keys = [file.get('Key') for file in files][1:]
#     objs = [s3_client.get_object(Bucket = bucket_name, Key= key) for key in keys]
#     dfs = [pd.read_csv(io.BytesIO(obj['Body'].read())) for obj in objs]
#     data = pd.concat(dfs)
#     return data

# Write data to S3 Bucket
def write_to_s3(data, bucket_name, folder):
    file_name = f"job_raw_data{datetime.now().strftime('%Y%m%d%h%s')}.csv" # Create a file name
    csv_buffer = StringIO() # Create a string buffer to collect csv string
    data.to_csv(csv_buffer, index=False) # Convert dataframe to CSV file and add to buffer
    csv_str = csv_buffer.getvalue() # Get the csv string
    # using the put_object(write) operation to write the data into s3
    s3_client.put_object(Bucket=bucket_name, Key=f'{folder}/{file_name}', Body=csv_str ) 

    
# def load_to_redshift(bucket_name, folder, redshift_table_name):
#     iam_role = config.get('IAM_ROLE')
#     conn = get_redshift_connection()
#     file_paths = [f's3://{bucket_name}/{file_name}' for file_name in list_files_in_folder(bucket_name, folder)]
#     for file_path in file_paths:
#         copy_query = f"""
#         copy {redshift_table_name}
#         from '{file_path}'
#         IAM_ROLE '{iam_role}'
#         csv
#         IGNOREHEADER 1;
#         """
#         execute_sql(copy_query, conn)
#     print('Data successfully loaded to Redshift')

# # def move_files_to_processed_folder(bucket_name, raw_data_folder, processed_data_folder):
# #     file_paths = list_files_in_folder(bucket_name, raw_data_folder)
# #     for file_path in file_paths:
# #         file_name = file_path.split('/')[-1]
# #         copy_source = {'Bucket': bucket_name, 'Key': file_path}
# #         # Copy files to processed folder
# #         s3_resource.meta.client.copy(copy_source, bucket_name, processed_data_folder + '/' + file_name)
#     #     s3_resource.Object(bucket_name, file_path).delete()
#     # print("Files successfully moved to 'processed_data' folder in S3")


