import requests
import boto3
import json

#URL on flood monitoring.
api_url = "http://environment.data.gov.uk/flood-monitoring/id/stations"

#API Fetch flood monitoring station.
response = requests.get(api_url)
data = response.json()


# Store data in S3
s3 = boto3.client('s3')
bucket_name = 'floodmonitoringstation'
file_name = 'station_monitoring.json'

s3.put_object(
    Bucket=bucket_name,
    Key=file_name,
    Body=json.dumps(data)
)

def lambda_handler(event, context):
    # Environment Agency API URL
    api_url = "http://environment.data.gov.uk/flood-monitoring/id/stations"


    return {
        'statusCode': 200,
        'body': json.dumps('Data stored in S3')
    }