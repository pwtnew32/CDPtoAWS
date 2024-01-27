import boto3
from datetime import datetime, timezone

def get_file_name(bucket_name, folder_name):
    # Set your AWS credentials

    # Create an S3 client
    s3 = boto3.client('s3')
    # Get a list of objects in the bucket
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=folder_name)


    min_last_modified_date = None
    min_last_modified_file = None

    # Iterate through objects and find the file with the minimum last modified date
    for obj in response.get('Contents', []):
        last_modified = obj['LastModified']
        if min_last_modified_date is None or last_modified < min_last_modified_date:
            min_last_modified_date = last_modified
            min_last_modified_file = obj['Key']
    return min_last_modified_file