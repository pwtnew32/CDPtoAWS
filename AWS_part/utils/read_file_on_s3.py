import boto3
import pandas as pd
from io import StringIO


def read_file_on_s3(bucket_name : str,table_name : str,file_name : str) : # --> df
  eb_object_key = f'eventbridge/{table_name}/{file_name}'

  # Create an S3 client
  s3 = boto3.client('s3')
  response = s3.get_object(Bucket=bucket_name, Key=eb_object_key)
  file_content = response['Body'].read().decode('utf-8')
  df = pd.read_csv(StringIO(file_content), dtype=str)

  # Copy file to datalake path
  copy_key_object = f'datalake/{table_name}/{file_name}'
  response = s3.copy_object(
    Bucket=bucket_name,
    CopySource={'Bucket': bucket_name, 'Key': eb_object_key},
    Key=copy_key_object
    )
  print(f'move {eb_object_key} to {copy_key_object} success!')


  # Delete File
  response = s3.delete_object(
      Bucket=bucket_name,
      Key=eb_object_key
  )
  print(f'delete file {eb_object_key}')
  return df
