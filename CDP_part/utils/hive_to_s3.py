import yaml
import boto3
import jaydebeapi
import pandas as pd
import os

config_file_path = 'PATH TO YOUR CONFIG YAML FILE'

def load_config(config_file_path):
  with open(config_file_path, 'r') as config_custom:
    return yaml.safe_load(config_custom)

def upload_file_to_s3(file_path, aws_bucket, aws_directory):
  config = load_config(config_file_path)
  aws_connection = config['connections']['aws_connection']
  aws_access_key_id = aws_connection['aws_access_key_id']
  aws_secret_access_key = aws_connection['aws_secret_access_key']
  bucket = aws_bucket

  file_name = file_path.split('/')[-1]
  object_key = aws_directory + file_name

  s3 = boto3.client(
    's3',
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
  )

  s3.upload_file(
    Filename=file_path,
    Bucket=bucket,
    Key=object_key
  )
  print(f'upload file to {object_key} success')

def hive_to_s3(query, file_name, bucket, directory):
  temp_path = '/home/cdsw/tmp_file/'
  temp_file_path = os.path.join(temp_path, file_name)

  config = load_config(config_file_path)
  hive_connection = config['connections']['hive_connection']
  driver_class = hive_connection['driver_class']
  host = hive_connection['host']
  truststore_file = hive_connection['truststore_file']
  SSLTrustStorePwd = hive_connection['SSLTrustStorePwd']
  queue = hive_connection['queue']


  jar_files = hive_connection['jar_files']
  user = hive_connection['user']
  password = hive_connection['password']

  connection_string = f'{host};SSL=1;AuthMech=3;' \
                      f'sslTrustStore={truststore_file};' \
                      f'SSLTrustStorePwd={SSLTrustStorePwd};' \
                      f'tez.queue.name={queue}'

  try:
    conn_hive = jaydebeapi.connect(driver_class, connection_string, {'UID': user, 'PWD': password},
                                       jars=jar_files)
    cursor = conn_hive.cursor()
    cursor.execute('set hive.tez.container.size=')
    cursor.execute('set tez.task.resource.memory.mb=')
    cursor.execute('set tez.am.resource.memory.mb=')
    cursor.execute(query)

    # Fetch all rows from the result set
    result_set = cursor.fetchall()

    # Convert the result set to a Pandas DataFrame
    df = pd.DataFrame(result_set, columns=[column[0] for column in cursor.description])
    df.to_csv(temp_file_path, index=False)
    print(f'write file to {temp_file_path} success')

    # Upload the CSV file to S3
    upload_file_to_s3(temp_file_path, bucket, directory)

    # Remove the temporary CSV file
    os.remove(temp_file_path)
    print(f'remove file to {temp_file_path} success')

  except Exception as e:
    error_message = str(e).split("errorMessage:")[-1]
    # print(f'Upload data failed for {table_name}: {error_message}')
    raise ValueError(str(e))
  finally:
    # Close the cursor and the connection
    cursor.close()
    conn_hive.close()
