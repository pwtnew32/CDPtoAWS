import sys
import os
from utils.get_file_name import get_file_name
from utils.read_file_on_s3 import read_file_on_s3
from utils.load_df_to_db import load_df_to_db
from sqlalchemy import types

bucket_name = 'BUCKET NAME'
folder_name = 'FOLDER FILE ON BUCKET'

# get file name
file_name = get_file_name(bucket_name,folder_name)
table_name = str(file_name).split('/')[-2]
file_name = str(file_name).split('/')[-1]

# read file on S3 to df
df = read_file_on_s3(bucket_name,table_name, file_name)

# load df to db
schema = 'SCHEMA NAME'
delete_query = f'''
    truncate table {schema}.{table_name}
    '''

dtype_dict = {
  'COLUMN NAME 1': types.String(length=50),
  'COLUMN NAME 2':types.Float,
  'COLUMN NAME 3':types.Date
}

load_df_to_db(df, schema, table_name, dtype_dict, delete_query)