import boto3
import yaml
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, types, text
from geoalchemy2 import Geometry, WKTElement



def load_df_to_db(df, schema, table_name, dtype_dict, delete_query):

   s3 = boto3.client('s3')
   response = s3.get_object(Bucket='BUCKET NAME', Key='PATH TO config.yaml')
   yaml_content = response['Body'].read().decode('utf-8')

   # Parse the YAML content
   config = yaml.safe_load(yaml_content)
   pslq_connection = config['connections']['postgresql_connection']
   host = pslq_connection['host']
   port = pslq_connection['port']
   database_name = pslq_connection['database_name']
   username = pslq_connection['username']
   password = pslq_connection['password']
   connection_string = f'postgresql://{username}:{password}@{host}:{port}/{database_name}'
   
   # Create a SQLAlchemy engine
   engine = create_engine(connection_string)
   
   # check df have records
   if len(df) > 0:
      try:
         conn = engine.connect()
         conn.execute(text(f'{delete_query}'))
         
         # reset and update index
         conn.execute(text(f"SELECT setval('{table_name}_id_seq', 1)"))
         conn.execute(text(f"UPDATE {table_name} set id = nextval('{table_name}_id_seq') - 1"))
         
         # Load the DataFrame into PostgreSQL using df.to_sql
         df.to_sql(name=table_name, schema=schema, con=conn, if_exists='append', index=False, dtype=dtype_dict)
  
         # Close the connection
         conn.commit()
         conn.close()
         print('load success!!')
      except Exception as e:
       raise e
   else:
      print('no data')
