import requests
from datetime import datetime
import pytz

def notification(schema, table_name, error_message):
  thai_timezone = pytz.timezone('Asia/Bangkok')
  try :
    web_url = 'WEB HOOK URL'
    message = f'''
:bangbang::bangbang:Fail:bangbang::bangbang:
Date : {datetime.now(thai_timezone)}
Table Name : {schema}.{table_name}
Error Message: {error_message}
    '''
    payload = {
        "text": message
    }
    response = requests.post(web_url, json=payload)
  except Exception as e:
    raise e