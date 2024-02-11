import sys 
sys.path.append("PATH TO SCRIPT")
from utils.execute_and_log import execute_and_log
from datetime import datetime, timedelta
import pytz

thai_timezone = pytz.timezone('Asia/Bangkok')
current_datetime = datetime.now(thai_timezone)

data_date = current_datetime.strftime("%Y-%m-%d")

schema = 'SCHEMA NAME'
table = 'TABLE NAME'

insert_data =f'''
    insert overwrite table {schema}.{table}
    select
    ....
  '''
execute_and_log(insert_data, schema, table, data_date)