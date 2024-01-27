import sys 
sys.path.append("PATH TO SCRIPT ")
from utils.hive_to_s3 import hive_to_s3
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import pytz

thai_timezone = pytz.timezone('Asia/Bangkok')
current_datetime = datetime.now(thai_timezone)

query = f'''
  SELECT ....
    '''

file_name = 'FILE NAME'
aws_bucket = 'BUCKET NAME'
directory = 'PATH OF YOUR FILE ON AWS S3'

hive_to_s3(query, file_name, aws_bucket, directory)