import jaydebeapi
import yaml
from utils.process_log import insert_success, insert_fail
from utils.get_record import replace_insert_with_count
from utils.notification import notification
import signal

class TimeoutError(Exception):
    pass

def timeout_handler(self, frame):
  raise TimeoutError("Timed Out")  
  
def execute_and_log(insert_sql, schema, table_name, data_date, timeout_seconds=1800):  
  with open('PATH TO YOUR CONFIG YAML FILE', 'r') as config_custom:
    common_config = yaml.safe_load(config_custom)
    
  hive_connection = common_config['connections']['hive_connection']
  driver_class = hive_connection['driver_class']
  host = hive_connection['host']
  truststore_file = hive_connection['truststore_file']
  SSLTrustStorePwd= hive_connection['SSLTrustStorePwd']
  queue = hive_connection['queue']

  jar_files = hive_connection['jar_files']
  user = hive_connection['user']
  password = hive_connection['password']

  connection_string = f'{host};SSL=1;AuthMech=3;\
  sslTrustStore={truststore_file};\
  SSLTrustStorePwd={SSLTrustStorePwd};\
  tez.queue.name={queue}' 

  conn_hive = jaydebeapi.connect(driver_class, connection_string, {'UID': user, 'PWD': password},jars=jar_files)
  cursor = conn_hive.cursor()
  
  try:
    signal.signal(signal.SIGALRM, timeout_handler)
    signal.alarm(timeout_seconds)
    cursor.execute('set hive.tez.container.size=20480')
    cursor.execute('set tez.task.resource.memory.mb=16384')
    cursor.execute('set tez.am.resource.memory.mb=20480')
    
    count_sql = replace_insert_with_count(insert_sql)
    cursor.execute(count_sql)
    num_records_inserted = cursor.fetchone()[0]


    cursor.execute(insert_sql)
    log_sql = insert_success(schema, table_name, num_records_inserted, data_date)  
    cursor.execute(log_sql)
  
  except TimeoutError as e:
    error_message = str(e).split("errorMessage:")[-1]
    print(f'Timeout error: {e}')
    log_sql = insert_fail(schema, table_name, error_message, data_date)  
    cursor.execute(log_sql)
    notification(schema, table_name, error_message)
    raise TimeoutError('Execution timed out.')
    
  except Exception as e:
    error_message = str(e).split("errorMessage:")[-1]
    print(f'Insert data failed for {table_name}: {error_message}')
    log_sql = insert_fail(schema, table_name, error_message, data_date)  
    cursor.execute(log_sql)
    notification(schema, table_name, error_message)
    raise ValueError(str(e))

  finally:
    #Close the cursor and the connection
    cursor.close()
    conn_hive.close()
    signal.alarm(0)  # Reset the alarm