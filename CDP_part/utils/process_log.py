def insert_success(schema_name, table_name, record, data_date):
  insert_log = f"""insert into table prd_pjt_geo_anlt_stg.process_log
  VALUES (
    '{schema_name}',
    '{table_name}',
    '{data_date}',
    '{record}',
    DATE_FORMAT(CURRENT_TIMESTAMP, 'yyyy-MM-dd HH:mm:ss'),
    'Success',
    ''
  )
  """
  return insert_log 


def insert_fail(schema_name, table_name, error_message, data_date):
  insert_log = f"""insert into table prd_pjt_geo_anlt_stg.process_log
  VALUES (
    '{schema_name}',
    '{table_name}',
     '{data_date}',
    0,
    DATE_FORMAT(CURRENT_TIMESTAMP, 'yyyy-MM-dd HH:mm:ss'),
    'Fail',
    "{error_message}"
  )
  """
  return insert_log