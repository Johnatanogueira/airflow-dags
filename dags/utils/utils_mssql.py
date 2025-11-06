import json
import time
import pandas as pd
import awswrangler as wr
import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy.engine.base import Connection
from sqlalchemy.exc import OperationalError

def get_mssql_connection(
    server_name,
    host,
    user,
    password,
    port,
    database,
    retry_limit = 3,
    retry_delay = 30
  ) -> [str, Connection]: # type: ignore
  
  start_server_conn = time.perf_counter()
  conn_str = f'mssql+pymssql://{user}:{password}@{host}:{port}/{database}'
  connection = None
  engine = create_engine(conn_str, pool_timeout=70, pool_pre_ping=True)
  
  for i in range(retry_limit):
    try:
      connection = engine.connect()
      elapsed_server_conn = time.perf_counter() - start_server_conn
      print(f"Elapsed time to connect on server [{server_name} - Retry {i}/{retry_limit}]: {elapsed_server_conn}")
      return connection
    except OperationalError as oe:
      print(f"Fail to connect on server {server_name}. Retry {i}/{retry_limit}")
      time.sleep(retry_delay)

  raise OperationalError(f'Fail to connect on server {server_name}. Retried {retry_limit} times.')

def extract_mssql(cur_conn, qry, cur_server_name, iteration) -> [pd.DataFrame, int]: # type: ignore
  try:
    start = time.perf_counter()
    df_enriched_from_mssql = pd.read_sql(qry, cur_conn)
    elapsed = time.perf_counter() - start
    df_enriched_from_mssql_count = df_enriched_from_mssql.shape[0]
    print(f"Enriquecido no servidor {cur_server_name}, {df_enriched_from_mssql_count} rows. took: {elapsed}")
    return df_enriched_from_mssql
  except Exception as e:
    print(f"Fail to extract from MSSQL server {cur_server_name}, iteration: {iteration}")
    print(e)
    raise e