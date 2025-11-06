import boto3
import awswrangler as wr
from datetime import datetime, timedelta
from airflow.models.baseoperator import chain

from airflow.models.dag import DAG
from utils.utils_dags import athena_query_execution_wr
from airflow.operators.empty import EmptyOperator
from airflow.providers.docker.operators.docker import DockerOperator
from utils.meditech_dags import DockerOperator_Single_Server, ContainerOperator_Single_Server
from airflow.operators.python_operator import PythonOperator
import json
from configs.ecr_images import ECR_CRAWLER_IMAGE


POD_NAME_PREFIX = 'airflow-dag-hcpcs'

RUN_EKS = False
AWS_CREDENTIALS_ABSOLUTE_PATH="/home/johnatan/.aws/credentials"


AIRFLOW_DAGS_PATH = "/opt/airflow/dags"


ATHENA_TABLE_SCHEMA = 'temp_db'
ATHENA_TABLE_NAME = 'hcpcs'
ATHENA_TABLE_SCHEMA_MODIFIERS = 'temp_db'
ATHENA_TABLE_NAME_MODIFIERS = 'hcpcs_modifiers'
ATHENA_OUTPUT_TABLE_LOCATION = f's3://claims-management-data-lake/warehouse/{ATHENA_TABLE_SCHEMA}/{ATHENA_TABLE_NAME}/'
ATHENA_OUTPUT_TABLE_LOCATION_MODIFIERS = f's3://claims-management-data-lake/warehouse/{ATHENA_TABLE_SCHEMA_MODIFIERS}/{ATHENA_TABLE_NAME_MODIFIERS}/'
ATHENA_QUERY_OUTPUT_LOCATION = 's3://claims-management-athena-results/ClaimsManagementDataCatalog/'


HCPCS_ENVIRONMENT_VARS={
  "ATHENA_TABLE_SCHEMA":ATHENA_TABLE_SCHEMA,
  "ATHENA_TABLE_NAME":ATHENA_TABLE_NAME,
  "ATHENA_TABLE_SCHEMA_MODIFIERS":ATHENA_TABLE_SCHEMA_MODIFIERS,
  "ATHENA_TABLE_NAME_MODIFIERS":ATHENA_TABLE_NAME_MODIFIERS,
  "ATHENA_OUTPUT_TABLE_LOCATION":ATHENA_OUTPUT_TABLE_LOCATION,
  "ATHENA_OUTPUT_TABLE_LOCATION_MODIFIERS":ATHENA_OUTPUT_TABLE_LOCATION_MODIFIERS,
  "ATHENA_QUERY_OUTPUT_LOCATION":ATHENA_QUERY_OUTPUT_LOCATION,
}


def read_query_file(file_path, schema, table, output_location):
    with open(file_path, 'r') as f:
        str_query = f.read()
        
    str_query = str_query.format(
        ATHENA_TABLE_SCHEMA=schema,
        ATHENA_TABLE_NAME=table,
        ATHENA_OUTPUT_TABLE_LOCATION=output_location
    )
    return str_query



with DAG(
  dag_id            = 'hcpcs',
  schedule_interval = timedelta(days=7),
  start_date        = datetime(2025, 10, 24, 3, 0),
  catchup           = True,
  max_active_runs   = 1,
  default_args      = {'retries': 3},
  tags              = ['athena', 'analytics', 'hcpcs'],
) as dag:
  
  start = EmptyOperator(
      task_id='start',
      wait_for_downstream=True,
  )
  
  create_hcpcs_code_table = PythonOperator(
    task_id             = 'create_hcpcs_table',
    python_callable     = athena_query_execution_wr,
    op_kwargs           = {
      'sql': read_query_file(file_path=f'{AIRFLOW_DAGS_PATH}/queries/cms_hcpcs/athena_create_table.sql',
            schema=ATHENA_TABLE_SCHEMA,
            table=ATHENA_TABLE_NAME,
            output_location=ATHENA_OUTPUT_TABLE_LOCATION),
      'database': 'temp_db',
      's3_output': ATHENA_QUERY_OUTPUT_LOCATION,
    }
  )
  
  create_hcpcs_code_modifiers_table = PythonOperator(
    task_id             = 'create_hcpcs_modifiers_table',
    python_callable     = athena_query_execution_wr,
    op_kwargs           = {
      'sql': read_query_file(file_path=f'{AIRFLOW_DAGS_PATH}/queries/cms_hcpcs/athena_create_table_modifiers.sql',
            schema=ATHENA_TABLE_SCHEMA_MODIFIERS,
            table=ATHENA_TABLE_NAME_MODIFIERS,
            output_location=ATHENA_OUTPUT_TABLE_LOCATION_MODIFIERS),
      'database': 'temp_db',
      's3_output': ATHENA_QUERY_OUTPUT_LOCATION,
    }
  )
  
  extract_hcpcs_code = ContainerOperator_Single_Server(
    name="airflow-dag_hcpcs",
    name_prefix                   = POD_NAME_PREFIX,
    cmds                          =["python", "/app/src/hcpcs.py"],
    env_vars                      = HCPCS_ENVIRONMENT_VARS,
    dag                           = dag,
    aws_credentials_absolute_path = AWS_CREDENTIALS_ABSOLUTE_PATH,
    image                         = 'lifemed-crawler',
    eks                           = RUN_EKS
  )
  
  end = EmptyOperator(
    task_id             ='hcpcs_end',
    wait_for_downstream =True,
    dag = dag
  )
  chain(
    start,
    create_hcpcs_code_table,
    create_hcpcs_code_modifiers_table,
    extract_hcpcs_code,
    end
  )