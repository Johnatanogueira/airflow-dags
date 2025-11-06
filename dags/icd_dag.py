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


POD_NAME_PREFIX = 'airflow-dag-icd'

RUN_EKS = True
AWS_CREDENTIALS_ABSOLUTE_PATH=""

AIRFLOW_RUN_DETAILS = {
  "AIRFLOW": json.dumps({
    "DAG_ID" : "{{ task.dag_id }}",
    "TASK_ID"   : "{{ task.task_id }}",
    "RUN_ID" : "{{ run_id }}"
  })
}


ATHENA_TABLE_SCHEMA = 'temp_db'
ATHENA_TABLE_NAME = 'icd_codes_teste'
ATHENA_OUTPUT_TABLE_LOCATION = f's3://claims-management-data-lake/warehouse/{ATHENA_TABLE_SCHEMA}/{ATHENA_TABLE_NAME}/'
ATHENA_QUERY_OUTPUT_LOCATION = 's3://claims-management-athena-results/ClaimsManagementDataCatalog/'


ATHENA_ICD_DDL = f"""
  CREATE EXTERNAL TABLE IF NOT EXISTS `{ATHENA_TABLE_SCHEMA}.{ATHENA_TABLE_NAME}`(
    `code` string,
    `description` string
  )
  PARTITIONED BY (
    `code_type` string,
    `year` string
  )
  ROW FORMAT SERDE 
    'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
  STORED AS INPUTFORMAT 
    'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
  OUTPUTFORMAT 
    'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
  LOCATION
    '{ATHENA_OUTPUT_TABLE_LOCATION}'
  TBLPROPERTIES (
    'classification'='parquet', 
    'compressionType'='snappy', 
    'projection.enabled'='false', 
    'typeOfData'='file'
  )
"""

ICD_ENVIRONMENT_VARS={
  "ATHENA_TABLE_SCHEMA":ATHENA_TABLE_SCHEMA,
  "ATHENA_TABLE_NAME":ATHENA_TABLE_NAME,
  "ATHENA_OUTPUT_TABLE_LOCATION":ATHENA_OUTPUT_TABLE_LOCATION,
  "ATHENA_QUERY_OUTPUT_LOCATION":ATHENA_QUERY_OUTPUT_LOCATION,
  "YEAR": "{{ data_interval_end.year }}"
}


with DAG(
  dag_id            = 'icd_my',
  schedule_interval = timedelta(days=7),
  start_date        = datetime(2021, 1, 1, 3, 0),
  catchup           = True,
  max_active_runs   = 1,
  default_args      = {'retries': 3},
  tags              = ['athena', 'analytics', 'icd'],
) as dag:
  
  dag.doc_md = f'Current ECR Image: {ECR_CRAWLER_IMAGE}'
  
  start = EmptyOperator(
      task_id='start',
      wait_for_downstream=True,
  )
  
  # create_icd_code_table = PythonOperator(
  #   task_id             = 'create_icd_table',
  #   python_callable     = athena_query_execution_wr,
  #   op_kwargs           = {
  #     'sql': ATHENA_ICD_DDL,
  #     'database': 'temp_db',
  #     's3_output': ATHENA_QUERY_OUTPUT_LOCATION,
  #   }
  # )
  
  extract_icd_code = ContainerOperator_Single_Server(
    name="airflow-dag_icd",
    name_prefix                   = POD_NAME_PREFIX,
    cmds                          =["python", "/app/src/icd.py"],
    env_vars                      = {**ICD_ENVIRONMENT_VARS, **AIRFLOW_RUN_DETAILS},
    dag                           = dag,
    aws_credentials_absolute_path = AWS_CREDENTIALS_ABSOLUTE_PATH,
    image                         = ECR_CRAWLER_IMAGE,
    eks                           = RUN_EKS,
    retries                       = 3,
  )
  
  end = EmptyOperator(
    task_id             ='icd_end',
    wait_for_downstream =True,
    dag = dag
  )
  chain(
    start,
    extract_icd_code,
    end
  )