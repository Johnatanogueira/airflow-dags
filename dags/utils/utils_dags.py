import os
import boto3
import redshift_connector
import awswrangler as wr
from datetime import datetime

client_s3 = boto3.client('s3')

def list_truncate_object(bucket, delimiter, prefix, continuation_token=None, data=[]):
    objects = None
    if (continuation_token != None):
        objects = client_s3.list_objects_v2(
            Bucket = bucket,
            Delimiter = delimiter,
            Prefix =  prefix,
            ContinuationToken = continuation_token
        )
    else:
        objects = client_s3.list_objects_v2(
            Bucket = bucket,
            Delimiter = delimiter,
            Prefix =  prefix
        )

    data = data + objects.get('Contents', [])

    if (objects.get('IsTruncated',False) == True):
        return list_truncate_object(bucket, delimiter, prefix, objects['NextContinuationToken'], data)
    
    return list(map(lambda o: { 'Key': o['Key'] }, data))

def delete_objects(bucket, objects='[]'):
    objs = eval(objects)
    if (not len(objs)): return False
    response = client_s3.delete_objects(
        Bucket=bucket,
        Delete={
            'Objects': objs
        }
    )
    return True

def delete_objects_wr(path):
    wr.s3.delete_objects(path)

def copy_objects_wr(source_path, destination_path, remove_after=False):
    objects = wr.s3.list_objects(source_path)
    wr.s3.copy_objects(
        paths=objects,
        source_path=source_path,
        target_path=destination_path
    )

    if remove_after:
        wr.s3.delete_objects(objects)

def copy_objects_template_file_name_wr(source_path, destination_path, remove_after=False, template="LF-{TIMESTAMP}.txt"):
    objects = wr.s3.list_objects(source_path)
    replace_map: dict[str,str] = {}
    for obj in objects:    
        basename = os.path.basename(obj)
        name, ext = os.path.splitext(basename)
        ts = datetime.now().strftime("%Y%m%d%H%M%S%f")[:-3]
        new_name = template.replace("{TIMESTAMP}", ts)
        replace_map[basename] = new_name
        
    wr.s3.copy_objects(
        paths=objects,
        source_path=source_path,
        target_path=destination_path,
        replace_filenames=replace_map
    )

def copy_s3_to_redshift(redshift_db, redshift_db_user, redshift_cluster, redshift_profile, redshift_iam_role, redshift_target_table, bucket_from, delimiter, format='CSV'):
    with redshift_connector.connect(
        iam=True,
        database=redshift_db,
        db_user=redshift_db_user,
        password='',
        user='',
        cluster_identifier=redshift_cluster,
        profile=redshift_profile
    ) as conn:
        with conn.cursor() as cursor:
            cursor.execute(f"""
                COPY {redshift_target_table}
                FROM '{bucket_from}'
                iam_role '{redshift_iam_role}'
                format {format}
                delimiter '{delimiter}';
            """)
            conn.commit()

def copy_s3_to_redshift_no_csv(redshift_db, redshift_db_user, redshift_cluster, redshift_profile, redshift_iam_role, redshift_target_table, bucket_from, format='CSV'):
    with redshift_connector.connect(
        iam=True,
        database=redshift_db,
        db_user=redshift_db_user,
        password='',
        user='',
        cluster_identifier=redshift_cluster,
        profile=redshift_profile
    ) as conn:
        with conn.cursor() as cursor:
            cursor.execute(f"""
                COPY {redshift_target_table}
                FROM '{bucket_from}'
                iam_role '{redshift_iam_role}'
                format {format};
            """)
            conn.commit()

def redshift_iam_query(redshift_db, redshift_db_user, redshift_cluster, redshift_profile, redshift_iam_role, query):
    with redshift_connector.connect(
        iam=True,
        database=redshift_db,
        db_user=redshift_db_user,
        password='',
        user='',
        cluster_identifier=redshift_cluster,
        profile=redshift_profile
    ) as conn:
        with conn.cursor() as cursor:
            cursor.execute(query)
            conn.commit()

def postgres_read_sql_query_wr(secret_id, sql):
    with wr.postgresql.connect(secret_id=secret_id) as con:
        return wr.postgresql.read_sql_query(sql=sql, con=con)

def athena_query_wr(sql, database = 'temp_db', s3_output = 's3://claims-management-athena-results/ClaimsManagementDataCatalog'):
    return wr.athena.read_sql_query(sql=sql, database=database, s3_output=s3_output)

def athena_query_execution_wr(sql, database = 'temp_db', s3_output = 's3://claims-management-athena-results/ClaimsManagementDataCatalog', wait=True):
    print(sql)
    wr.athena.start_query_execution(
        sql=sql,
        database=database,
        s3_output=s3_output,
        wait=wait
    )

def athena_multi_query_execution_wr(queries, database, s3_output):
    for sql in queries:
        athena_query_execution_wr(sql, database, s3_output)

def athena_create_ctas_table_wr(sql, database, ctas_table_name, ctas_table_path, s3_output, storage_format="CSV", schema_only=False):
    wr.catalog.delete_table_if_exists(database=database, table=ctas_table_name)
    wr.s3.delete_objects(ctas_table_path)
    wr.athena.create_ctas_table(
        sql=sql,
        database=database,
        ctas_table=ctas_table_name,
        s3_output=s3_output,
        storage_format=storage_format,
        schema_only=schema_only,
        wait=True
    )
    return True

def athena_insert_partition_wr(sql, table, path_partition, partitions_values, database, s3_output, wait=True):
    delete_objects_wr(path_partition)
    wr.catalog.delete_partitions(
        table=table,
        database=database,
        partitions_values=partitions_values
    )
    wr.athena.start_query_execution(
        sql=sql,
        database=database,
        s3_output=s3_output,
        wait=True
    )
    return True

def athena_execute_from_file(file_path, **file_kwargs):
    sql = \
        open(file_path,'r') \
        .read() \
        .format( **file_kwargs )

    wr.athena.start_query_execution(
        sql= sql,
        database='temp_db',
        s3_output='s3://claims-management-athena-results/ClaimsManagementDataCatalog',
        wait=True
    )


def athena_purge_table(database, table):
    
    wr.athena.start_query_execution(
        sql =   f"DROP TABLE IF EXISTS {database}.{table}",
        database =  'temp_db',
        s3_output =  's3://claims-management-athena-results/ClaimsManagementDataCatalog',
        wait =  True
    )
    
    athena_output_table_location:str = f's3://claims-management-data-lake/warehouse/{database}/{table}'
    wr.s3.delete_objects(path=athena_output_table_location)