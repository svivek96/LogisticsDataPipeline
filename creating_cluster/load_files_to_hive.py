from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.sensors.gcs import GCSObjectsWithPrefixExistenceSensor
from airflow.providers.google.cloud.operators.dataproc import ( DataprocCreateClusterOperator,DataprocDeleteClusterOperator,DataprocSubmitHiveJobOperator )
from airflow.operators.bash_operator import BashOperator


default_args = {
    'owner':'airflow',
    'depends_on_past':False,
    'email_on_failure':False,
    'email_on_retry':False,
    'retries':1,
    'retry_delay':timedelta(minutes=1)
}

dag = DAG(
    'loading_csv_files_to_hive_tables',
    default_args =default_args,
    schedule_interval = timedelta(days=1),
    start_date=datetime(2024,1,28),
    description = 'loading files from gcs to hive tables',
    tags = ['Project-3'],
)

file_scan_task = GCSObjectsWithPrefixExistenceSensor(
    task_id='sense_logistics_file',
    bucket='logistics_files',
    prefix='input_files/logistics_',
    google_cloud_conn_id='google_cloud_default',
    mode='poke',
    poke_interval=30,
    timeout=300,
    dag=dag,
)

PROJECT_ID='pyspark-learning-407410'
CLUSTER_NAME='Airflow-Cluster'
REGION='us-central1'
CLUSTER_CONFIG = {
    "master_config": {
        "num_instances": 1,
        "machine_type_uri": "n1-standard-4",
        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 30},
    },
    "worker_config": {
        "num_instances": 2,
        "machine_type_uri": "n1-standard-4",
        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 30},
    },
    "software_config":{
        "image_version":"2.1-debian11"}
}


create_cluster = DataprocCreateClusterOperator(
    task_id="create_cluster",
    project_id=PROJECT_ID,
    cluster_config=CLUSTER_CONFIG,
    region=REGION,
    cluster_name=CLUSTER_NAME,
    dag=dag,
)

create_hive_database = DataprocSubmitHiveJobOperator(
    task_id='create_hive_database',
    query='CREATE DATABASE IF NOT EXISTS logistics_db;',
    cluster_name=CLUSTER_NAME,
    region=REGION,
    dag=dag,
)

create_hive_tables = DataprocSubmitHiveJobOperator(
    task_id='create_hive_tables',
    query="""CREATE EXTERNAL TABLE IF NOT EXISTS logistics_db.logistics_data(
            delivery_id INT,
            `date` STRING,
            origin STRING,
            destination STRING,
            vehicle_type STRING,
            delivery_status STRING,
            delivery_time STRING)
            ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
            STORED AS TEXTFILE
            LOCATION 'gs://logistics_files/input_files/'
            tblproperties('skip.header.line.count'='1') """,
    cluster_name=CLUSTER_NAME,
    region=REGION,
    dag=dag,
)

create_hive_partition_tables = DataprocSubmitHiveJobOperator(
    task_id='create_hive_partition_tables',
    query="""CREATE TABLE IF NOT EXISTS logistics_db.logistics_data_par(
            delivery_id INT,
            origin STRING,
            destination STRING,
            vehicle_type STRING,
            delivery_status STRING,
            delivery_time STRING)
            PARTITIONED BY (`date` STRING)
            STORED AS TEXTFILE ;""",
    cluster_name=CLUSTER_NAME,
    region=REGION,
    dag=dag,
)

load_hive_partition_tables = DataprocSubmitHiveJobOperator(
    task_id='load_hive_partition_tables',
    query="""SET hive.exec.dynamic.partition=true;
             SET hive.exec.dynamic.partition.mode=nonstrict;
             
             INSERT INTO logistics_db.logistics_data_par partition(`date`)
             SELECT delivery_id, origin,destination, vehicle_type, delivery_status, delivery_time, `date` FROM logistics_db.logistics_data;
             """,
    cluster_name=CLUSTER_NAME,
    region=REGION,
    dag=dag,
)

deleting_cluster = DataprocDeleteClusterOperator(
    task_id="delete_cluster",
    project_id=PROJECT_ID,
    cluster_name=CLUSTER_NAME,
    region=REGION,
    dag=dag,
    )

moving_files = BashOperator(
            task_id='moving_files_to_archive',
            bash_command="gsutil -m mv 'gs://logistics_files/input_files/logistics_*.csv' 'gs://logistics_files/archive_files/' ",
            dag=dag
        )

file_scan_task >> create_cluster >> create_hive_database >> create_hive_tables >> create_hive_partition_tables >> load_hive_partition_tables >> deleting_cluster >>moving_files