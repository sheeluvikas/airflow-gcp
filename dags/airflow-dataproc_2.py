from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateClusterOperator, ClusterGenerator, \
    DataprocDeleteClusterOperator, DataprocSubmitJobOperator
from airflow.providers.google.cloud.sensors.pubsub import PubSubPullSensor
from airflow.operators.python import PythonOperator
import time

PROJECT_ID = "imposing-ace-344215"
CLUSTER_NAME = "cluster-6bd4"
REGION = "us-central1"
ZONE = 'us-central1-c'

OUTPUT_FOLDER = "wordcount"

SPARK_JOB = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "spark_job": {
        "jar_file_uris": ["file:///usr/lib/spark/examples/jars/spark-examples.jar"],
        "main_class": "org.apache.spark.examples.SparkPi",
    },
}

default_args = {
    'sla': timedelta(seconds=10)
}

dag = DAG("airflow-dataproc", start_date=datetime(2021, 1, 1),
          schedule_interval="@daily", catchup=False, default_args=default_args)

CLUSTER_CONFIG = {
    "master_config": {
        "num_instances": 1,
        "machine_type_uri": "n1-standard-2",
        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 500},
    },
    "worker_config": {
        "num_instances": 0,
        "machine_type_uri": "n1-standard-2",
        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 500},
    },
}

config = ClusterGenerator(
    project_id=PROJECT_ID,
    num_masters=1,
    num_workers=0,
    master_machine_type='n1-standard-2',
    worker_machine_type='n1-standard-2',
    zone=ZONE
).make()


def my_custom_function(ts, **kwargs):
    print("task is sleeping")
    time.sleep(20)


sla_task = PythonOperator(
    task_id='sla_task',
    python_callable=my_custom_function,
    dag=dag
)

wait_pub_sub = PubSubPullSensor(
    task_id="wait_pub_sub",
    project_id=PROJECT_ID,
    subscription="db-pub-sub-topic-1-sub",
    return_immediately=True,
    ack_messages=False,
    gcp_conn_id='google_cloud_default'
)

create_cluster = DataprocCreateClusterOperator(
    task_id="create_cluster",
    project_id=PROJECT_ID,
    cluster_config=CLUSTER_CONFIG,
    region=REGION,
    cluster_name=CLUSTER_NAME,
    dag=dag,
    retries=3
)

spark_submit_task = DataprocSubmitJobOperator(
    task_id="spark_submit_task",
    job=SPARK_JOB,
    location=REGION,
    project_id=PROJECT_ID
)

delete_cluster = DataprocDeleteClusterOperator(
    task_id="delete_cluster",
    project_id=PROJECT_ID,
    region=REGION,
    cluster_name=CLUSTER_NAME,
    dag=dag
)

sla_task >> wait_pub_sub >> create_cluster >> spark_submit_task >> delete_cluster
