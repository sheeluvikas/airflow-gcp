from airflow import DAG
from datetime import datetime
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateClusterOperator, ClusterGenerator, \
    DataprocDeleteClusterOperator, DataprocSubmitJobOperator
from airflow.providers.google.cloud.sensors.pubsub import PubSubPullSensor
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor

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

dag = DAG("airflow-dataproc", start_date=datetime(2021, 1, 1),
          schedule_interval="@daily", catchup=False)

config = ClusterGenerator(
    project_id=PROJECT_ID,
    num_masters=1,
    num_workers=0,
    master_machine_type='n1-standard-2',
    worker_machine_type='n1-standard-2',
    zone=ZONE,
).make()

gcs_file_sensor = GCSObjectExistenceSensor(
    task_id='gcs_file_sensor',
    bucket='db-imposing-ace-test-bucket',
    google_cloud_conn_id='google_cloud_default',
    object='abc.txt',
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
    cluster_config=config,
    region=REGION,
    cluster_name=CLUSTER_NAME,
    dag=dag
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

gcs_file_sensor >> wait_pub_sub >> create_cluster >> spark_submit_task >> delete_cluster

if __name__ == "__main__":
    dag.cli()
