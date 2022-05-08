from airflow import DAG
from datetime import datetime
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateClusterOperator, ClusterGenerator, \
    DataprocDeleteClusterOperator, DataprocSubmitJobOperator

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
    project_id="imposing-ace-344215",
    num_masters=1,
    num_workers=0,
    master_machine_type='n1-standard-2',
    worker_machine_type='n1-standard-2',
    zone='us-central1-c',
).make()

create_cluster = DataprocCreateClusterOperator(
    task_id="create_cluster",
    project_id="imposing-ace-344215",
    cluster_config=config,
    region="us-central1",
    cluster_name="cluster-6bd4",
    dag=dag
)

spark_task = DataprocSubmitJobOperator(
    task_id="spark_task", job=SPARK_JOB, location=REGION, project_id=PROJECT_ID
)

delete_cluster = DataprocDeleteClusterOperator(
    task_id="delete_cluster",
    project_id="imposing-ace-344215",
    cluster_config=config,
    region="us-central1",
    cluster_name="cluster-6bd4",
    dag=dag
)

create_cluster >> spark_task >> delete_cluster

if __name__ == "__main__":
    dag.cli()
