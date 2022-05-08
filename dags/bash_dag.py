""" This dag shows how to create a simple pipeline which uses BashOperator,
first task copies the data from one location to another, and another task, which checks if the file has recevied at the location.
if yes, then pass, otherwise fail"""

from airflow import DAG
from airflow.sensors.filesystem import FileSensor

from datetime import datetime

""" Now create a DAG object """
dag = DAG("bash_perator_dag", start_date=datetime(2022, 5, 1), schedule_interval="*/5 * * * *")

check_file_task = FileSensor(
    task_id="check_file",
    poke_interval=5,
    filepath="/Users/vikas/app/tmp/abc.txt",
    timeout=550,
    dag=dag
)

check_file_task
"""If there is a single task in the file, then we don't need to mention here "check_file_task", 
thats why its giving warning. We don't even need to assign any varible for the task. """