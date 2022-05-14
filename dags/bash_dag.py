""" This dag shows how to create a simple pipeline which uses BashOperator,
first task copies the data from one location to another, and another task, which checks if the file has recevied at the location.
if yes, then pass, otherwise fail"""

from airflow import DAG
from airflow.sensors.filesystem import FileSensor
from airflow.operators.bash import BashOperator

from datetime import datetime, timedelta

EXEC_DATE = '{{ macros.ds_format(macros.ds_add(ds, 0), "%Y-%m-%d", "%Y%m%d") }}'
SOURCE_SYSTEM_ID = '{{ dag_run.conf["source_system_id"] }}'

""" Now create a DAG object """
dag = DAG("bash_operator_dag", start_date=datetime(2022, 5, 1), schedule_interval="@daily")

check_file_task = FileSensor(
    task_id="check_file",
    poke_interval=5,
    filepath="/Users/vikas/app/tmp/abc.txt",
    timeout=550,
    dag=dag
)

mkdirCommand = BashOperator(
    task_id="mkdirCommand",
    bash_command='mkdir -p /Users/vikas/app/tmp/' + EXEC_DATE + '/' + SOURCE_SYSTEM_ID
)

sleep_task = BashOperator(
    task_id="sleep_the_task",
    bash_command='echo "{{ ti.xcom_push(key="k1", value="vikas.txt") }}"; echo "This is an echo after pushing xcom"',
    sla=timedelta(seconds=5)
)

createFile = BashOperator(
    task_id="createFile",
    bash_command='touch /Users/vikas/app/tmp/'+EXEC_DATE + '/' + SOURCE_SYSTEM_ID+'/"{{ ti.xcom_pull(key="k1") }}"'
)

check_file_task >> mkdirCommand >> sleep_task >> createFile
"""If there is a single task in the file, then we don't need to mention here "check_file_task", 
thats why its giving warning. We don't even need to assign any variable for the task. """