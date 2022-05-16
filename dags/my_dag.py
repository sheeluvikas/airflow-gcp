""" This dag shows how to create a simple pipeline which uses BashOperator,
first task copies the data from one location to another, and another task, which checks if the file has recevied at the location.
if yes, then pass, otherwise fail"""

from airflow import DAG
from airflow.sensors.filesystem import FileSensor
from airflow.operators.bash import BashOperator
from airflow.models import Variable

from datetime import datetime, timedelta

EXEC_DATE = '{{ ds_nodash }}'


def create_dag(
        dag_ids,
        source_system_id):
    dag = DAG(dag_ids,
              start_date=datetime(2022, 5, 1),
              schedule_interval="@daily")
    with dag:
        check_file_task = FileSensor(
            task_id="check_file",
            poke_interval=5,
            filepath="/Users/vikas/app/tmp/abc.txt",
            timeout=550,
            dag=dag
        )

        mkdir_command = BashOperator(
            task_id="mkdirCommand",
            bash_command='mkdir -p /Users/vikas/app/tmp/' + EXEC_DATE + '/' + source_system_id
        )

        sleep_task = BashOperator(
            task_id="sleep_the_task",
            bash_command='echo "{{ ti.xcom_push(key="k1", value="vikas.txt") }}"; echo "This is an echo after pushing '
                         'xcom"',
            sla=timedelta(seconds=5)
        )

        create_file = BashOperator(
            task_id="createFile",
            bash_command='touch /Users/vikas/app/tmp/' + EXEC_DATE + '/' + source_system_id + '/"{{ ti.xcom_pull('
                                                                                              'key="k1") }}" '
        )
        check_file_task >> mkdir_command >> sleep_task >> create_file
        return dag


# build a dag for each source system

json_file = Variable.get('source_system_id', deserialize_json=True)

source_system_list = json_file['source_system_id']

for source_system_id in source_system_list:
    dag_id = "dynamic_dag_example_{}".format(str(source_system_id))
    globals()[dag_id] = create_dag(dag_id, source_system_id)

"""If there is a single task in the file, then we don't need to mention here "check_file_task", 
thats why its giving warning. We don't even need to assign any variable for the task. """
