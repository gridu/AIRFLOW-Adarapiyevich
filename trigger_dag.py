from airflow import DAG
from airflow.contrib.sensors.file_sensor import FileSensor
from airflow.models import Variable
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from datetime import datetime

trigger_file = Variable.get(key='trigger_file', default_var='/Users/adarapiyevich/Workspace/dag_trigger')
TRIGGER_DAG_ID = 'process_third_table'

start_date = datetime(2020, 1, 1)

default_args = {
    'owner': 'adarapiyevich',
    'start_date': start_date
}

with DAG(dag_id='trigger_table_update', schedule_interval=None, default_args=default_args) as dag:
    wait_for_file_sensor = FileSensor(
        task_id='wait_for_file',
        filepath=trigger_file,
        poke_interval=5
    )

    trigger_process_table_dag_task = TriggerDagRunOperator(
        task_id='trigger_process_table_dag',
        trigger_dag_id=TRIGGER_DAG_ID
    )

    remove_trigger_file_task = BashOperator(
        task_id='remove_trigger_file',
        bash_command='rm -f {}'.format(trigger_file)
    )

    wait_for_file_sensor >> trigger_process_table_dag_task >> remove_trigger_file_task
