import os

from airflow import DAG
from airflow.contrib.sensors.file_sensor import FileSensor
from airflow.models import Variable, XCom, DagRun
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.sensors import ExternalTaskSensor
from airflow.operators.subdag_operator import SubDagOperator
from airflow.utils.db import provide_session
from datetime import datetime
from sqlalchemy import func

import jobs_dag

trigger_file = Variable.get(key='trigger_file', default_var='/Users/adarapiyevich/Workspace/dag_trigger')
DAG_TO_TRIGGER = 'process_third_table'
FINISHED_TS_FILES_DIR = '/usr/local/airflow/shared/finished'

start_date = datetime(2020, 1, 1)

default_args = {
    'owner': 'adarapiyevich',
    'start_date': start_date
}


def create_process_results_sub_dag(parent_dag_id, parent_dag_start_date, dag_id_to_monitor):
    sub_dag_id = '{}.process_results'.format(parent_dag_id)
    latest_execution_date_key = 'latest_execution_date'
    push_triggered_dag_execution_date_task_id = 'push_triggered_dag_execution_date'

    @provide_session
    def push_triggered_dag_execution_date(session, **kwargs):
        query_result = session.query(func.max(DagRun.execution_date).label('latest_execution_date')) \
            .filter(DagRun.dag_id == dag_id_to_monitor) \
            .one()
        kwargs['ti'].xcom_push(key=latest_execution_date_key, value=query_result.latest_execution_date)

    def pull_triggered_dag_execution_date(self_dag_execution_date):
        return XCom.get_one(execution_date=self_dag_execution_date,
                            dag_id=sub_dag_id,
                            task_id=push_triggered_dag_execution_date_task_id,
                            key=latest_execution_date_key)

    def print_process_table_results(process_table_dag, **kwargs):
        message = kwargs['ti'].xcom_pull(
            task_ids=jobs_dag.LAST_TASK_ID,
            dag_id=process_table_dag,
            key=jobs_dag.RUN_ID_ENDED_KEY,
            include_prior_dates=True
        )
        print('Message: {}'.format(message))
        print('Execution context: {}'.format(kwargs))

    def create_file(dir_path, file_name):
        if not os.path.exists(dir_path):
            os.mkdir(dir_path)
        with open('{}/{}'.format(dir_path, file_name), 'w'):
            pass

    sub_dag = DAG(
        dag_id=sub_dag_id,
        start_date=parent_dag_start_date,
        default_args=default_args
    )
    with sub_dag:
        push_triggered_dag_execution_date_task = PythonOperator(
            task_id=push_triggered_dag_execution_date_task_id,
            provide_context=True,
            python_callable=push_triggered_dag_execution_date
        )

        wait_for_triggered_dag_completion_sensor = ExternalTaskSensor(
            task_id='wait_for_table_processing_completion',
            external_dag_id=dag_id_to_monitor,
            external_task_id=None,
            execution_date_fn=pull_triggered_dag_execution_date,
            poke_interval=10
        )

        print_results_task = PythonOperator(
            task_id='print_results',
            python_callable=print_process_table_results,
            op_kwargs={'process_table_dag': DAG_TO_TRIGGER},
            provide_context=True
        )

        remove_trigger_file_task = BashOperator(
            task_id='remove_trigger_file',
            bash_command='rm -f {}'.format(trigger_file)
        )

        create_timestamp_file = PythonOperator(
            task_id='create_finished_timestamp_file',
            python_callable=create_file,
            op_kwargs={'dir_path': FINISHED_TS_FILES_DIR, 'file_name': '{{ ts_nodash }}'}
        )

        push_triggered_dag_execution_date_task >> wait_for_triggered_dag_completion_sensor >> print_results_task
        print_results_task >> remove_trigger_file_task >> create_timestamp_file

    return sub_dag


with DAG(dag_id='trigger_table_update', schedule_interval=None, default_args=default_args) as dag:
    wait_for_file_sensor = FileSensor(
        task_id='wait_for_file',
        filepath=trigger_file,
        poke_interval=5
    )

    trigger_process_table_dag_task = TriggerDagRunOperator(
        task_id='trigger_process_table_dag',
        trigger_dag_id=DAG_TO_TRIGGER
    )

    process_results_task = SubDagOperator(
        task_id='process_results',
        subdag=create_process_results_sub_dag(
            parent_dag_id='trigger_table_update',
            parent_dag_start_date=None,
            dag_id_to_monitor=DAG_TO_TRIGGER
        )
    )

    wait_for_file_sensor >> trigger_process_table_dag_task >> process_results_task
