import getpass
from datetime import datetime

from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.utils.trigger_rule import TriggerRule

start_date = datetime(2020, 1, 1)

SCHEDULE_INTERVAL = 'schedule_interval'
START_DATE = 'start_date'
TABLE = 'table'

configs = {
    'process_first_table': {SCHEDULE_INTERVAL: '5 * * * *', START_DATE: start_date, TABLE: 'first'},
    'process_second_table': {SCHEDULE_INTERVAL: '@once', START_DATE: start_date, TABLE: 'second'},
    'process_third_table': {SCHEDULE_INTERVAL: None, START_DATE: None, TABLE: 'third'}
}

default_args = {
    'owner': 'adarapiyevich',
    'start_date': start_date
}


def log_dag_run_start(dag_id, table):
    print('{} start processing tables in database: {}'.format(dag_id, table))


def create_table(table_name):
    print('Creating table {}'.format(table_name))


def get_create_table_branch(table_name):
    hook = PostgresHook()
    query = hook.get_first(sql="SELECT 1 FROM information_schema.tables "
                               "WHERE table_schema='template0' AND table_name='{}';".format(table_name))
    if query:
        return 'skip_table_creation'
    else:
        return 'create_table'


RUN_ID_ENDED_KEY = 'run_id_ended'


def push_run_id(run_id, **kwargs):
    kwargs['ti'].xcom_push(key=RUN_ID_ENDED_KEY, value='{} ended'.format(run_id))


def push_current_user(**kwargs):
    kwargs['ti'].xcom_push(key='current_user', value=getpass.getuser())


LAST_TASK_ID = 'query_table'

for dag_id in configs:
    config = configs[dag_id]

    with DAG(dag_id=dag_id,
             start_date=config[START_DATE],
             schedule_interval=config[SCHEDULE_INTERVAL],
             default_args=default_args
             ) as dag:
        logging_task = PythonOperator(
            task_id='logging',
            python_callable=log_dag_run_start,
            op_kwargs={'dag_id': dag_id, 'table': config[TABLE]}
        )

        print_current_user_task = PythonOperator(
            task_id='print_current_user',
            python_callable=push_current_user,
            provide_context=True
        )

        create_table_fork = BranchPythonOperator(
            task_id='create_table_fork',
            python_callable=get_create_table_branch,
            op_args=[config[TABLE]]
        )

        create_table_task = PythonOperator(
            task_id='create_table',
            python_callable=create_table,
            op_kwargs={'table_name': config[TABLE]}
        )

        skip_table_creation = DummyOperator(task_id='skip_table_creation')

        insert_new_row_task = DummyOperator(task_id='insert_new_row', trigger_rule=TriggerRule.ALL_DONE)

        query_table_task = PythonOperator(
            task_id=LAST_TASK_ID,
            provide_context=True,
            python_callable=push_run_id,
            op_kwargs={'run_id': '{{ run_id }}'}
        )

        logging_task >> print_current_user_task >> create_table_fork
        create_table_fork >> [create_table_task, skip_table_creation] >> insert_new_row_task >> query_table_task
        globals()[dag_id] = dag
