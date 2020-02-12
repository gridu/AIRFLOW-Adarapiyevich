from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime

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
    return '{} start processing tables in database: {}'.format(dag_id, table)


def create_table(table_name):
    print('Creating table {}'.format(table_name))


def get_create_table_branch():
    if True:
        return 'skip_table_creation'
    else:
        return 'create_table'


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

        create_table_fork = BranchPythonOperator(
            task_id='create_table_fork',
            python_callable=get_create_table_branch
        )

        create_table_task = PythonOperator(
            task_id='create_table',
            python_callable=create_table,
            op_kwargs={'table_name': config[TABLE]}
        )

        skip_table_creation = DummyOperator(task_id='skip_table_creation')

        insert_new_row_task = DummyOperator(task_id='insert_new_row', trigger_rule=TriggerRule.ALL_DONE)

        query_table_task = DummyOperator(task_id='query_table')

        logging_task >> create_table_fork >> [create_table_task,
                                              skip_table_creation] >> insert_new_row_task >> query_table_task
        globals()[dag_id] = dag
