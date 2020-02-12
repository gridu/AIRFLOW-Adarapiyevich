from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
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

        insert_new_row_task = DummyOperator(task_id='insert_new_row')

        query_table_task = DummyOperator(task_id='query_table')

        logging_task >> insert_new_row_task >> query_table_task
        globals()[dag_id] = dag
