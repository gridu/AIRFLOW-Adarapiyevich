import getpass
import random
from datetime import datetime

from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.utils.trigger_rule import TriggerRule

from airflow.operators.row_count_postgres import RowCountPostgresOperator

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


def get_create_table_branch(table_name):
    hook = PostgresHook()
    query_result = hook.get_first(sql="SELECT 1 FROM information_schema.tables "
                                      "WHERE table_catalog='airflow_gridu' AND table_name='{}';".format(table_name))
    if query_result is None:
        return 'create_table'
    else:
        return 'skip_table_creation'


RUN_ID_ENDED_KEY = 'run_id_ended'


def push_run_id(run_id, **kwargs):
    kwargs['ti'].xcom_push(key=RUN_ID_ENDED_KEY, value='{} ended'.format(run_id))


def push_current_user(**kwargs):
    kwargs['ti'].xcom_push(key='current_user', value=getpass.getuser())


LAST_TASK_ID = 'push_run_id'

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

        create_table_task = PostgresOperator(
            task_id='create_table',
            sql='''CREATE TABLE {}(
                custom_id integer NOT NULL,
                user_name VARCHAR (50) NOT NULL,
                timestamp TIMESTAMP NOT NULL);'''.format(config[TABLE])
        )

        skip_table_creation = DummyOperator(task_id='skip_table_creation')

        insert_new_row_task = PostgresOperator(
            task_id='insert_new_row',
            sql="INSERT INTO " + config[TABLE] +
                " VALUES (%s, '{{ ti.xcom_pull(key='current_user', task_ids='print_current_user') }}', %s)",
            parameters=(random.randint(-2147483648, 2147483647), datetime.now()),
            trigger_rule=TriggerRule.ALL_DONE
        )

        query_table_task = RowCountPostgresOperator(
            task_id='query_table',
            table_name=config[TABLE]
        )

        push_run_id_task = PythonOperator(
            task_id=LAST_TASK_ID,
            provide_context=True,
            python_callable=push_run_id,
            op_kwargs={'run_id': '{{ run_id }}'}
        )

        logging_task >> print_current_user_task >> create_table_fork
        create_table_fork >> [create_table_task, skip_table_creation] >> insert_new_row_task >> query_table_task
        query_table_task >> push_run_id_task
        globals()[dag_id] = dag
