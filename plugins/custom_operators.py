from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class RowCountPostgresOperator(BaseOperator):

    @apply_defaults
    def __init__(self, table_name, *args, **kwargs):
        super(RowCountPostgresOperator, self).__init__(*args, **kwargs)
        self.table_name = table_name
        self.hook = PostgresHook()

    def execute(self, context):
        sql = 'SELECT COUNT(*) FROM {}'.format(self.table_name)
        rows_count = self.hook.get_first(sql=sql)[0]
        context['ti'].xcom_push(key='rows_count', value=rows_count)
