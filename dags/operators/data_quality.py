from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import BaseOperator


class DataQualityOperator(BaseOperator):
    """This operator takes a list of tables and performs data quality checks.
    The first check makes sure the tables contain data, and the second chack makes sure that there
    are no NULL values in the primary key fields for each table."""
    ui_color = '#9999FF'

    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 redshift_conn_id='',
                 tables=[],
                 schema='',
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tables = tables
        self.schema = schema

        # Map params here
        # Example:
        # self.conn_id = conn_id

    def execute(self, context):
        self.log.info('Starting Data Quality Checks')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info('Check 1: Determine whether tables contain data')
        for table in self.tables:

            # check for records in table
            records = redshift.get_records(f'SELECT COUNT(*) FROM {self.schema}.{table}')
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(f'Data quality chack failed. {table} returned no results.')
            else:
                self.log.info(
                    f'Data quality check passed. The {table} table is not empty and contains {str(records[0][0])} records.')

        self.log.info('Check 2: Ensure that tables do not contain NULL values in the primary key field.')
        for table in self.tables:
            p_key = redshift.get_records(f"""
            select 
                   kcu.column_name as key_column
            from information_schema.table_constraints tco
            join information_schema.key_column_usage kcu 
                 on kcu.constraint_name = tco.constraint_name
                 and kcu.constraint_schema = tco.constraint_schema
                 and kcu.constraint_name = tco.constraint_name
            where 
                tco.constraint_type = 'PRIMARY KEY'  
                and kcu.table_name = '{table}'
                and kcu.constraint_schema = '{self.schema}'
            order by tco.constraint_schema,
                     tco.constraint_name,
                     kcu.ordinal_position""")

            records = redshift.get_records(f"SELECT COUNT(*) FROM {self.schema}.{table} WHERE {p_key[0][0]} IS NULL")
            if int(records[0][0]) == 0:
                self.log.info(f'Data quality check on primary key {p_key[0][0]} passed for the {table} table.')
            else:
                raise ValueError(
                    f'Data quality check failed. There are {str(records[0][0])} records in the {self.schema}.{table} table that have NULL values in the {p_key[0][0]} primary key field.')