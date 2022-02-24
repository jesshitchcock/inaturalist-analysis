from airflow import DAG
from datetime import datetime
import logging
from airflow.providers.postgres.hooks.postgres import PostgresHook

from scripts import create_staging_tables
# from airflow.operators.python import PythonOperator
# from airflow.providers.amazon.aws.transfers.google_api_to_s3 import GoogleApiToS3Operator
from airflow.operators.dummy import DummyOperator
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from operators.data_quality import DataQualityOperator
from botocore.config import Config

config = Config(
    region_name='us-east-1',
    signature_version='v4',
    retries={
        'max_attempts': 10,
        'mode': 'standard'
    }
)

OPEN_DATA_BUCKET = "inaturalist-open-data"
CONSOLIDATED_RESOURCE_BUCKET = "inaturalist-bucket"
AWS_CREDENTIALS_ID = 'aws_credentials'
REDSHIFT_CONN_ID = 'redshift_dev'

with DAG(
        "import_s3_dag",
        start_date=datetime(2021, 12, 1),
        schedule_interval="@monthly",
        max_active_runs=1,
        concurrency=2

) as dag:
    start_data_pipeline = DummyOperator(
        task_id="start_data_pipeline",
        dag=dag)

## Create schemas
    create_staging_schema = PostgresOperator(
        task_id="create_staging_schema",
        dag=dag,
        postgres_conn_id=REDSHIFT_CONN_ID,
        sql="CREATE SCHEMA IF NOT EXISTS staging AUTHORIZATION awsuser"
    )

    create_prod_schema = PostgresOperator(
        task_id="create_prod_schema",
        dag=dag,
        postgres_conn_id=REDSHIFT_CONN_ID,
        sql="CREATE SCHEMA IF NOT EXISTS production AUTHORIZATION awsuser"
    )


## Create Staging Table Tasks
    create_geospatial_table = PostgresOperator(
        task_id="create_species_geospatial_raw_table",
        dag=dag,
        postgres_conn_id=REDSHIFT_CONN_ID,
        sql=create_staging_tables.CREATE_GEOSPATIAL_SQL
    )

    create_observers_table = PostgresOperator(
        task_id="create_observers_raw_table",
        dag=dag,
        postgres_conn_id=REDSHIFT_CONN_ID,
        sql=create_staging_tables.CREATE_OBSERVERS_TABLE_SQL
    )

    create_observations_table = PostgresOperator(
        task_id="create_observations_raw__table",
        dag=dag,
        postgres_conn_id=REDSHIFT_CONN_ID,
        sql=create_staging_tables.CREATE_OBSERVATIONS_TABLE_SQL
    )

    create_taxa_table = PostgresOperator(
        task_id="create_taxa_raw_table",
        dag=dag,
        postgres_conn_id=REDSHIFT_CONN_ID,
        sql=create_staging_tables.CREATE_TAXA_TABLE_SQL
    )

## Copy Staging Data from S3 to Redshift
    copy_geospatial_data = S3ToRedshiftOperator(
        task_id='copy_geospatial_data_to_redshift',
        dag=dag,
        s3_bucket=CONSOLIDATED_RESOURCE_BUCKET,
        schema="staging",
        s3_key="amphibians/amphibians.shp",
        table="species_geospatial_raw",
        aws_conn_id=AWS_CREDENTIALS_ID,
        copy_options=["FORMAT SHAPEFILE SIMPLIFY AUTO"],
        method='REPLACE',
        redshift_conn_id=REDSHIFT_CONN_ID
    )

    copy_observers_data = S3ToRedshiftOperator(
        task_id='copy_observers_data_to_redshift',
        dag=dag,
        s3_bucket=OPEN_DATA_BUCKET,
        schema="staging",
        s3_key="observers",
        table="observers_raw",
        aws_conn_id=AWS_CREDENTIALS_ID,
        copy_options=["delimiter '\t' gzip IGNOREHEADER 1 ESCAPE"],
        method='REPLACE',
        redshift_conn_id=REDSHIFT_CONN_ID
    )

    copy_observations_data = S3ToRedshiftOperator(
        task_id='copy_observations_data_to_redshift',
        dag=dag,
        s3_bucket=OPEN_DATA_BUCKET,
        schema="staging",
        s3_key="observations",
        table="observations_raw",
        aws_conn_id=AWS_CREDENTIALS_ID,
        copy_options=[f"delimiter '\t' csv gzip IGNOREHEADER 1"],
        method='REPLACE',
        redshift_conn_id=REDSHIFT_CONN_ID
    )

    copy_taxa_data = S3ToRedshiftOperator(
        task_id='copy_taxa_data_to_redshift',
        dag=dag,
        s3_bucket=OPEN_DATA_BUCKET,
        schema="staging",
        s3_key="taxa",
        table="taxa_raw",
        aws_conn_id=AWS_CREDENTIALS_ID,
        copy_options=["delimiter '\t' gzip IGNOREHEADER 1 ESCAPE"],
        method='REPLACE',
        redshift_conn_id=REDSHIFT_CONN_ID
    )

## Data Quality Checks
    run_staging_quality_checks = DataQualityOperator(
        task_id='run_data_quality_checks',
        dag=dag,
        redshift_conn_id=REDSHIFT_CONN_ID,
        schema="staging",
        tables=['observers_raw', 'observations_raw', 'taxa_raw', 'species_geospatial_raw'])

    end_data_pipeline = DummyOperator(
        task_id="end_data_pipeline",
        dag=dag)

# Create staging tables in dev database
start_data_pipeline >> create_staging_schema
create_staging_schema >> [create_observers_table, create_taxa_table, create_observations_table, create_geospatial_table]
# Copy staging data from S3
create_observers_table >> copy_observers_data
create_taxa_table >> copy_taxa_data
create_observations_table >> copy_observations_data
create_geospatial_table >> copy_geospatial_data
# Run data quality checks
[copy_observers_data, copy_taxa_data, copy_observations_data, copy_geospatial_data] >> run_staging_quality_checks

run_staging_quality_checks >> create_prod_schema >>  end_data_pipeline

