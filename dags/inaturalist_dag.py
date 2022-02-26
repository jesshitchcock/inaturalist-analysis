from datetime import datetime

from airflow import DAG
# from airflow.operators.python import PythonOperator
# from airflow.providers.amazon.aws.transfers.google_api_to_s3 import GoogleApiToS3Operator
from airflow.operators.dummy import DummyOperator
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from botocore.config import Config

from operators.data_quality import DataQualityOperator
from scripts import create_production_tables
from scripts import create_staging_tables
from scripts import transformation_scripts

config = Config(
    region_name='us-east-1',
    signature_version='v4',
    retries={
        'max_attempts': 20,
        'mode': 'standard'
    }
)

OPEN_DATA_BUCKET = "inaturalist-open-data"
CONSOLIDATED_RESOURCE_BUCKET = "inaturalist-bucket"
AWS_CREDENTIALS_ID = 'aws_credentials'
REDSHIFT_CONN_ID = 'redshift'

with DAG(
        "inaturalist_elt_dag",
        start_date=datetime(2022, 1, 1),
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
    create_geospatial_raw_table = PostgresOperator(
        task_id="create_species_geospatial_raw_table",
        dag=dag,
        postgres_conn_id=REDSHIFT_CONN_ID,
        sql=create_staging_tables.create_geospatial_raw_table_sql
    )

    create_observers_raw_table = PostgresOperator(
        task_id="create_observers_raw_table",
        dag=dag,
        postgres_conn_id=REDSHIFT_CONN_ID,
        sql=create_staging_tables.create_observers_raw_table_sql
    )

    create_observations_raw_table = PostgresOperator(
        task_id="create_observations_raw__table",
        dag=dag,
        postgres_conn_id=REDSHIFT_CONN_ID,
        sql=create_staging_tables.create_observations_raw_table_sql
    )

    create_taxa_raw_table = PostgresOperator(
        task_id="create_taxa_raw_table",
        dag=dag,
        postgres_conn_id=REDSHIFT_CONN_ID,
        sql=create_staging_tables.create_taxa_raw_table_sql
    )
## Create Production tables
    create_taxa_prod_table = PostgresOperator(
        task_id="create_taxa_table",
        dag=dag,
        postgres_conn_id=REDSHIFT_CONN_ID,
        sql=create_production_tables.create_taxa_prod_table_sql
    )

    create_geospatial_prod_table = PostgresOperator(
        task_id="create_geospatial_table",
        dag=dag,
        postgres_conn_id=REDSHIFT_CONN_ID,
        sql=create_production_tables.create_geospatial_prod_table_sql
    )

    create_observations_prod_table = PostgresOperator(
        task_id="create_observations_table",
        dag=dag,
        postgres_conn_id=REDSHIFT_CONN_ID,
        sql=create_production_tables.create_observations_prod_table_sql
    )

    create_observers_prod_table = PostgresOperator(
        task_id="create_observers_table",
        dag=dag,
        postgres_conn_id=REDSHIFT_CONN_ID,
        sql=create_production_tables.create_observers_prod_table_sql
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
        copy_options=[f"delimiter '\t' gzip IGNOREHEADER 1 ESCAPE"],
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

## Transform Staging and Data Load Prod tables
    transform_taxa_data = PostgresOperator(
        task_id="transform_and_load_taxa_to_prod",
        dag=dag,
        postgres_conn_id=REDSHIFT_CONN_ID,
        sql=transformation_scripts.taxa_transformation_sql
    )

    transform_geospatial_data = PostgresOperator(
        task_id="transform_and_load_geospatial_to_prod",
        dag=dag,
        postgres_conn_id=REDSHIFT_CONN_ID,
        sql=transformation_scripts.geospatial_transformation_sql
    )

    transform_observers_data = PostgresOperator(
        task_id="transform_and_load_observers_to_prod",
        dag=dag,
        postgres_conn_id=REDSHIFT_CONN_ID,
        sql=transformation_scripts.observers_transformation_sql
    )

    transform_observations_data = PostgresOperator(
        task_id="transform_and_load_observations_to_prod",
        dag=dag,
        postgres_conn_id=REDSHIFT_CONN_ID,
        sql=transformation_scripts.observations_transformation_sql
    )

## Data Quality Checks
    run_staging_quality_checks = DataQualityOperator(
        task_id='run_staging_data_quality_checks',
        dag=dag,
        redshift_conn_id=REDSHIFT_CONN_ID,
        schema="staging",
        tables=['observers_raw', 'observations_raw', 'taxa_raw', 'species_geospatial_raw'])

    run_prod_quality_checks = DataQualityOperator(
        task_id='run_prod_data_quality_checks',
        dag=dag,
        redshift_conn_id=REDSHIFT_CONN_ID,
        schema="production",
        tables=['observers', 'species_observations', 'taxa', 'species_geospatial'])

    end_data_pipeline = DummyOperator(
        task_id="end_data_pipeline",
        dag=dag)

# Create staging tables in dev database
start_data_pipeline >> create_staging_schema
create_staging_schema >> [create_observers_raw_table, create_taxa_raw_table, create_observations_raw_table, create_geospatial_raw_table]
# Copy staging data from S3
create_observers_raw_table >> copy_observers_data
create_taxa_raw_table >> copy_taxa_data
create_observations_raw_table >> copy_observations_data
create_geospatial_raw_table >> copy_geospatial_data
# Copy data to staging tables
# Run data quality checks
[copy_geospatial_data, copy_observations_data, copy_observers_data, copy_taxa_data] >> run_staging_quality_checks

# Create production schema and prod tables
# First transform the taxa table - it is referenced by the species_geospatial and species_observations queries
# Then transform and load the species_geospatial and species_observations
# Then load the observers table using the production.species_observations table
# Transform and Load other prod tables
run_staging_quality_checks >> create_prod_schema
create_prod_schema >> [create_taxa_prod_table, create_geospatial_prod_table, create_observations_prod_table, create_observers_prod_table] >> transform_taxa_data
transform_taxa_data >> [transform_geospatial_data, transform_observations_data] >> transform_observers_data >> run_prod_quality_checks
run_prod_quality_checks >> end_data_pipeline

