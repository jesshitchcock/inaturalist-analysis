from airflow import DAG
from datetime import datetime
# from airflow.operators.python import PythonOperator
# from airflow.providers.amazon.aws.transfers.google_api_to_s3 import GoogleApiToS3Operator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.providers.postgres.hooks.postgres import PostgresHook

import create_tables
import boto3
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
EMR_DEFAULT = 'emr_default'
REDSHIFT_CONN_ID = 'redshift'
GOOGLE_SHEET_ID = "1V1PusD_2IY-Ztmp2FYqW3YNp2de83zXxKcnkkPhc4uY"

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

    create_amphibian_table = PostgresOperator(
        task_id="create_amphibians_table",
        dag=dag,
        postgres_conn_id=REDSHIFT_CONN_ID,
        sql=create_tables.CREATE_AMPHIBIAN_TABLE_SQL
    )

    create_species_dist_table = PostgresOperator(
        task_id="create_species_distribution_table",
        dag=dag,
        postgres_conn_id=REDSHIFT_CONN_ID,
        sql=create_tables.CREATE_SPECIES_DIST_SQL
    )

    create_observers_table = PostgresOperator(
        task_id="create_observers_table",
        dag=dag,
        postgres_conn_id=REDSHIFT_CONN_ID,
        sql=create_tables.CREATE_OBSERVERS_TABLE_SQL
    )

    create_observations_table = PostgresOperator(
        task_id="create_observations_table",
        dag=dag,
        postgres_conn_id=REDSHIFT_CONN_ID,
        sql=create_tables.CREATE_OBSERVATIONS_TABLE_SQL
    )

    create_taxa_table = PostgresOperator(
        task_id="create_taxa_table",
        dag=dag,
        postgres_conn_id=REDSHIFT_CONN_ID,
        sql=create_tables.CREATE_TAXA_TABLE_SQL
    )

    copy_amphibian_data = S3ToRedshiftOperator(
        task_id='transfer_s3_amphibian_data_to_redshift',
        dag=dag,
        s3_bucket=CONSOLIDATED_RESOURCE_BUCKET,
        schema="public",
        s3_key="iNat_Amphibian_Reference",
        table="staging_amphibian_reference",
        aws_conn_id=AWS_CREDENTIALS_ID,
        copy_options=["DELIMITER ',' CSV QUOTE AS '\"' IGNOREHEADER 1"],
        method='REPLACE',
        redshift_conn_id=REDSHIFT_CONN_ID
    )

    copy_species_dist_data = S3ToRedshiftOperator(
        task_id='transfer_s3_species_data_to_redshift',
        dag=dag,
        s3_bucket=CONSOLIDATED_RESOURCE_BUCKET,
        schema="public",
        s3_key="amphibians_v1/amphibians_v1.shp",
        table="staging_species_distribution",
        aws_conn_id=AWS_CREDENTIALS_ID,
        copy_options=["FORMAT SHAPEFILE SIMPLIFY AUTO"],
        method='REPLACE',
        redshift_conn_id=REDSHIFT_CONN_ID
    )

    copy_observers_data = S3ToRedshiftOperator(
        task_id='transfer_s3_observers_data_to_redshift',
        dag=dag,
        s3_bucket=OPEN_DATA_BUCKET,
        schema="public",
        s3_key="observers",
        table="staging_observers",
        aws_conn_id=AWS_CREDENTIALS_ID,
        copy_options=["delimiter '\t' gzip IGNOREHEADER 1 ESCAPE"],
        method='REPLACE',
        redshift_conn_id=REDSHIFT_CONN_ID
    )

    copy_observations_data = S3ToRedshiftOperator(
        task_id='transfer_s3_observations_data_to_redshift',
        dag=dag,
        s3_bucket=OPEN_DATA_BUCKET,
        schema="public",
        s3_key="observations",
        table="staging_observations",
        aws_conn_id=AWS_CREDENTIALS_ID,
        copy_options=[f"delimiter '\t' csv gzip IGNOREHEADER 1"],
        method='REPLACE',
        redshift_conn_id=REDSHIFT_CONN_ID
    )

    copy_taxa_data = S3ToRedshiftOperator(
        task_id='transfer_s3_taxa_data_to_redshift',
        dag=dag,
        s3_bucket=OPEN_DATA_BUCKET,
        schema="public",
        s3_key="taxa",
        table="staging_taxa",
        aws_conn_id=AWS_CREDENTIALS_ID,
        copy_options=["delimiter '\t' gzip IGNOREHEADER 1 ESCAPE"],
        method='REPLACE',
        redshift_conn_id=REDSHIFT_CONN_ID
    )

    end_data_pipeline = DummyOperator(
        task_id="end_data_pipeline",
        dag=dag)


start_data_pipeline >> [create_observers_table,
                        create_taxa_table,
                        create_observations_table,
                        create_amphibian_table,
                        create_species_dist_table]
create_observers_table >> copy_observers_data
create_taxa_table >> copy_taxa_data
create_observations_table >> copy_observations_data
create_amphibian_table >> copy_amphibian_data
create_species_dist_table >> copy_species_dist_data
[copy_observers_data, copy_taxa_data, copy_observations_data, copy_amphibian_data, copy_species_dist_data] >> end_data_pipeline
