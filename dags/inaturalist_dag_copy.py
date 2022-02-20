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
from airflow.contrib.operators.emr_create_job_flow_operator import (
    EmrCreateJobFlowOperator,
)
from airflow.contrib.operators.emr_add_steps_operator import EmrAddStepsOperator
from airflow.contrib.sensors.emr_step_sensor import EmrStepSensor
from airflow.contrib.operators.emr_terminate_job_flow_operator import (
    EmrTerminateJobFlowOperator,
)

import sql_statements
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
BUCKET_NAME = "inaturalist-bucket"
OPEN_DATA_BUCKET = "inaturalist-open-data"
CONSOLIDATED_RESOURCE_BUCKET = "inaturalist-bucket"
AWS_CREDENTIALS_ID = 'aws_credentials'
EMR_DEFAULT = 'emr_default'
REDSHIFT_CONN_ID = 'redshift'
GOOGLE_SHEET_ID = "1V1PusD_2IY-Ztmp2FYqW3YNp2de83zXxKcnkkPhc4uY"

local_data = "dags/data/movie_review.csv"
s3_data = "data/movie_review.csv"
local_script = "dags/scripts/spark/random_text_classification.py"
s3_script = "scripts/random_text_classification.py"
s3_clean = "clean_data/"

JOB_FLOW_OVERRIDES = {
    "Name": "spark-capstone",
    "ReleaseLabel": "emr-5.29.0",
    "Applications": [{"Name": "Hadoop"}, {"Name": "Spark"}],  # We want our EMR cluster to have HDFS and Spark
    "Configurations": [
        {
            "Classification": "spark-env",
            "Configurations": [
                {
                    "Classification": "export",
                    "Properties": {"PYSPARK_PYTHON": "/usr/bin/python3"},  # by default EMR uses py2, change it to py3
                }
            ],
        }
    ],
    "Instances": {
        "InstanceGroups": [
            {
                "Name": "Master node",
                "Market": "SPOT",
                "InstanceRole": "MASTER",
                "InstanceType": "m5.xlarge",
                "InstanceCount": 1,
            },
            {
                "Name": "Core - 2",
                "Market": "SPOT",  # Spot instances are a "use as available" instances
                "InstanceRole": "CORE",
                "InstanceType": "m5.xlarge",
                "InstanceCount": 2,
            },
        ],
        "KeepJobFlowAliveWhenNoSteps": True,
        "TerminationProtected": False,  # this lets us programmatically terminate the cluster
    },
    "JobFlowRole": "EMR_EC2_DefaultRole",
    "ServiceRole": "EMR_DefaultRole",
}

SPARK_STEPS = [  # Note the params values are supplied to the operator
    {
        "Name": "Move raw data from S3 to HDFS",
        "ActionOnFailure": "CANCEL_AND_WAIT",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "s3-dist-cp",
                "--src=s3://{{ params.BUCKET_NAME }}/data",
                "--dest=/movie",
            ],
        },
    },
    {
        "Name": "Classify movie reviews",
        "ActionOnFailure": "CANCEL_AND_WAIT",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                "--deploy-mode",
                "client",
                "s3://{{ params.BUCKET_NAME }}/{{ params.s3_script }}",
            ],
        },
    },
    {
        "Name": "Move clean data from HDFS to S3",
        "ActionOnFailure": "CANCEL_AND_WAIT",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "s3-dist-cp",
                "--src=/output",
                "--dest=s3://{{ params.BUCKET_NAME }}/{{ params.s3_clean }}",
            ],
        },
    },
]


with DAG(
        "s3_data_copy",
        start_date=datetime(2021, 12, 1),
        schedule_interval="@monthly",
        max_active_runs=1,
        concurrency=2

) as dag:
    start_data_pipeline = DummyOperator(
        task_id="start_data_pipeline",
        dag=dag)

    create_emr_cluster = EmrCreateJobFlowOperator(
        task_id='create_emr_cluster',
        job_flow_overrides=JOB_FLOW_OVERRIDES,
        aws_conn_id=AWS_CREDENTIALS_ID,
        region_name="us-east-1",
        dag=dag
    )

    # Add your steps to the EMR cluster
    step_adder = EmrAddStepsOperator(
        task_id="add_steps",
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
        aws_conn_id=AWS_CREDENTIALS_ID,
        steps=SPARK_STEPS,
        params={  # these params are used to fill the paramterized values in SPARK_STEPS json
            "BUCKET_NAME": BUCKET_NAME,
            "s3_data": s3_data,
            "s3_script": s3_script,
            "s3_clean": s3_clean,
        },
        dag=dag,
    )

    last_step = len(SPARK_STEPS) - 1  # this value will let the sensor know the last step to watch
    # wait for the steps to complete

    step_checker = EmrStepSensor(
        task_id="watch_step",
        job_flow_id="{{ task_instance.xcom_pull('create_emr_cluster', key='return_value') }}",
        step_id="{{ task_instance.xcom_pull(task_ids='add_steps', key='return_value')["
                + str(last_step)
                + "] }}",
        aws_conn_id=AWS_CREDENTIALS_ID,
        dag=dag,
    )

    terminate_emr_cluster = EmrTerminateJobFlowOperator(
        task_id='terminate_emr_cluster',
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
        aws_conn_id=AWS_CREDENTIALS_ID,
        dag=dag
    )

    create_amphibian_table = PostgresOperator(
        task_id="create_amphibians_table",
        dag=dag,
        postgres_conn_id=REDSHIFT_CONN_ID,
        sql=sql_statements.CREATE_AMPHIBIAN_TABLE_SQL
    )

    create_species_dist_table = PostgresOperator(
        task_id="create_species_distribution_table",
        dag=dag,
        postgres_conn_id=REDSHIFT_CONN_ID,
        sql=sql_statements.CREATE_SPECIES_DIST_SQL
    )

    create_observers_table = PostgresOperator(
        task_id="create_observers_table",
        dag=dag,
        postgres_conn_id=REDSHIFT_CONN_ID,
        sql=sql_statements.CREATE_OBSERVERS_TABLE_SQL
    )

    create_observations_table = PostgresOperator(
        task_id="create_observations_table",
        dag=dag,
        postgres_conn_id=REDSHIFT_CONN_ID,
        sql=sql_statements.CREATE_OBSERVATIONS_TABLE_SQL
    )

    create_taxa_table = PostgresOperator(
        task_id="create_taxa_table",
        dag=dag,
        postgres_conn_id=REDSHIFT_CONN_ID,
        sql=sql_statements.CREATE_TAXA_TABLE_SQL
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

start_data_pipeline >> create_emr_cluster
create_emr_cluster >> step_adder >> step_checker >> terminate_emr_cluster
terminate_emr_cluster >> create_observers_table
create_observers_table >> copy_observers_data
create_taxa_table >> copy_taxa_data
create_observations_table >> copy_observations_data
create_amphibian_table >> copy_amphibian_data
create_species_dist_table >> copy_species_dist_data
copy_species_dist_data >> end_data_pipeline
