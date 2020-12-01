import logging
import datetime

from airflow import models
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator


gcs_bucket = models.Variable.get('gcs_bucket')
project_id = models.Variable.get('gcp_project')
gce_zone = models.Variable.get("gce_zone")
gce_region = models.Variable.get('gce_region')

default_args = {
    'owner': 'airflow',
    'depends_on_past': True,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=1),
    'dagrun_timeout': datetime.timedelta(seconds=30)
}

with models.DAG(
    dag_id='gcs_to_bq_etl',
    start_date=datetime.datetime.now() - datetime.timedelta(minutes=10),
    schedule_interval='*/2 * * * *',
    default_args=default_args
) as dag:

    begin = DummyOperator(task_id='begin')

    user_etl = GoogleCloudStorageToBigQueryOperator(
        task_id='user_etl',
        bucket=gcs_bucket,
        source_format='NEWLINE_DELIMITED_JSON',
        source_objects=['data/test/*.ndjson'],
        destination_project_dataset_table='airflow.user',
        schema_fields=[
            {'name': 'user', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'timestamp', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'},
            {'name': 'evtname', 'type': 'STRING', 'mode': 'NULLABLE'}
        ],
        write_disposition='WRITE_TRUNCATE'
    )

    revenue_etl = GoogleCloudStorageToBigQueryOperator(
        task_id='revenue_etl',
        bucket=gcs_bucket,
        source_format='NEWLINE_DELIMITED_JSON',
        source_objects=['data/test/*.ndjson'],
        destination_project_dataset_table='airflow.revenue',
        schema_fields=[
            {'name': 'user', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'timestamp', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'},
            {'name': 'spend', 'type': 'NUMERIC', 'mode': 'NULLABLE'},
        ],
        write_disposition='WRITE_TRUNCATE'
    )

    revenue_total_etl = BigQueryOperator(
        task_id='revenue_total_etl',
        destination_dataset_table='airflow.revenue_total',
        sql="""
        SELECT user, SUM(spend) AS revenue_total
        FROM airflow.revenue
        GROUP BY user
        ORDER BY revenue_total DESC
        """,
        use_legacy_sql=False,
        write_disposition='WRITE_TRUNCATE'
    )

    end = DummyOperator(task_id='end')

begin >> [user_etl, revenue_etl] >> revenue_total_etl >> end
