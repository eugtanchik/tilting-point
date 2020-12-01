import os
import datetime
import ndjson
import random
import random_timestamp

from airflow import models
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator


gcs_bucket = models.Variable.get('gcs_bucket')
project_id = models.Variable.get('gcp_project')
gce_zone = models.Variable.get("gce_zone")
gce_region = models.Variable.get('gce_region')


def generate_nd_json(*args, **kwargs):
    item_number = 10
    users = ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H']
    events = ['tutorial', 'iap']
    items = [
        {
            'user': random.choice(users),
            'timestamp': str(random_timestamp.random_timestamp(2020)),
            'evtname': random.choice(events),
            'spend': round(random.random(), 2)
        }
        for i in range(item_number)
    ]
    # dump to file-like objects
    with open(os.path.join(
            '/home/airflow/gcs',
            'data',
            'test',
            f"{kwargs['execution_date']}.ndjson"
    ), 'w') as f:
        ndjson.dump(items, f)


with models.DAG(
    dag_id='ndjson_to_gcs',
    start_date=datetime.datetime.now() - datetime.timedelta(minutes=10),
    schedule_interval='*/2 * * * *',
) as dag:

    begin = DummyOperator(task_id='begin')
    generate_ndjson_task = PythonOperator(
        task_id='generate_ndjson_task',
        python_callable=generate_nd_json,
        provide_context=True
    )
    end = DummyOperator(task_id='end')

begin >> generate_ndjson_task >> end
