from __future__ import annotations

from pprint import pprint

import pendulum
from airflow.decorators import task
from airflow.models.dag import DAG

with DAG(
        dag_id="dags_show_templates",
        schedule="30 0 * * *",
        start_date=pendulum.datetime(year=2023, month=12, day=1, tz="Asia/Seoul"),
        catchup=True,  # <==
) as dag:
    @task(task_id="show_templates")
    def show_templates(**kwargs):
        pprint(kwargs)


    show_templates()
