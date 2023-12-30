from __future__ import annotations

import pendulum
from airflow.decorators import task
from airflow.models.dag import DAG

with DAG(
        dag_id="dags_python_with_macro",
        schedule="10 0 L * *",
        start_date=pendulum.datetime(year=2023, month=1, day=1, tz="Asia/Seoul"),
        catchup=False,
) as dag:
    @task(
        task_id="get_datetime",
        templates_dict={
            "start_date": "{{ (data_interval_end.in_timezone('Asia/Seoul') - macros.dateutil.relativedelta.relativedelta(months=-1, day=1) ) | ds }}",
            "end_date": "{{ (data_interval_end.in_timezone('Asia/Seoul').replace(day=1) - macros.dateutil.relativedelta.relativedelta(days=-1) ) | ds }}",
        },
    )
    def get_datetime(**kwargs):
        templates_dict = kwargs["templates_dict"]
        print(f"start_date: {templates_dict['start_date']}")
        print(f"end_date: {templates_dict['end_date']}")


    get_datetime()
