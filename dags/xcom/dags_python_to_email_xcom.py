from __future__ import annotations

import random

import pendulum
from airflow.decorators import task
from airflow.models.dag import DAG
from airflow.operators.email import EmailOperator

with DAG(
        dag_id="dags_python_to_email_xcom",
        schedule="0 0 * * *",
        start_date=pendulum.datetime(year=2023, month=12, day=1, tz="Asia/Seoul"),
        catchup=False,
) as dag:
    @task(task_id="some_logic")
    def some_logic():
        return random.choice(['success', 'fail'])


    send_email = EmailOperator(
        task_id="send_email",
        to="sonyc5720@gmail.com",
        subject="처리결과: {{ data_interval_end.in_timezone('Asia/Seoul') | ds }}",
        html_content="{{ ti.xcom_pull(task_ids='some_logic') }}"
    )

    some_logic() >> send_email
