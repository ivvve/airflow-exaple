from __future__ import annotations

import pendulum
from airflow.models.dag import DAG
from airflow.operators.email import EmailOperator

with DAG(
        dag_id="dags_email_operator",
        schedule="* * * * *",  # every minutes
        start_date=pendulum.datetime(year=2023, month=12, day=1, tz="Asia/Seoul"),
        catchup=False,
) as dag:
    send_email_task = EmailOperator(
        task_id="send_email_task",
        to="sonyc5720@gmail.com",
        subject="Airflow 처리결과",
        html_content="정상 처리되었습니다.<br/>"
    )
