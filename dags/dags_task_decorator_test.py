from __future__ import annotations

import pendulum
from airflow.decorators import task
from airflow.models.dag import DAG

with DAG(
        dag_id="dags_task_decorator_test",
        schedule="0 0 * * *",
        start_date=pendulum.datetime(year=2023, month=12, day=1, tz="Asia/Seoul"),
        catchup=False,
) as dag:
    @task(task_id="print_input")
    def print_input(some_input):
        print(f"Input is {some_input}")


    print_input = print_input("@task 실행")

    print_input
