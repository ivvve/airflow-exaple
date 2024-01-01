from __future__ import annotations

import pendulum
from airflow.models.dag import DAG
from airflow.operators.python import task

with DAG(
        dag_id="dags_python_xcom_2",
        schedule="0 0 * * *",
        start_date=pendulum.datetime(year=2023, month=12, day=1, tz="Asia/Seoul"),
        catchup=False,
) as dag:
    @task(task_id="xcom_push_task")
    def xcom_push_task(**kwargs):
        return "Success"


    @task(task_id="xcom_pull_task_1")
    def xcom_pull_task_1(**kwargs):
        ti = kwargs["ti"]
        result = ti.xcom_pull(task_ids="xcom_push_task")
        print(f"return value of xcom_push_task: {result}")


    @task(task_id="xcom_pull_task_2")
    def xcom_pull_task_2(result, **kwargs):
        print(f"return value from parameter {result}")


    xcom_push_task_instance = xcom_push_task()
    xcom_push_task_instance >> xcom_pull_task_1()
    xcom_pull_task_2(xcom_push_task_instance)
