from __future__ import annotations

import pendulum
from airflow.models.dag import DAG
from airflow.operators.python import task

with DAG(
        dag_id="dags_python_xcom_1",
        schedule="0 0 * * *",
        start_date=pendulum.datetime(year=2023, month=12, day=1, tz="Asia/Seoul"),
        catchup=False,
) as dag:
    @task(task_id="xcom_push_task_1")
    def xcom_push_task_1(**kwargs):
        ti = kwargs["ti"]
        ti.xcom_push(key="result1", value="value1")
        ti.xcom_push(key="result2", value=[1, 2, 3])


    @task(task_id="xcom_push_task_2")
    def xcom_push_task_2(**kwargs):
        ti = kwargs["ti"]
        ti.xcom_push(key="result1", value="value2")
        ti.xcom_push(key="result2", value=[-1, -2, -3])


    @task(task_id="xcom_pull_task")
    def xcom_pull_task(**kwargs):
        ti = kwargs["ti"]
        result1 = ti.xcom_pull(key="result1")
        result2 = ti.xcom_pull(key="result2", task_ids="xcom_push_task_1")

        print(f"result1: {result1}")
        print(f"result2 of xcom_push_task_1: {result2}")


    xcom_push_task_1() >> xcom_push_task_2() >> xcom_pull_task()
