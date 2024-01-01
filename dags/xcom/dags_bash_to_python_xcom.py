from __future__ import annotations

import pendulum
from airflow.decorators import task
from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator

with DAG(
        dag_id="dags_bash_to_python_xcom",
        schedule="0 0 * * *",
        start_date=pendulum.datetime(year=2023, month=12, day=1, tz="Asia/Seoul"),
        catchup=False,
) as dag:
    bash_push_task = BashOperator(
        task_id="bash_push_task",
        bash_command="echo START "
                     "{{ ti.xcom_push(key='bash_pushed', value=200) }} && "
                     "echo COMPLETE",
    )


    @task(task_id="python_pull_task")
    def python_pull_task(**kwargs):
        ti = kwargs["ti"]
        status_value = ti.xcom_pull(key="bash_pushed")
        return_value = ti.xcom_pull(task_ids="bash_push_task")
        print(f"status: {status_value}")
        print(f"return: {return_value}")


    bash_push_task >> python_pull_task()
