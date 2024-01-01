from __future__ import annotations

import pendulum
from airflow.decorators import task
from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator

with DAG(
        dag_id="dags_python_to_bash_xcom",
        schedule="0 0 * * *",
        start_date=pendulum.datetime(year=2023, month=12, day=1, tz="Asia/Seoul"),
        catchup=False,
) as dag:
    @task(task_id="python_push_task")
    def python_push_task():
        return {
            "status": "Good",
            "data": [1, 2, 3],
            "count": 100,
        }


    bash_pull_task = BashOperator(
        task_id="bash_pull_task",
        env={
            "STATUS": "{{ ti.xcom_pull(task_ids='python_push_task')['status'] }}",
            "DATA": "{{ ti.xcom_pull(task_ids='python_push_task')['data'] }}",
            "COUNT": "{{ ti.xcom_pull(task_ids='python_push_task')['count'] }}",
        },
        bash_command="echo $STATUS && echo $DATA && echo $COUNT",
        do_xcom_push=False,
    )

    python_push_task() >> bash_pull_task
