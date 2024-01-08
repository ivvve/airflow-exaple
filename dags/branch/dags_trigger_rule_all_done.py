from __future__ import annotations

import pendulum
from airflow import AirflowException
from airflow.decorators import task
from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator

with DAG(
        dag_id="dags_trigger_rule_all_done",
        schedule="0 0 * * *",
        start_date=pendulum.datetime(year=2023, month=12, day=1, tz="Asia/Seoul"),
        catchup=False,
) as dag:
    bash_upstream_1 = BashOperator(
        task_id="bash_upstream_1",
        bash_command="echo upstream 1"
    )


    # 실패하는 task
    @task(task_id="python_upstream_1")
    def python_upstream_1():
        raise AirflowException("python upstream 1 exception")


    @task(task_id="python_upstream_2")
    def python_upstream_2():
        print("python upstream 1")


    @task(
        task_id="python_downstream",
        trigger_rule="all_done",  # <== 실패하는 task가 있지만 실패 역시 실행된 것이므로 실행된다
    )
    def python_downstream():
        print("python downstream 1")


    [bash_upstream_1, python_upstream_1(), python_upstream_2()] >> python_downstream()
