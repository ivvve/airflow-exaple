from __future__ import annotations

import pendulum
from airflow.decorators import task
from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator

with DAG(
        dag_id="dags_trigger_rule_none_skipped",
        schedule="0 0 * * *",
        start_date=pendulum.datetime(year=2023, month=12, day=1, tz="Asia/Seoul"),
        catchup=False,
) as dag:

    # return 값에 따라 실행되는 task가 정해진다
    # 실행되지 않는 task는 skip 처리된다
    @task.branch(task_id="random_branch")
    def random_branch():
        import random
        return random.choice(["task_a", "task_b", "task_c"])


    task_a = BashOperator(
        task_id="task_a",
        bash_command="echo task a"
    )


    @task(task_id="task_b")
    def task_b():
        print("task b")


    @task(task_id="task_c")
    def task_c():
        print("task c")


    @task(
        task_id="task_d",
        trigger_rule="none_skipped",  # skip 되는 task가 있기 때문에 실행되지 않을 예정
    )
    def task_d():
        print("task d")


    random_branch() >> [task_a, task_b(), task_c()] >> task_d()
