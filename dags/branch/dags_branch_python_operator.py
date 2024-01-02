from __future__ import annotations

import pendulum
from airflow.models.dag import DAG
from airflow.operators.python import BranchPythonOperator, PythonOperator

with DAG(
        dag_id="dags_branch_python_operator",
        schedule="0 0 * * *",
        start_date=pendulum.datetime(year=2023, month=12, day=1, tz="Asia/Seoul"),
        catchup=False,
) as dag:
    task_a = PythonOperator(
        task_id="task_a",
        python_callable=lambda: print("a"),
    )
    task_b = PythonOperator(
        task_id="task_b",
        python_callable=lambda: print("b"),
    )
    task_c = PythonOperator(
        task_id="task_c",
        python_callable=lambda: print("c"),
    )


    def select_random():
        import random
        items = ["A", "B", "C"]
        selected_item = random.choice(items)

        if selected_item == "A":
            return "task_a"  # 후행 task의 ID를 리턴한다

        if selected_item in ["B", "C"]:
            return ["task_b", "task_c"]  # 후행 task의 ID를 리턴한다


    python_branch_task = BranchPythonOperator(
        task_id="python_branch_task",
        python_callable=select_random,
    )

    python_branch_task >> [task_a, task_b, task_c]
