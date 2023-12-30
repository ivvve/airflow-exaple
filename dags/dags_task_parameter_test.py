from __future__ import annotations

import pendulum
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator

with DAG(
        dag_id="dags_task_parameter_test",
        schedule="0 0 * * *",
        start_date=pendulum.datetime(year=2023, month=12, day=1, tz="Asia/Seoul"),
        catchup=False,
) as dag:
    def print_input(some_input, *args, **kwargs):
        print(f"Input is {some_input}")
        print(f"args is {args}")
        print(f"kwargs is {kwargs}")


    t1 = PythonOperator(
        task_id="t1",
        python_callable=print_input,
        op_args=["input", "op_0", "op_2"],
        op_kwargs={
            "kwargs_a": "a",
            "kwargs_b": "b",
        }
    )

    t1
