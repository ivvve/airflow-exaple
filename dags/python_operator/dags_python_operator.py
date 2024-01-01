from __future__ import annotations

import random

import pendulum
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator

with DAG(
        dag_id="dags_python_operator",
        schedule="0 0 * * *",
        start_date=pendulum.datetime(year=2023, month=12, day=1, tz="Asia/Seoul"),
        catchup=False,
) as dag:
    def select_fruit():
        fruits = ["APPLE", "BANANA", "ORANGE", "AVOCADO"]
        fruit = random.choice(fruits)
        print(f"Chosen fruit is {fruit}")


    py_t1 = PythonOperator(
        task_id="py_t1",
        python_callable=select_fruit
    )

    py_t1
