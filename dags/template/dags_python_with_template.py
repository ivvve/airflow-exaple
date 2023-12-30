from __future__ import annotations

import pendulum
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator, task

with DAG(
        dag_id="dags_python_with_template",
        schedule="30 9 * * *",
        start_date=pendulum.datetime(year=2023, month=12, day=1, tz="Asia/Seoul"),
        catchup=False,
) as dag:
    # kwargs의 Key와 동일한 이름의 파라미터가 있으면 매핑해준다
    def py_t1_func(start_date, end_date):
        print(f"data_interval_start: {start_date}")
        print(f"data_interval_end: {end_date}")


    py_t1 = PythonOperator(
        task_id="py_t1",
        python_callable=py_t1_func,
        op_kwargs={
            "start_date": "{{ data_interval_start | ds }}",
            "end_date": "{{ data_interval_end | ds }}",
        }
    )


    # kwargs에 template에 사용되는 값들이 있다.
    @task(task_id="py_t2")
    def py_t2_func(**kwargs):
        print(f'ds: {kwargs["ds"]}')
        print(f'ts: {kwargs["ts"]}')
        print(f'data_interval_start: {kwargs["data_interval_start"]}')
        print(f'data_interval_end: {kwargs["data_interval_end"]}')
        print(f'ti: {kwargs["ti"]}')


    py_t2 = py_t2_func()

    py_t1 >> py_t2
