from __future__ import annotations

import pendulum
from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator

with DAG(
        dag_id="dags_task_connection_test",  # 관리를 위해서 DAG id는 file명과 일치시키는게 좋다.
        schedule="* * * * *",
        start_date=pendulum.datetime(year=2023, month=12, day=1, tz="Asia/Seoul"),
        catchup=False,
) as dag:
    t1 = EmptyOperator(task_id="t1")
    t2 = EmptyOperator(task_id="t2")
    t3 = EmptyOperator(task_id="t3")
    t4 = EmptyOperator(task_id="t4")
    t5 = EmptyOperator(task_id="t5")
    t6 = EmptyOperator(task_id="t6")
    t7 = EmptyOperator(task_id="t7")
    t8 = EmptyOperator(task_id="t8")

    t1 >> [t2, t3] >> t4 >> t6 >> t8
    t5 >> t4
    t7 >> t6
