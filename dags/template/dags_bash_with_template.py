from __future__ import annotations

import pendulum
from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator

with DAG(
        dag_id="dags_bash_with_template",
        schedule="0 0 * * *",
        start_date=pendulum.datetime(year=2023, month=12, day=1, tz="Asia/Seoul"),
        catchup=False,
) as dag:
    bash_t1 = BashOperator(
        task_id="bash_t1",
        bash_command="""echo "data_interval_end: {{ data_interval_end }}" """,
    )

    bash_t2 = BashOperator(
        task_id="bash_t2",
        env={
            "START_DATE": "{{ data_interval_start | ds }}",  # | ds: pipe로 date를 YYYY-MM-DD 형태로 변환한다
            "END_DATE": "{{ data_interval_end | ds }}",
        },
        bash_command="""echo "data_interval_start: $START_DATE " && echo "data_interval_end: $END_DATE" """,
    )

    bash_t1 >> bash_t2
