from __future__ import annotations

import pendulum
from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator

with DAG(
        dag_id="dags_bash_with_macro_1",
        schedule="10 0 L * *",
        start_date=pendulum.datetime(year=2023, month=1, day=1, tz="Asia/Seoul"),
        catchup=False,
) as dag:
    t1 = BashOperator(
        task_id="t1",
        env={
            "START_DATE": "{{ data_interval_start.in_timezone('Asia/Seoul') }}",
            "END_DATE": "{{ data_interval_end.in_timezone('Asia/Seoul') - macros.dateutil.relativedelta.relativedelta(days=1) }}",
        },
        bash_command="""echo "START_DATE: $START_DATE" && echo "END_DATE: $END_DATE" """
    )
