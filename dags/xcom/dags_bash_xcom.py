from __future__ import annotations

import pendulum
from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator

with DAG(
        dag_id="dags_bash_xcom",
        schedule="0 0 * * *",
        start_date=pendulum.datetime(year=2023, month=12, day=1, tz="Asia/Seoul"),
        catchup=False,
) as dag:
    bash_push_task = BashOperator(
        task_id="bash_push_task",
        bash_command="echo START && "
                     "echo XCOM_PUSHED "
                     "{{ ti.xcom_push(key='bash_pushed', value='first bash message') }} && "
                     "echo COMPLETE",
    )

    bash_pull_task = BashOperator(
        task_id="bash_pull_task",
        env={
            "PUSHED_VALUE": "{{ ti.xcom_pull(key='bash_pushed') }}",
            "RETURN_VALUE": "{{ ti.xcom_pull(task_ids='bash_push_task') }}",
        },
        bash_command="echo $PUSHED_VALUE && echo $RETURN_VALUE",
        do_xcom_push=False,
    )

    bash_push_task >> bash_pull_task
