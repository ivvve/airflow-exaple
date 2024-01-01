from __future__ import annotations

import datetime

import pendulum
from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator

with DAG(
        dag_id="dags_bash_operator",  # 관리를 위해서 DAG id는 file명과 일치시키는게 좋다.
        schedule="* * * * *",  # every minutes
        start_date=pendulum.datetime(year=2023, month=12, day=1, tz="Asia/Seoul"),
        # start_date와 현재 사이에 돌았어야할 DAG를 실행시킬지 여부
        # catchup은 차례차례 돌지 않고 한번에 실행된다.
        catchup=False,
        dagrun_timeout=datetime.timedelta(minutes=60),
        tags=["example", "example2"],  # DAG에 대한 metadata (DAG 실행 자체에는 영향을 주지 않음)
        params={"name": "Chris"},  # DAG 실행 시 사용할 파라미터
) as dag:
    bash_task_1 = BashOperator(
        task_id="bash_task_1",  # instance 명과 동일하게 주는게 관리 상 좋다
        bash_command="echo Who am I?",
    )

    bash_task_2 = BashOperator(
        task_id="bash_task_2",
        bash_command="echo $HOSTNAME",
    )

    # DAG task 실행 순서
    bash_task_1 >> bash_task_2
