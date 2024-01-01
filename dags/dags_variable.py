from __future__ import annotations

import pendulum
from airflow.models import Variable
from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator

with DAG(
        dag_id="dags_variable",
        schedule="0 0 * * *",
        start_date=pendulum.datetime(year=2023, month=12, day=1, tz="Asia/Seoul"),
        catchup=False,
) as dag:
    # 스케줄러가 DAG 파싱을 할 때 마다 DB에 variable을 조회하므로 권고하지 않는 방식
    name = Variable.get("name")
    bash_var_1 = BashOperator(
        task_id="bash_var_1",
        bash_command=f"echo {name}"
    )

    # Worker에서 runtime에 DB를 조회하기 때문에 스케줄러 부하 X: 권고되는 방식
    bash_var_2 = BashOperator(
        task_id="bash_var_2",
        bash_command="echo {{ var.value.name }}"
    )

    bash_var_1 >> bash_var_2
