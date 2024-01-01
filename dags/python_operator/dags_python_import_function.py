from __future__ import annotations

import pendulum
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from common.sftp_utils import get_sftp

with DAG(
        dag_id="dags_python_import_function",
        schedule="0 0 * * *",
        start_date=pendulum.datetime(year=2023, month=12, day=1, tz="Asia/Seoul"),
        catchup=False,
) as dag:
    py_t1 = PythonOperator(
        task_id="py_t1",
        python_callable=get_sftp
    )

    py_t1
