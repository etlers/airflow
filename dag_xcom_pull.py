from datetime import timedelta
import datetime

import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

default_args = {
    "owner": "etlers",
    "depends_on_past": False,
    'start_date': airflow.utils.dates.days_ago(2),
    "provide_context":True,
}

dag = DAG("xcom_pull_test",
        default_args=default_args,
        schedule_interval="@once",
        )


def pull_function(**context):
    value = context['ti'].xcom_pull(dag_id="xcom_tutorial", task_ids='push_info')
    print("context['ti'].xcom_pull:", value)



# pull
pull_1 = PythonOperator(
    task_id='pull_info_1',
    python_callable=pull_function,
    dag=dag,
)

pull_1