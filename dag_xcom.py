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

dag = DAG("xcom_tutorial",
        default_args=default_args,
        schedule_interval="@once",
        )

def push_function(**context):
    return 'xcom_test'

def push_by_xcom_push(**context):
    context['task_instance'].xcom_push(key='pushed_value', value='xcom_push')

def pull_function(**context):
    value = context['ti'].xcom_pull(task_ids='push_info')
    print("pull:", value)


# push
push_info = PythonOperator(
    task_id='push_info',
    python_callable=push_function,
    dag=dag,
)

push_by_xcom = PythonOperator(
    task_id='push_by_xcom',
    python_callable=push_by_xcom_push,
    dag=dag,
)

# pull - xcom_test
pull_1 = PythonOperator(
    task_id='pull_info_1',
    python_callable=pull_function,
    dag=dag,
)

# pull - xcom_push
pull_2 = BashOperator(
    task_id='pull_info_2',
    bash_command='echo "{{ ti.xcom_pull(key="pushed_value") }}"',
    dag=dag,
)

push_info >> push_by_xcom >> pull_1 >> pull_2