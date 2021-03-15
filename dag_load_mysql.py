from datetime import timedelta

import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator

default_args = {
    'owner': 'etlers',
    'start_date': airflow.utils.dates.days_ago(2),
    #'end_date': datetime(2021, 03, 12),
    'depends_on_past': False,
    'email': ['etlersadm@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    # if a task fails, retry it once after waiting
    # at least 5 minutes
    #'retries': 1,
    #'retry_delay': timedelta(minutes=5),
    'schedule_interval': '@hourly',
}

dag = DAG(
    'dag_load_mysql',
    default_args=default_args,
    description='from csv into mysql DAG',
    schedule_interval=timedelta(days=1),
)

t1 = BashOperator(
    task_id='print_date',
    bash_command='date',
    dag=dag,
)

t2 = BashOperator(
    task_id='insert_data',
    depends_on_past=False,
    bash_command='python3 /home/etlers/airflow/dags/py/apply_mysql.py',
    dag=dag,
)

t1 >> t2