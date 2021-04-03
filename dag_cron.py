from datetime import timedelta

import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator

default_args = {
    'owner': 'etlers',
    'start_date': airflow.utils.dates.days_ago(0),
    #'end_date': datetime(2021, 03, 12),
    'depends_on_past': False,
    'email': ['etlersadm@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    # if a task fails, retry it once after waiting
    # at least 5 minutes
    #'retries': 1,
    #'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'dag_cron',
    default_args=default_args,
    description='A simple tutorial DAG',
    schedule_interval="20 3 3 * *",
)

t1 = BashOperator(
    task_id='print_date',
    bash_command='date',
    dag=dag,
)

t1.doc_md = """\
#### Task Documentation
You can documentation your task usinf the attributes `doc_md` (,arkdown),
`doc` (plain text), `doc_rst`, `doc_json`, `doc_yaml` which gets
rendered in the UI's Task Instance Details page
"""

dag.doc_md = __doc__

t2 = BashOperator(
    task_id='sleep',
    depends_on_past=False,
    bash_command='sleep 5',
    dag=dag,
)

template_command = """
{% for i in range(5) %}
    echo "{{ ds }}"
    echo "{{ macros.ds_add(ds, 7) }}"
    echo "{{ params.my_param }}"
{% endfor %}
"""

t3 = BashOperator(
    task_id='templated',
    depends_on_past=False,
    bash_command=template_command,
    params={'my_param': 'Parameter I passed in'},
    dag=dag,
)
"""
t1.set_downstream(t2)
t3.set_upstream(t1)
t1.set_downstream([t2, t3])
"""
t1 >> [t2, t3]
"""
[t2, t3] << t1
"""