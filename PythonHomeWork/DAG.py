from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.sensors.date_time_sensor import DateTimeSensor

t = 'first'


def print_hello_world():
    print("Hello world!")
    return 'Xcom returned success'


def print_hello_world1():
    print("Hello world from python_task1!")


def get_branch():
    if t == 'first4':
        return 'branch_1'
    else:
        return 'branch_2'


def xcom_result(**kwargs):
    ti = kwargs['ti']
    xcom_value = ti.xcom_pull(task_ids='my_python_task')
    print(f"Received XCom value: {xcom_value}")


with DAG("my_dag", start_date=datetime(2024, 8, 17), schedule_interval=None) as dag:
    python_task = PythonOperator(
        task_id="my_python_task",
        python_callable=print_hello_world,
    )
    branching = BranchPythonOperator(
        task_id="branching_step",
        python_callable=get_branch,
    )
    branch_1 = DummyOperator(task_id="branch_1")
    branch_2 = DummyOperator(task_id="branch_2")
    branching >> [branch_1, branch_2]

    with TaskGroup("my_task_group") as task_group:
        task_1 = DummyOperator(task_id="task_1")
        task_2 = DummyOperator(task_id="task_2")
        python_task1 = PythonOperator(
            task_id="my_python_task1",
            python_callable=print_hello_world1,
        )

    xcom_test = PythonOperator(
        task_id='xcom_test',
        python_callable=xcom_result,
    )

    time_sensor = DateTimeSensor(
        task_id='wait_2024_08_18',
        target_time='2024-08-18T00:00:00',
    )

python_task >> task_group >> xcom_test >> time_sensor




