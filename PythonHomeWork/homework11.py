from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.sensors.python import PythonSensor
from datetime import datetime, timedelta
import boto3

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'homework11',
    default_args=default_args,
    description='DAG to run Spark tasks with LocalStack S3 check',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
)

# Функція для перевірки наявності файлу в LocalStack S3
def check_file_in_s3():
    s3_client = boto3.client('s3', endpoint_url='http://localhost:4566')
    bucket_name = 'my-test-bucket'
    file_key = 'transactions.csv'
    response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=file_key)
    return 'Contents' in response

# Перевірка наявності нового файлу з транзакціями
check_file = PythonSensor(
    task_id='check_file',
    python_callable=check_file_in_s3,
    poke_interval=10,
    timeout=600,
    dag=dag,
)

# Завдання Spark п.1
spark_task_1 = SparkSubmitOperator(
    task_id='spark_task_1',
    application='/path/to/spark_task_1.py',
    conn_id='spark_default',
    dag=dag,
)

# Завдання Spark п.2
spark_task_2 = SparkSubmitOperator(
    task_id='spark_task_2',
    application='/path/to/spark_task_2.py',
    conn_id='spark_default',
    dag=dag,
)

# Завдання Spark п.3
spark_task_3 = SparkSubmitOperator(
    task_id='spark_task_3',
    application='/path/to/spark_task_3.py',
    conn_id='spark_default',
    dag=dag,
)

# Завдання Spark п.4
spark_task_4 = SparkSubmitOperator(
    task_id='spark_task_4',
    application='/path/to/spark_task_4.py',
    conn_id='spark_default',
    dag=dag,
)

# Dummy task to end the DAG if no file is found
end_dag = DummyOperator(
    task_id='end_dag',
    dag=dag,
)

# Визначення послідовності задач
check_file >> [spark_task_1, end_dag]
spark_task_1 >> spark_task_2 >> spark_task_3 >> spark_task_4
