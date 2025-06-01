from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
}

dag = DAG(
    'ecommerce_pipeline',
    default_args=default_args,
    schedule_interval='@hourly',
    catchup=False
)

start = DummyOperator(task_id='start', dag=dag)

start_kafka = BashOperator(
    task_id='start_kafka_producer',
    bash_command='python /opt/airflow/kafka/kafka_producer.py',
    dag=dag
)

start_spark = BashOperator(
    task_id='run_spark_job',
    bash_command='spark-submit /opt/airflow/spark/streaming_job.py',
    dag=dag
)

end = DummyOperator(task_id='end', dag=dag)

start >> start_kafka >> start_spark >> end
