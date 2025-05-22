from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from scripts.src.data_collector import DataCollector

def initial_full_update():
    """
    주식 데이터를 매일 업데이트하는 함수
    """
    collector = DataCollector()
    
    result = collector.update_all_stocks_parallel(market="US", period="1y", delay=0.2)
    collector.close()
    return result

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 5, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='initial_full_update',
    default_args=default_args,
    description='초기 주식 데이터를 업데이트하는 DAG',
    schedule_interval=None,
    catchup=False,
    tags = ['stock', 'initial'],
) as dag:
    
    init_task = PythonOperator(
        task_id='full_init',
        python_callable=initial_full_update,
    )