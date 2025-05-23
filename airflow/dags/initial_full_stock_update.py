from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta
from src.data_collector import DataCollector

def initial_full_update():
    """
    초기 주식 데이터를 업데이트하는 함수
    """
    collector = DataCollector()
    
    result = collector.update_all_stocks_parallel(market="US", period="max", delay=0.2)
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
    schedule=None,
    catchup=False,
    tags = ['stock', 'initial'],
) as dag:
    
    init_task = PythonOperator(
        task_id='full_init',
        python_callable=initial_full_update,
        execution_timeout=None,
    )