from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta
from src.data_collector import DataCollector

def daily_update():
    """
    주식 데이터를 매일 업데이트하는 함수
    """
    collector = DataCollector()
    result = collector.update_all_stocks_parallel(market="US", period="1d", delay=0.2)
    collector.close()
    return result

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 5, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='daily_stock_update',
    default_args=default_args,
    description='매일 주식 데이터를 업데이트하는 DAG',
    schedule="0 0 * * 1-5",
    catchup=False,
    tags = ['stock', 'daily'],
) as dag:
    
    daily_task = PythonOperator(
        task_id='update_daily',
        python_callable=daily_update,
        execution_timeout=None,
    )