from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow.src.data_collector import DataCollector
from helper.common import logger

def initial_full_update():
    """
    초기 주식 데이터를 업데이트하는 함수
    """
    collector = DataCollector()

    try:
        logger.info("초기 전체 주식 데이터 수집 시작")
        
        result = collector.update_all_stocks_parallel(
            market="US", 
            period="max",  # 가능한 모든 기간의 데이터
            delay=0.05,    # 초기 로드이므로 빠르게
            max_workers=12  # 초기 로드이므로 더 많은 워커 사용
        )

        success_count = sum(1 for success in result.values() if success)
        total_count = len(result)

        logger.info(f"초기 데이터 로드 완료: {success_count}/{total_count} 심볼 성공")

        return {
            'success_count': success_count,
            'total_count': total_count,
            'period': 'max',
            'type': 'initial_load'
        }
    
    except Exception as e:
        logger.error(f"초기 데이터 로드 실패: {str(e)}")
        raise
        
    finally:
        collector.close()

default_args = {
    'owner': 'data_team',
    'depends_on_past': False,
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