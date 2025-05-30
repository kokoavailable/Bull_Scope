from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta
from src.async_data_collector import DataCollector
import pytz
from helper.common import logger

def daily_stock_update(**context):
    """
    주식 데이터를 매일 업데이트하는 함수
    """
    collector = DataCollector()

    try:

        execution_date = context['execution_date']
        dag_run = context['dag_run']
        if dag_run and dag_run.run_type == 'manual':
            end_date = execution_date
            start_date = end_date - timedelta(days=3)
            run_type = "manual"
            logger.info(f"수동 실행 감지: {start_date} - {end_date}")

        elif 'data_interval_start' in context and 'data_interval_end' in context:
            start_date = context['data_interval_start']
            end_date = context['data_interval_end']
            run_type = "backfill"
            logger.info(f"백필 실행 감지: {start_date} - {end_date}")
        
        else:
            start_date = execution_date
            end_date = start_date + timedelta(days=1)
            run_type = "scheduled"
            logger.info(f"정규 스케줄 감지")

        logger.info(f"실행 타입: {run_type}")

        result = collector.update_stocks_by_date_range(
            start_date=start_date,
            end_date=end_date,
            market="US",
            delay=0.1,  # 일일 업데이트이므로 적당한 지연
            max_workers=8
        )

        success_count = sum(1 for success in result.values() if success)
        total_count = len(result)

        logger.info(f"데이터 수집 완료: {success_count}/{total_count} 심볼 성공")
        
    
    

        return {
            'success_count': success_count,
            'total_count': total_count,
            'start_date': start_date.isoformat(),
            'end_date': end_date.isoformat(),
            'run_type': run_type
        }

    except Exception as e:
        logger.error(f"일일 데이터 업데이트 실패: {str(e)}")
        raise
        
    finally:
        collector.close()


    


default_args = {
    'owner': 'data-team',
    'depends_on_past': False,  # 이전 실행 실패해도 다음 실행 가능
    'start_date': datetime(2025, 5, 1, tzinfo=pytz.UTC),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'max_active_runs': 2,
}


with DAG(
    dag_id='daily_stock_update',
    default_args=default_args,
    description='매일 주식 데이터 업데이트 (백필 지원)',
    schedule="30 8 * * 1-5",  # 평일 오전 8:30 (시장 개장 전)
    catchup=True,  # 백필 활성화
    max_active_runs=2,
    tags=['stock', 'daily', 'production', 'scheduled'],
    doc_md="""
    ## 일일 주식 데이터 업데이트 DAG

    **스케줄**: 평일 오전 8:30 (미국 증시 개장 전)

    **기능**:
    - 정규 실행: 해당 날짜 데이터만 수집
    - 백필 실행: 지정된 기간 데이터 수집
    - 수동 실행: 최근 3일 데이터 수집
    
    **백필 사용법**:
    ```bash
    airflow dags backfill daily_stock_update \
        --start-date 2025-05-01 \
        --end-date 2025-05-10
    ```
    """,
) as dag:
    
    daily_update = PythonOperator(
        task_id='update_daily_stock_data',
        python_callable=daily_stock_update,
        execution_timeout=timedelta(hours=2),

        doc_md="""
        일일 주식 데이터를 수집합니다.
        - 정규 스케줄: 당일 데이터
        - 백필: 지정 기간 데이터
        - 수동 실행: 최근 3일 데이터
        """,
    )


    
daily_update