# dags/daily_technicals.py
from airflow.decorators import dag, task
from datetime import datetime
import pendulum
from technical_collector import TechnicalCollector
from airflow.sensors.external_task import ExternalTaskSensor

@dag(schedule_interval=None,
     start_date=pendulum.datetime(2025,5,29,tz="Asia/Seoul"),
     catchup=False, tags=["etl","technicals"])
def daily_technicals():
    # 1) price DAG 끝날 때 트리거
    wait = ExternalTaskSensor(
        task_id="wait_for_prices",
        external_dag_id="daily_price_update",
        external_task_id="update_prices",
        mode="poke",
        poke_interval=60,
        timeout=60*60*2  # 최대 2시간 대기
    )

    @task()
    def compute_technicals():
        syms = [s for s in fetch_stock_symbols()]
        c = TechnicalCollector()
        res = c.update_all_technicals_parallel(syms, max_workers=12)
        c.close()
        return res

    wait >> compute_technicals()

dag = daily_technicals()
