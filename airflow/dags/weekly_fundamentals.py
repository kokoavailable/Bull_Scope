# dags/weekly_fundamentals.py
from airflow.decorators import dag, task
import pendulum
from src.fundamental_collector import FundamentalCollector

@dag(schedule="0 4 * * FRI",
     start_date=pendulum.datetime(2025,5,29,tz="Asia/Seoul"),
     catchup=False, tags=["etl","fundamentals"])
def weekly_fundamentals():
    @task()
    def update_fundamentals():
        c = FundamentalCollector()
        res = c.update_all_parallel(market="US", max_workers=8)
        c.close()
        return res
    update_fundamentals()

dag = weekly_fundamentals()