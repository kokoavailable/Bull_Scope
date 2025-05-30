# dags/daily_market_indicators.py
from airflow.decorators import dag, task
import pendulum
from src.market_collector import MarketCollector

@dag(schedule_interval="10 6 * * 1-5",
     start_date=pendulum.datetime(2025,5,29,tz="Asia/Seoul"),
     catchup=False, tags=["etl","market"])
def daily_market_indicators():
    @task()
    def fetch_market():
        c = MarketCollector()
        ok = c.run()
        c.close()
        return ok
    fetch_market()

dag = daily_market_indicators()
