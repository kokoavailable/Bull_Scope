# dags/daily_price_update.py
from airflow.decorators import dag, task
from datetime import datetime
import pendulum
from src.price_collector import PriceCollector
from helper.common import fetch_stock_symbols

@dag(schedule="0 18 * * 1-5",
     start_date=pendulum.datetime(2025,5,29,tz="Asia/Seoul"),
     catchup=False, tags=["etl","prices"])
def daily_price_update():
    @task()
    def update_prices():
        symbols = fetch_stock_symbols()
        c = PriceCollector()
        res = c.update_all(period="1y", interval="1d", batch_size=200, pause=1)
        c.close()
        return res
    update_prices()

dag = daily_price_update()
