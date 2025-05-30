# dags/weekly_stock_list.py
from airflow.decorators import dag, task
from datetime import datetime
import pendulum
from src.stock_collector import StockCollector

@dag(schedule="0 3 * * 1",
     start_date=pendulum.datetime(2025,5,29,tz="Asia/Seoul"),
     catchup=False, tags=["stocks"])
def weekly_stock_list():
    @task()
    def update_stock_list():
        c = StockCollector()
        ok = c.update_stock_list(max_symbols=1000, max_workers=16)
        c.close()
        return ok
    update_stock_list()

dag = weekly_stock_list()
