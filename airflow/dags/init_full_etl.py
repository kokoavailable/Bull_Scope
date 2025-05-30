# airflow/dags/init_full_etl.py
import sys, os
from datetime import datetime
import pendulum

from airflow.decorators import dag, task
from src.stock_collector      import StockCollector
from src.price_collector      import PriceCollector
from src.fundamental_collector import FundamentalCollector
from src.technical_collector  import TechnicalCollector
from src.market_collector     import MarketCollector
from helper.common import logger

@dag(
    schedule=None,           # 수동 트리거 전용
    start_date=pendulum.datetime(2025, 5, 29, tz="Asia/Seoul"),
    catchup=False,
    tags=["init","etl"]
)
def init_full_etl():

    @task()
    def init_stocks() -> list[str]:
        c = StockCollector()
        c.update_stock_list(max_symbols=300, batch_size=100, max_workers=4)
        syms_df = c.fetch_stock_symbols()
        c.close_pool()
        return syms_df["symbol"].tolist()

    @task()
    def init_prices(symbols: list[str]):
        c = PriceCollector()
        c.update_all(period="max", interval="1d", batch_size=500, pause=0.5)
        c.close_pool()
        return True

    @task()
    def init_fundamentals(symbols: list[str]):
        c = FundamentalCollector()
        c.update_all_parallel(market="US", max_workers=8)
        c.close_pool()
        return True

    @task()
    def init_market():
        c = MarketCollector()
        c.run()
        c.close_pool()
        return True

    @task()
    def init_technicals(symbols: list[str]):
        c = TechnicalCollector()
        c.update_all_technicals_parallel(symbols, max_workers=12)
        c.close_pool()
        return True

    # 워크플로우 정의
    symbols = init_stocks()

    # 시장 지표는 심볼 목록과 무관하게 실행
    market_ok = init_market()
    # 종목 → 가격, 재무 동시 로드
    prices_ok = init_prices(symbols)
    funds_ok  = init_fundamentals(symbols)
    # 가격·재무 끝난 뒤 기술적 지표
    techs_ok  = init_technicals(symbols)

    symbols >> [market_ok, prices_ok, funds_ok] >> techs_ok

dag = init_full_etl()
