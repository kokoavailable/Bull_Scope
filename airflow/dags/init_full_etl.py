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
from src.support_resistance_collector     import SupportResistanceCollector

@dag(
    schedule=None,           # 수동 트리거 전용
    start_date=pendulum.datetime(2025, 5, 29, tz="Asia/Seoul"),
    catchup=False,
    tags=["init","etl"]
)
def init_full_etl():

    @task()
    def init_stocks():
        c = StockCollector()
        c.update_stock_list(max_workers=4, delay=0.2)
        c.close_pool()

    @task()
    def init_prices():
        c = PriceCollector()
        c.update_all(period="max", interval="1d", batch_size=500, max_workers=4, delay=0.2)
        c.close_pool()

    @task()
    def init_fundamentals():
        c = FundamentalCollector()
        c.update_all_parallel(market="US", max_workers=4, delay=0.2)
        c.close_pool()

    @task()
    def init_market():
        c = MarketCollector()
        c.run()
        c.close_pool()

    @task()
    def init_technicals():
        c = TechnicalCollector()
        c.update_all_technicals_parallel(max_workers=12)
        c.close_pool()
    
    @task()
    def init_support_resistances():
        c = SupportResistanceCollector()
        c.update_all("US", max_workers=8)
        c.close_pool()


    # 워크플로우 정의
    symbols = init_stocks()

    # 시장 지표는 심볼 목록과 무관하게 실행
    market_ok = init_market()
    # 종목 → 가격, 재무 동시 로드
    prices_ok = init_prices()
    funds_ok  = init_fundamentals()
    # 가격·재무 끝난 뒤 기술적 지표
    techs_ok  = init_technicals()

    support_resistance_ok = init_support_resistances()

    # 의존성 정의
    symbols >> prices_ok >> funds_ok         # 가격 → 재무 (순차)
    symbols >> market_ok                     # 마켓은 독립 실행
    prices_ok >> [techs_ok, support_resistance_ok]  # 가격 기반 분석


dag = init_full_etl()
